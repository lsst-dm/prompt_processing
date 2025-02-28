# This file is part of prompt_processing.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.


"""A mapping of which local repos are owned by which workers.

This module is designed to be imported from within a Gunicorn server hook. As
such, it must be as minimal as possible, have no side effects, and avoid
expensive imports like `lsst`.
"""


__all__ = ["LocalRepoTracker"]


import collections.abc
import contextlib
import fcntl
import logging
import os.path
import typing


_log = logging.getLogger("lsst." + __name__)
_log.setLevel(logging.DEBUG)


# Absolute path on this worker's system where local repos may be created
local_repos = os.environ.get("LOCAL_REPOS", "/tmp")


class LocalRepoTracker:
    """A mapping of process IDs to repo locations.

    This class works only with directories, and has no knowledge of the
    Butler itself.

    Notes
    -----
    This class is process-safe; ``LocalRepoTracker`` objects in different
    processes all refer to a shared backend.
    """
    _instance: typing.Self | None = None

    # Temp space is guaranteed to be in persistent storage (local disk
    # in unit tests, ephemeral in container). The same is NOT true for
    # ~ or /app.
    _BACKEND_FILE = os.path.join(local_repos, ".repo.tracker.csv")

    @staticmethod
    def get() -> typing.Self:
        """Return the tracker."""
        if LocalRepoTracker._instance is None:
            LocalRepoTracker._instance = LocalRepoTracker()
        return LocalRepoTracker._instance

    def init_tracker(self):
        """Prepare the tracker for use.

        This method should be called by the parent process of any processes
        that will use the tracker, or at least a process that is guaranteed
        to outlast the client processes.

        Return
        ------
        old_repos : collection [`str`]
            The contents of any pre-existing registry.
        """
        try:
            with self._open_exclusive(self._BACKEND_FILE, mode="x") as f:
                self._write_data(f, {})
            return set()
        except FileExistsError:
            # Don't log a stack trace, because this is likely to be called from
            # a non-JSON logger.
            with self._open_exclusive(self._BACKEND_FILE, mode="r+") as f:
                data = self._read_data(f)
                self._write_data(f, {})
                _log.warning("Dangling repo registry found with the following repos: %s", data.values())
                return data.values()
        finally:
            _log.info("Local repo tracker is ready for use.")

    def cleanup_tracker(self):
        """Remove the tracker and delete all state.

        Return
        ------
        repos : collection [`str`]
            The (hopefully empty) set of repos that were still registered.
        """
        try:
            with self._open_exclusive(self._BACKEND_FILE, mode="r") as f:
                data = self._read_data(f)
                repos = data.values()
            os.remove(self._BACKEND_FILE)
        except FileNotFoundError:
            repos = set()
        _log.info("Local repo tracker has been cleaned up.")
        if repos:
            _log.warning("The following repos were still registered: %s", repos)
        return repos

    @staticmethod
    @contextlib.contextmanager
    def _open_exclusive(*args, **kwargs):
        """Open a file with exclusive locking.

        This method blocks if the file is already held by another process.
        Locks are **not** re-entrant.

        Parameters
        ----------
        *args, **kwargs
            Arguments, interpreted as for `open`.

        Yields
        ------
        file : file object
            The open file.
        """
        with open(*args, **kwargs) as open_file:
            fcntl.flock(open_file, fcntl.LOCK_EX)
            try:
                yield open_file
            finally:
                # Mostly for redundancy -- flock guarantees that the lock is
                # released if the file descriptor is closed.
                fcntl.flock(open_file, fcntl.LOCK_UN)

    def _read_data(self, file) -> collections.abc.Mapping[int, str]:
        """Load the current tracker state.

        Parameters
        ----------
        file : file object
            The file backing the tracker.

        Returns
        -------
        data : mapping [`int`, `str`]
            A mapping from process IDs to the associated local repo.

        Note
        ----
        This method is not process-safe.
        """
        data = {}
        line = file.readline().strip()
        while line != "":
            id, repo = line.split(",", maxsplit=1)
            data[int(id)] = repo
            line = file.readline().strip()
        return data

    def _write_data(self, file, data: collections.abc.Mapping[int, str]):
        """Set the tracker state.

        This method overwrites the contents of the backend with ``data``
        instead of merging them.

        Parameters
        ----------
        file : file object
            The file backing the tracker.
        data : mapping [`int`, `str`]
            The mapping from process IDs to the associated local repo to store.

        Note
        ----
        This method is not process-safe.
        """
        file.seek(0)
        file.truncate()
        file.writelines(f"{pid},{repo}\n" for pid, repo in data.items())

    K = typing.TypeVar('K')
    V = typing.TypeVar('V')

    @staticmethod
    def _reverse_lookup(mapping: collections.abc.Mapping[K, V], value: V) -> K:
        """Look up a key by its corresponding value.

        Parameters
        ----------
        mapping
            The mapping to search.
        value
            The value to look up.

        Returns
        -------
        key
            A key that maps to ``value``. If there is more than one such key,
            which one is returned is undefined.
        """
        for key, kvalue in mapping.items():
            if value == kvalue:
                return key
        raise KeyError(f"No key maps to {value}.")

    def register(self, pid: int, repo: str):
        """Add a repository to the tracker.

        Parameters
        ----------
        pid : `int`
            The process ID responsible for the repo.
        repo : `str`
            The canonical path to the repo.

        Raises
        ------
        ValueError
            Raised if the tracker already has an entry for ``pid`` or ``repo``.
        """
        with self._open_exclusive(self._BACKEND_FILE, mode="r+") as f:
            data = self._read_data(f)

            if pid in data:
                raise ValueError(f"Pid {pid} already owns {data[pid]}.")
            if repo in data.values():
                raise ValueError(f"Repo {repo} is already owned by "
                                 f"pid {self._reverse_lookup(data, repo)}.")

            data[pid] = repo
            self._write_data(f, data)
        _log.info("Registered %s to process %d.", repo, pid)

    def pop(self, pid: int) -> str:
        """Unregister a repository.

        Parameters
        ----------
        pid : `int`
            The process ID responsible for the repo.

        Returns
        -------
        repo: `str`
            The popped repo.

        Raises
        ------
        ValueError
            Raised if there is no repo for ``pid``.
        """
        with self._open_exclusive(self._BACKEND_FILE, mode="r+") as f:
            data = self._read_data(f)
            try:
                repo = data.pop(pid)
                self._write_data(f, data)
            except KeyError:
                raise ValueError(f"No known repository for process {pid}.")
        _log.info("Unregistered %s from process %d.", repo, pid)
        return repo

    def get_owner(self, repo: str) -> int:
        """Check the process responsible for a repo.

        Parameters
        ----------
        repo : `str`
            The repo to look up.

        Returns
        ------
        pid : `int`
            The process associated with the repo.

        Raises
        ------
        ValueError
            Raised if ``repo`` is unknown to the tracker.
        """
        with self._open_exclusive(self._BACKEND_FILE, mode="r+") as f:
            data = self._read_data(f)

        try:
            return self._reverse_lookup(data, repo)
        except KeyError:
            raise ValueError(f"Repository {repo} is not owned by any process.")
