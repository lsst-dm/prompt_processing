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


"""A registry of which local repos are owned by which workers.

This module is designed to be imported from within a Gunicorn server hook. As
such, it must be as minimal as possible, have no side effects, and avoid
expensive imports like `lsst`.
"""


__all__ = ["LocalRepoRegistry"]


import collections.abc
import logging
import os.path
import typing


_log = logging.getLogger("lsst." + __name__)
_log.setLevel(logging.DEBUG)


class LocalRepoRegistry:
    """A registry that maps process IDs to repo locations.

    This class works only with directories, and has no knowledge of the
    Butler itself.

    Notes
    -----
    This class is process-safe; ``LocalRepoRegistry`` objects in different
    processes all refer to a shared backend.
    """
    _instance: typing.Self | None = None

    _REGISTRY_FILE = os.path.join(os.path.expanduser("~"), ".repo.registry.csv")

    @staticmethod
    def get() -> typing.Self:
        """Return the registry."""
        if LocalRepoRegistry._instance is None:
            LocalRepoRegistry._instance = LocalRepoRegistry()
        return LocalRepoRegistry._instance

    def init_registry(self):
        """Prepare the registry for use.

        This method should be called by the parent process of any processes
        that will use the registry, or at least a process that is guaranteed
        to outlast the client processes.
        """
        with open(self._REGISTRY_FILE, mode="x", opener=self._open_blocking) as f:
            self._write_registry(f, {})
        _log.info("Local repo registry is ready for use.")

    def cleanup_registry(self):
        """Remove the registry and delete all state.
        """
        try:
            os.remove(self._REGISTRY_FILE)
        except FileNotFoundError:
            pass
        _log.info("Local repo registry has been cleaned up.")

    @staticmethod
    def _open_blocking(path: str, flags: int, *args, **kwargs) -> int:
        """Force a file to open in non-blocking mode.

        Parameters
        ----------
        path : `str`
            The path of the file to open.
        flags : `int`
            Low-level I/O flags; see `os.open` for a list.
        args, kwargs
            Extra arguments to `os.open`.

        Returns
        -------
        fd : `int`
            A file descriptor for the opened file.
        """
        # Remove os.O_NONBLOCK, can't do it cleanly with bitwise operators
        blocking_flags = flags - os.O_NONBLOCK if flags & os.O_NONBLOCK else flags
        return os.open(path, blocking_flags, *args, **kwargs)

    def _read_registry(self, file) -> collections.abc.Mapping[int, str]:
        """Load the contents of the registry.

        Parameters
        ----------
        file : file object
            The file containing the registry.

        Returns
        -------
        registry : mapping [`int`, `str`]
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

    def _write_registry(self, file, data: collections.abc.Mapping[int, str]):
        """Replace the contents of the registry.

        This method overwrites the contents of the registry with ``data``
        instead of merging them.

        Parameters
        ----------
        file : file object
            The file containing the registry.
        data : mapping [`int`, `str`]
            The mapping from process IDs to the associated local repo to store
            in the registry.

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
        """Add a repository to the registry.

        Parameters
        ----------
        pid : `int`
            The process ID responsible for the repo.
        repo : `str`
            The canonical path to the repo.

        Raises
        ------
        ValueError
            Raised if the registry already has an entry for ``pid`` or ``repo``.
        """
        # With blocking I/O, we can use the open file handle as an OS-enforced lock
        with open(self._REGISTRY_FILE, mode="r+", opener=self._open_blocking) as f:
            registry = self._read_registry(f)

            if pid in registry:
                raise ValueError(f"Pid {pid} already owns {registry[pid]}.")
            if repo in registry.values():
                raise ValueError(f"Repo {repo} is already owned by "
                                 f"pid {self._reverse_lookup(registry, repo)}.")

            registry[pid] = repo
            self._write_registry(f, registry)
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
        # With blocking I/O, we can use the open file handle as an OS-enforced lock
        with open(self._REGISTRY_FILE, mode="r+", opener=self._open_blocking) as f:
            registry = self._read_registry(f)
            try:
                repo = registry.pop(pid)
                self._write_registry(f, registry)
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
            Raised if ``repo`` is unknown to the registry.
        """
        # With blocking I/O, we can use the open file handle as an OS-enforced lock
        with open(self._REGISTRY_FILE, mode="r+", opener=self._open_blocking) as f:
            registry = self._read_registry(f)

        try:
            return self._reverse_lookup(registry, repo)
        except KeyError:
            raise ValueError(f"Repository {repo} is not owned by any process.")
