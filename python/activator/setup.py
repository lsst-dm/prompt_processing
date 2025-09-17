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


__all__ = ["ServiceSetup"]


import logging


_log = logging.getLogger("lsst." + __name__)
_log.setLevel(logging.DEBUG)


class ServiceSetup:
    """An interface that abstracts away any code that should be run on service
    start.

    ServiceSetup is designed as a static class rather than a singleton to
    simplify the notation, especially for decorators.
    """

    _registered = []
    """The callables to call on start (mutable collection [callable]).
    """

    @staticmethod
    def check_on_init(func):
        """A function decorator that registers a function for calls to
        `run_init_checks`.

        Parameters
        ----------
        func : callable
            The function to be registered. Must be callable without arguments.
            It is recommended that it be idempotent.
        """
        # There doesn't seem to be a reliable way to check if `func` is nullary
        ServiceSetup._registered.append(func)  # For a decorator, don't need to guard vs. duplicates
        return func

    @staticmethod
    def run_init_checks():
        """Run all functions registered with `check_on_init`.

        The registered functions are called without arguments in an undefined
        order.
        """
        for func in ServiceSetup._registered:
            func()

    @staticmethod
    def reset():
        """Unregister all functions registered with `check_on_init`.
        """
        ServiceSetup._registered.clear()
