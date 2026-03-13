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


__all__ = ["ServiceManager"]


import functools
import logging


_log = logging.getLogger("lsst." + __name__)
_log.setLevel(logging.DEBUG)


class ServiceManager:
    """An interface that abstracts away any code that should be run on service
    start or stop.

    ServiceManager is designed as a static class rather than a singleton to
    simplify the notation, especially for decorators.
    """

    _registered_start = []
    """The callables to call on start (mutable collection [callable]).
    """
    _registered_close = []
    """The callables to close on shutdown (mutable sequence [callable]). They
    are listed in the order in which they should be called.
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
        ServiceManager._registered_start.append(func)  # For a decorator, don't need to guard vs. duplicates
        return func

    @staticmethod
    def clean_on_exit(closer):
        """A function decorator that registers a function's return value(s) for
        calls to `run_cleanups`.

        If the decorated function returns `None`, nothing is registered.
        Otherwise, objects are cleaned up in LIFO order.

        ``clean_on_exit`` may be combined with other decorators, but must be
        the innermost to ensure that other decorators don't bypass
        registration. If it's combined with `check_on_init`, the order of
        object creation and registration may be undefined.

        Parameters
        ----------
        closer : callable
            The callable to call on the return value of the decorated function.
        """
        # Design note -- don't try to default to obj.close; it's impossible to
        # implement a default without risking resource leaks.
        def decorator(func):
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                obj = func(*args, **kwargs)
                if obj is not None:
                    ServiceManager._registered_close.insert(0, functools.partial(closer, obj))
                return obj
            return wrapper
        return decorator

    @staticmethod
    def run_init_checks():
        """Run all functions registered with `check_on_init`.

        The registered functions are called without arguments in an undefined
        order.
        """
        for func in ServiceManager._registered_start:
            func()

    @staticmethod
    def run_cleanups():
        """Run all cleanup methods registered with `clean_on_exit`.

        The registered methods are called without arguments in LIFO order.
        """
        for closer in ServiceManager._registered_close:
            closer()

    @staticmethod
    def reset():
        """Unregister all functions registered with `check_on_init` or
        `clean_on_exit`.
        """
        ServiceManager._registered_start.clear()
        ServiceManager._registered_close.clear()
