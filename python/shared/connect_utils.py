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

__all__ = ["retry"]


import functools
import logging
import random
import time


_log = logging.getLogger("lsst." + __name__)
_log.setLevel(logging.DEBUG)


def retry(tries, exceptions, *, wait=0.0):
    """A decorator that retries a function/method call a fixed number of times
    on specific exceptions.

    Parameters
    ----------
    tries : `int`
        The number of *total* attempts to make, must be at least 1.
    exceptions : `type` [`BaseException`] or `tuple` [`type`, ...]
        The exception(s) on which to retry the call. All other exceptions are
        passed through.
    wait : `float`, optional
        The average number of seconds to wait for retries. The actual wait time
        is jittered by 25% to avoid the thundering herd problem.

    Raises
    ------
    BaseExceptionGroup
        Raised if all tries raised an exception from ``exceptions``. The
        group contains the exception raised on each attempt.
    """
    if tries < 1:
        raise ValueError("Can't try a function {tries} times.")

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            results = []
            for t in range(tries):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    results.append(e)
                    if t < tries-1:
                        if wait > 0.0:
                            this_wait = wait * random.uniform(0.75, 1.25)
                            _log.warning("%s failed with %r. Waiting %.3f seconds before trying again.",
                                         func.__name__, e, this_wait)
                            time.sleep(this_wait)
                        else:
                            _log.warning("%s failed with %r, retrying.", func.__name__, e)
                    continue
            # Will be an ExceptionGroup if all exceptions are Exception
            raise BaseExceptionGroup("{func.__name__} failed in {tries} tries.", results)
        return wrapper
    return decorator
