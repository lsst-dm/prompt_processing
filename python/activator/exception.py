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


__all__ = ["NonRetriableError", "RetriableError", "GracefulShutdownInterrupt",
           "InvalidVisitError", "IgnorableVisit",
           "InvalidPipelineError", "NoGoodPipelinesError",
           "PipelinePreExecutionError", "PipelineExecutionError",
           ]


class NonRetriableError(Exception):
    """A processing failure that must not be retried, regardless of the
    underlying error.

    This class is intended as an adapter for another exception, and exposes
    the ``nested`` field for this purpose.
    """

    @property
    def nested(self):
        """The exception nested inside this one (`BaseException`, read-only).

        This property is guaranteed non-raising, to make it easier to use
        inside exception handlers. If there is no nested exception, it is equal
        to `None`.
        """
        if self.__cause__:
            return self.__cause__
        elif self.__context__ and not self.__suppress_context__:
            return self.__context__
        else:
            return None


class RetriableError(Exception):
    """A processing failure that can be safely retried.

    This class serves as an abstraction layer between the activator (which is
    responsible for retry behavior) and the Middleware interface (which has the
    information to determine whether retries are safe).

    This class is intended as an adapter for another exception, and exposes
    the ``nested`` field for this purpose.
    """

    @property
    def nested(self):
        """The exception nested inside this one (`BaseException`, read-only).

        This property is guaranteed non-raising, to make it easier to use
        inside exception handlers. If there is no nested exception, it is equal
        to `None`.
        """
        if self.__cause__:
            return self.__cause__
        elif self.__context__ and not self.__suppress_context__:
            return self.__context__
        else:
            return None


# See https://docs.python.org/3.11/library/exceptions.html#KeyboardInterrupt
# for why interrupts should not subclass Exception.
class GracefulShutdownInterrupt(BaseException):
    """An interrupt indicating that the service should shut down gracefully.

    Like all interrupts, ``GracefulShutdownInterrupt`` can be raised between
    *any* two bytecode instructions, and handling it requires special care. See
    `the Python docs <https://docs.python.org/3.11/library/signal.html#handlers-and-exceptions>`__.
    """


class InvalidVisitError(ValueError):
    """An exception raised if a visit object has invalid or inappropriate
    fields.

    See Also
    --------
    activator.visit.SummitVisit
    activator.visit.FannedOutVisit
    """


class IgnorableVisit(ValueError):
    """An exception raised if a visit object has fields that are valid,
    but that is configured to not be processed.

    See Also
    --------
    activator.visit.SummitVisit
    activator.visit.FannedOutVisit
    """


class InvalidPipelineError(ValueError):
    """Exception raised if a pipeline cannot be loaded or configured.
    """


class NoGoodPipelinesError(RuntimeError):
    """Exception raised if none of the configured pipelines could be run on
    the data.
    """


class PipelinePreExecutionError(RuntimeError):
    """Exception raised if a pipeline could not be prepared for execution.

    Usually chained to an internal exception.
    """


class PipelineExecutionError(RuntimeError):
    """Exception raised if a pipeline attempted to run, but failed.

    Usually chained to an internal exception.
    """
