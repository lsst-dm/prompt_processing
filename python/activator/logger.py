# This file is part of prompt_prototype.
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

__all__ = ["GCloudStructuredLogFormatter", "setup_google_logger", "setup_usdf_logger",
           "RecordFactoryContextAdapter"]

from contextlib import contextmanager
import json
import logging
import os
import threading

import lsst.daf.butler as daf_butler

try:
    import lsst.log as lsst_log
except ModuleNotFoundError:
    lsst_log = None


def _parse_log_levels(spec):
    """Parse a string description of logging levels.

    Parameters
    ----------
    spec : `str`
        A string consisting of space-separated pairs of logger=level
        specifications. No attempt is made to validate whether the string
        conforms to this format. "." refers to the root logger.

    Returns
    -------
    levels : `list` [`tuple` [`str`, `str`]]
        A list of tuples whose first element is a logger (root logger
        represented by `None`) and whose second element is a logging level.
    """
    levels = [tuple(s.split("=", maxsplit=1)) for s in spec.split(None)]
    return [(k if k != "." else None, v) for k, v in levels]


def _set_lsst_logging_levels():
    """Set up standard logging levels for LSST code.

    This function consistently sets both the Python logger and lsst.log.
    """
    # Don't call CliLog.initLog because we need different formatters from what
    # Butler assumes.
    default_levels = [(None, "WARNING"),  # De-prioritize output from 3rd-party packages.
                      ("lsst", "INFO"),   # Capture output from Middleware and pipeline.
                      ]

    log_request = os.environ.get("SERVICE_LOG_LEVELS", "")
    daf_butler.cli.cliLog.CliLog.setLogLevels(default_levels + _parse_log_levels(log_request))


def _channel_all_to_pylog():
    """Set up redirection of lsst.log and warning output to the Python logger.

    This ensures that all service output is formatted consistently, making it easier to parse.
    """
    if lsst_log is not None:
        lsst_log.configure_pylog_MDC("DEBUG", MDC_class=None)
        lsst_log.usePythonLogging()

    logging.captureWarnings(True)


# TODO: replace with something more extensible, once we know what needs to
# vary besides the formatter (handler type?).
def setup_google_logger(labels=None):
    """Set global logging settings for prompt_prototype.

    Calling this function makes `GCloudStructuredLogFormatter` the root
    formatter and redirects all warnings to go through it.

    Parameters
    ----------
    labels : `dict` [`str`, `str`]
        Any metadata that should be attached to all logs. See
        ``LogEntry.labels`` in Google Cloud REST API documentation.

    Returns
    -------
    handler : `logging.Handler`
        The handler used by the root logger.
    """
    log_handler = logging.StreamHandler()
    log_handler.setFormatter(GCloudStructuredLogFormatter(labels))
    logging.basicConfig(handlers=[log_handler])
    _channel_all_to_pylog()
    _set_lsst_logging_levels()
    return log_handler


def setup_usdf_logger(labels=None):
    """Set global logging settings for prompt_prototype.

    Calling this function redirects all warnings to go through the logger.

    Parameters
    ----------
    labels : `dict` [`str`, `str`]
        Any metadata that should be attached to all logs.

    Returns
    -------
    handler : `logging.Handler`
        The handler used by the root logger.
    """
    log_handler = logging.StreamHandler()
    logging.basicConfig(handlers=[log_handler])
    _channel_all_to_pylog()
    _set_lsst_logging_levels()
    return log_handler


class GCloudStructuredLogFormatter(logging.Formatter):
    """A formatter that can be parsed by the Google Cloud logging agent.

    The formatter's output is a JSON-encoded message containing keywords
    recognized by the logging agent.

    Parameters
    ----------
    labels : `dict` [`str`, `str`]
        Any metadata that should be attached to the log. See ``LogEntry.labels``
        in Google Cloud REST API documentation.
    """
    def __init__(self, labels=None):
        super().__init__()

        if labels:
            self._labels = labels
        else:
            self._labels = {}

    def format(self, record):
        # Call for side effects only; ignore result.
        super().format(record)

        entry = {
            "severity": record.levelname,
            "logging.googleapis.com/labels": self._labels,
            "message": record.getMessage(),
        }
        return json.dumps(entry)


class RecordFactoryContextAdapter:
    """A log record factory that adds contextual data to another factory.

    This factory adds a ``logging_context`` mapping to the log record. The
    mapping is empty by default, and can be managed with the `add_context`
    context manager. Formatters that can handle the contents of this field
    must be configured separately.

    Parameters
    ----------
    factory : callable
        A log record factory (satisfying the interface described under
        `logging.setLogRecordFactory`) to which to add context.

    Notes
    -----
    This class is designed to be passed to `logging.setLogRecordFactory`, and
    therefore be shared among all threads of an application. However, all
    context is held in thread-local state, so the class is thread-safe in the
    sense that most application code can act as if each thread had its own
    factory object.
    """
    def __init__(self, factory):
        self._old_factory = factory
        # Record factories must be shared to be useful; keep all nontrivial
        # state in a `local` object to emulate a thread-specific factory.
        self._store = threading.local()
        self._store.context = {}

    def __call__(self, *args, **kwargs):
        """Create a log record from the provided arguments.

        See `logging.setLogRecordFactory` for the parameters.

        Returns
        -------
        record : `logging.LogRecord`
            A log record containing a ``logging_context`` mapping. The mapping
            maps strings to arbitrary values, as determined by any enclosing
            calls to `add_context`.
        """
        record = self._old_factory(*args, **kwargs)
        # _store.context is mutable; make sure record can't be changed after the fact.
        record.logging_context = self._store.context.copy()
        return record

    @contextmanager
    def add_context(self, **context):
        """A context manager that adds contextual data to all logging calls
        inside it.

        This manager adds key-value pairs to the ``logging_context`` mapping in
        this factory's log records.

        Parameters
        ----------
        context
            The keys and values to be added to ``logging_context``.

        Notes
        -----
        This method is thread-safe (``logging_context`` is thread-confined,
        even if ``self`` is shared).

        Examples
        --------
        >>> import logging, sys
        >>> logging.basicConfig(stream=sys.stdout,
        ...                     format="%(logging_context)s: %(levelname)s: %(message)s")
        >>> # The following line is not thread-safe, for simplicity.
        >>> logging.setLogRecordFactory(RecordFactoryContextAdapter(logging.getLogRecordFactory()))
        >>> with logging.getLogRecordFactory().add_context(visit=101, detector=42):
        ...     logging.error("Does not compute!")
        {'visit': 101, 'detector': 42}: ERROR: Does not compute!
        """
        _old_context = self._store.context.copy()
        try:
            self._store.context.update(**context)
            yield
        finally:
            # This replacement is safe because self._store cannot have been
            # changed by other threads. Changes can only have been made by
            # nested context managers, which have already been rolled back.
            self._store.context = _old_context
