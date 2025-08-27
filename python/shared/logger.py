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

__all__ = ["UsdfJsonFormatter", "setup_usdf_logger", "RecordFactoryContextAdapter"]

import collections.abc
from contextlib import contextmanager
import json
import logging
import os
import threading

import lsst.daf.butler.cli.cliLog

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
    lsst.daf.butler.cli.cliLog.CliLog.setLogLevels(default_levels + _parse_log_levels(log_request))


def _channel_all_to_pylog():
    """Set up redirection of lsst.log and warning output to the Python logger.

    This ensures that all service output is formatted consistently, making it easier to parse.
    """
    if lsst_log is not None:
        lsst_log.configure_pylog_MDC("DEBUG", MDC_class=None)
        lsst_log.usePythonLogging()

    logging.captureWarnings(True)


# TODO: automate precondition on DM-52151
def logging_context(**context):
    """A context manager that adds contextual data to all logging calls
    inside it.

    This manager requires that `setup_usdf_logger` or a similar function have
    been previously called to set up an appropriate log factory. It adds
    key-value pairs to the factory's log records, and also adds a mapping to
    any exceptions that escape. The details depend on the factory
    implementation; this function is only to keep the client from depending on
    the log factory directly.

    Parameters
    ----------
    context
        The keys and values to be added to logging records.
    """
    log_factory = logging.getLogRecordFactory()
    return log_factory.add_context(**context)


def _set_context_logger():
    """Set up RecordFactoryContextAdapter as the global log factory.

    This allows the use of its context manager inside the activator, and the
    use of the ``logging_context`` field in formatters.

    Notes
    -----
    Because it affects global configuration, this function should be called
    from the application's main thread. Unexpected behavior may result if it is
    called from a request handler instead.
    """
    logging.setLogRecordFactory(RecordFactoryContextAdapter(logging.getLogRecordFactory()))


def setup_usdf_logger(labels=None):
    """Set global logging settings for prompt_processing.

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
    _set_context_logger()
    log_handler = logging.StreamHandler()
    log_handler.setFormatter(UsdfJsonFormatter(labels))
    logging.basicConfig(handlers=[log_handler])
    _channel_all_to_pylog()
    _set_lsst_logging_levels()
    return log_handler


class UsdfJsonFormatter(logging.Formatter):
    """A formatter that can be parsed by the Loki/Grafana system at USDF.

    The formatter's output is a JSON-encoded message with "flattened" metadata
    to make it easy to inspect with Grafana filters.

    Parameters
    ----------
    labels : `dict` [`str`, `str`]
        Any metadata that should be attached to the log.
    """
    def __init__(self, labels=None):
        super().__init__()

        if labels:
            self._labels = labels
        else:
            self._labels = {}

    def format(self, record):
        # format updates record.message, but the full info is *only* in the return value.
        msg = super().format(record)

        # Many LogRecord attributes are only useful for interrogating the
        # record in Python; filter to what's useful in Grafana.
        entry = {
            # formatTime only automatically handles msecs (uuu) with the
            # default format, and the assumption that they're the last part of
            # the string is hardcoded. Use manual formatting instead.
            # RFC3339Nano is the only buit-in promtail format that supports
            # fractional seconds.
            "asctime": self.formatTime(record, datefmt='%Y-%m-%dT%H:%M:%S.%(msecs)03d%z')
            % {"msecs": record.msecs},
            "funcName": record.funcName,
            "level": record.levelname,  # "level" auto-parsed by Grafana
            "lineno": record.lineno,
            "message": msg,
            "name": record.name,
            "pathname": record.pathname,
            "process": record.process,
            "thread": record.thread,
        }
        entry.update(self._labels)
        entry.update(record.logging_context)

        return json.dumps(entry, default=_encode_json_extras)


def _encode_json_extras(obj):
    """Encode objects that are not JSON-serializable by default.

    Parameters
    ----------
    obj
        The object to encode. Assumed to not be JSON-serializable.

    Returns
    -------
    encodable
        A JSON-serializable object equivalent to ``obj``.
    """
    # Store collections as arrays
    if isinstance(obj, collections.abc.Collection):
        return list(obj)
    else:
        raise TypeError(f"{obj.__class__} is not JSON seriablizable")


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

    @property
    def _context(self):
        """The values to add to ``logging_context`` at any given time
        (mutable mapping).

        This value is guaranteed to be thread confined.
        """
        # Cannot initialize self._store.context in a way that's visible to all
        # threads, so handle initialization lazily instead.
        if not hasattr(self._store, "context"):
            self._store.context = {}
        return self._store.context

    @_context.setter
    def _context(self, value):
        self._store.context = value

    def __call__(self, name, level, fn, lno, msg, args, exc_info, func=None, sinfo=None, **kwargs):
        """Create a log record from the provided arguments.

        See `logging.setLogRecordFactory` for the parameters.

        Returns
        -------
        record : `logging.LogRecord`
            A log record containing a ``logging_context`` mapping. The mapping
            maps strings to arbitrary values, as determined by any enclosing
            calls to `add_context`.
        """
        record = self._old_factory(name, level, fn, lno, msg, args, exc_info, func, sinfo, **kwargs)
        # _context is mutable; make sure record can't be changed after the fact.
        record.logging_context = self._context.copy()
        if exc_info is not None:
            _, ex, _ = exc_info  # Only care about the exception object passed to the logger
            if hasattr(ex, "logging_context"):
                # Context at the point where the exception was raised takes precedence.
                record.logging_context.update(ex.logging_context)
        return record

    @contextmanager
    def add_context(self, **context):
        """A context manager that adds contextual data to all logging calls
        inside it.

        This manager adds key-value pairs to the ``logging_context`` mapping in
        this factory's log records. It also adds a mapping of that name to any
        exceptions that escape.

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
        _old_context = self._context.copy()
        try:
            self._context.update(**context)
            yield
        except BaseException as e:
            # In logging, inner context overrules outer context. Need the same for exceptions.
            inner_context = e.logging_context if hasattr(e, "logging_context") else {}
            e.logging_context = self._context.copy() | inner_context
            raise
        finally:
            # This replacement is safe because self._context cannot have been
            # changed by other threads. Changes can only have been made by
            # nested context managers, which have already been rolled back.
            self._context = _old_context
