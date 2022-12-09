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

__all__ = ["GCloudStructuredLogFormatter", "setup_google_logger", "setup_usdf_logger"]

import json
import logging

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
    # Don't call daf_butler.CliLog.initLog because we need different formatters
    # from what Butler assumes.

    # De-prioritize output from 3rd-party packages.
    logging.getLogger().setLevel(logging.WARNING)
    # Capture output from Middleware and pipeline.
    if lsst_log is not None:
        lsst_log.Log.getLogger("lsst").setLevel(lsst_log.LevelTranslator.logging2lsstLog(logging.INFO))
    logging.getLogger("lsst").setLevel(logging.INFO)


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
