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
    logging.captureWarnings(True)
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
    logging.captureWarnings(True)
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
