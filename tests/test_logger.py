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


import io
import json
import logging
import unittest

from activator.logger import GCloudStructuredLogFormatter


class GoogleFormatterTest(unittest.TestCase):
    """Test GCloudStructuredLogFormatter with fake log messages.
    """
    def setUp(self):
        super().setUp()

        # Buffer for log output.
        # Can't use assertLogs, because it inserts its own handler/formatter.
        self.output = io.StringIO()
        self.addCleanup(io.StringIO.close, self.output)

        log_handler = logging.StreamHandler(self.output)
        log_handler.setFormatter(GCloudStructuredLogFormatter(
            labels={"instrument": "NotACam"},
        ))
        # Unique logger per test
        self.log = logging.getLogger(self.id())
        self.log.propagate = False
        self.log.addHandler(log_handler)
        self.log.setLevel(logging.DEBUG)

    def _check_log(self, outputs, level, labels, texts):
        """Check that the log output is formatted correctly.

        Parameters
        ----------
        outputs : `list` [`str`]
            A list of the formatted log messages.
        level : `str`
            The emitted log level.
        labels : `dict` [`str`, `str`]
            The labels attached to the log message.
        texts : `list` [`str`]
            The expected log messages.
        """
        self.assertEqual(len(outputs), len(texts))
        for output, text in zip(outputs, texts):
            parsed = json.loads(output)
            self.assertEqual(parsed["severity"], level)
            self.assertEqual(parsed["message"], text)
            self.assertEqual(parsed["logging.googleapis.com/labels"], labels)

    def test_direct(self):
        """Test the translation of verbatim log messages.
        """
        msg = "Consider a spherical cow..."
        self.log.info(msg)
        self._check_log(self.output.getvalue().splitlines(),
                        "INFO", {"instrument": "NotACam"},
                        [msg])

    def test_args(self):
        """Test the translation of arg-based log messages.
        """
        msg = "Consider a %s..."
        args = "rotund bovine"
        self.log.warning(msg, args)
        self._check_log(self.output.getvalue().splitlines(),
                        "WARNING", {"instrument": "NotACam"},
                        [msg % args])

    def test_quotes(self):
        """Test handling of messages containing single or double quotes.
        """
        msgs = ["Consider a so-called 'spherical cow'.",
                'Consider a so-called "spherical cow".',
                ]
        for msg in msgs:
            self.log.info(msg)
        self._check_log(self.output.getvalue().splitlines(),
                        "INFO", {"instrument": "NotACam"},
                        msgs)

    def test_side_effects(self):
        """Test that format still modifies exposure records in the same way
        as Formatter.format.
        """
        msg = "Consider a %s..."
        args = "rotund bovine"
        factory = logging.getLogRecordFactory()
        record = factory(self.id(), logging.INFO, "file.py", 42, msg, args, None)
        formatter = GCloudStructuredLogFormatter()
        formatter.format(record)
        # If format has no side effects, record.message does not exist.
        self.assertEqual(record.message, msg % args)
