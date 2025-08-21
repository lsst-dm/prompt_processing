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


import time
import unittest

from shared.connect_utils import retry


class ConnectionUtilsTest(unittest.TestCase):

    def test_retry_no_except(self):
        dummy = unittest.mock.Mock(__name__="function")
        wrapped = retry(2, ValueError)(dummy)
        wrapped("Foo", 42)
        dummy.assert_called_once_with("Foo", 42)

    def test_retry_except(self):
        dummy = unittest.mock.Mock(__name__="function", side_effect=[ValueError, True])
        wrapped = retry(2, ValueError)(dummy)
        wrapped()
        self.assertEqual(dummy.call_count, 2)

        dummy = unittest.mock.Mock(__name__="function", side_effect=ValueError)
        wrapped = retry(2, ValueError)(dummy)
        with self.assertRaises(ExceptionGroup):
            wrapped()
        self.assertEqual(dummy.call_count, 2)

        dummy = unittest.mock.Mock(__name__="function", side_effect=[ValueError, RuntimeError, False])
        wrapped = retry(3, ValueError)(dummy)
        # RuntimeError does not trigger a retry
        with self.assertRaises(RuntimeError):
            wrapped()
        self.assertEqual(dummy.call_count, 2)

        dummy = unittest.mock.Mock(__name__="function", side_effect=[ValueError, RuntimeError, False])
        wrapped = retry(3, (RuntimeError, ValueError))(dummy)
        wrapped()
        self.assertEqual(dummy.call_count, 3)

    def test_retry_parameters(self):
        dummy = unittest.mock.Mock(__name__="function")
        with self.assertRaises(ValueError):
            retry(0, RuntimeError)(dummy)
        retry(1, RuntimeError)(dummy)

    def test_retry_jitter(self):
        dummy = unittest.mock.Mock(__name__="function", side_effect=ValueError)
        # Long wait time is to minimize noise from CPU availability
        wrapped = retry(2, ValueError, wait=5.0)(dummy)
        start = time.time()
        with self.assertRaises(ExceptionGroup):
            wrapped()
        stop = time.time()
        self.assertEqual(dummy.call_count, 2)
        # Should have only waited after the first call
        # Expected wait is +/- 25%, or 3.75-6.25 s
        # If it waited twice, that's at least 7.5 s
        self.assertGreaterEqual(stop - start, 3.75)
        self.assertLessEqual(stop - start, 6.35)  # Fudge the high end for exception handling overhead
