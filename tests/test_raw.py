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

import re
import unittest

from activator.raw import Snap, RAW_REGEXP, get_raw_path


class RawTest(unittest.TestCase):
    """Test the API for handling raw paths.
    """
    def setUp(self):
        super().setUp()

        self.instrument = "NotACam"
        self.detector = 42
        self.group = "2022032100001"
        self.snaps = 2
        self.filter = "k2022"
        self.ra = 134.5454
        self.dec = -65.3261
        self.rot = 135.0
        self.kind = "IMAGINARY"
        self.snap = 1
        self.exposure = 404

    def test_writeread(self):
        """Test that raw module can parse the paths it creates.
        """
        path = get_raw_path(self.instrument, self.detector, self.group, self.snap, self.exposure, self.filter)
        parsed = re.match(RAW_REGEXP, path)
        self.assertIsNotNone(parsed)
        self.assertEqual(parsed['instrument'], str(self.instrument))
        self.assertEqual(parsed['detector'], str(self.detector))
        self.assertEqual(parsed['group'], str(self.group))
        self.assertEqual(parsed['snap'], str(self.snap))
        self.assertEqual(parsed['expid'], str(self.exposure))
        self.assertEqual(parsed['filter'], str(self.filter))

    def test_snap(self):
        """Test that Snap objects can be constructed from parseable paths.
        """
        path = get_raw_path(self.instrument, self.detector, self.group, self.snap, self.exposure, self.filter)
        parsed = Snap.from_oid(path)
        self.assertIsNotNone(parsed)
        # These tests automatically include type-checking.
        self.assertEqual(parsed.instrument, self.instrument)
        self.assertEqual(parsed.detector, self.detector)
        self.assertEqual(parsed.group, self.group)
        self.assertEqual(parsed.snap, self.snap)
        self.assertEqual(parsed.exp_id, self.exposure)
        self.assertEqual(parsed.filter, self.filter)

    def test_bad_snap(self):
        path = get_raw_path(self.instrument, f"{self.detector}b", self.group,
                            self.snap, self.exposure, self.filter)
        with self.assertRaisesRegex(ValueError, "not .* parsed"):
            Snap.from_oid(path)
