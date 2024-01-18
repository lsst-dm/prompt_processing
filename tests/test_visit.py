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

import dataclasses
import json
import unittest

from activator.visit import FannedOutVisit, BareVisit


class FannedOutVisitTest(unittest.TestCase):
    """Test the FannedOutVisit class's functionality.
    """
    def setUp(self):
        super().setUp()

        self.testbed = FannedOutVisit(
            instrument="NotACam",
            detector=42,
            groupId="2023-01-23T23:33:14.762",
            nimages=2,
            filters="k2022",
            coordinateSystem=FannedOutVisit.CoordSys.ICRS,
            position=[134.5454, -65.3261],
            startTime=1_674_516_900.0,
            rotationSystem=FannedOutVisit.RotSys.SKY,
            cameraAngle=135.0,
            survey="IMAGINARY",
            salIndex=42,
            scriptSalIndex=42,
            dome=FannedOutVisit.Dome.OPEN,
            duration=35.0,
            totalCheckpoints=1,
            private_sndStamp=1_674_516_794.0,
        )

    def test_hash(self):
        # Strictly speaking should test whether FannedOutVisit fulfills the hash
        # contract, but it's not clear what kinds of differences the default
        # __hash__ might be insensitive to. So just test that the object
        # is hashable.
        value = hash(self.testbed)
        self.assertNotEqual(value, 0)

    def test_json(self):
        serialized = json.dumps(self.testbed.__dict__).encode("utf-8")
        deserialized = FannedOutVisit(**json.loads(serialized))
        self.assertEqual(deserialized, self.testbed)
        # Test that enums are handled correctly despite being serialized as shorts.
        # isinstance checks are ambigious because IntEnum is-an int.
        self.assertIs(type(self.testbed.coordinateSystem), FannedOutVisit.CoordSys)
        self.assertIs(type(deserialized.coordinateSystem), int)
        self.assertIsNot(type(deserialized.coordinateSystem), FannedOutVisit.CoordSys)

    def test_str(self):
        self.assertNotEqual(str(self.testbed), repr(self.testbed))
        self.assertIn(str(self.testbed.detector), str(self.testbed))
        self.assertIn(str(self.testbed.groupId), str(self.testbed))


class BareVisitTest(unittest.TestCase):
    """Test the BareVisit class's functionality.
    """
    def setUp(self):
        super().setUp()

        visit_info = dict(
            instrument="NotACam",
            groupId="2023-01-23T23:33:14.762",
            nimages=2,
            filters="k2022",
            coordinateSystem=BareVisit.CoordSys.ICRS,
            position=[134.5454, -65.3261],
            startTime=1_674_516_900.0,
            rotationSystem=BareVisit.RotSys.SKY,
            cameraAngle=135.0,
            survey="IMAGINARY",
            salIndex=42,
            scriptSalIndex=42,
            dome=BareVisit.Dome.OPEN,
            duration=35.0,
            totalCheckpoints=1,
        )
        self.visit = BareVisit(**visit_info)
        self.fannedOutVisit = FannedOutVisit(
            detector=42,
            private_sndStamp=1_674_516_794.0,
            **visit_info
        )

    def test_get_bare(self):
        self.assertEqual(
            self.fannedOutVisit.get_bare_visit(),
            dataclasses.asdict(self.visit)
        )
