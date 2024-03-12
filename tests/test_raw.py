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

import json
import os
import re
import unittest
import warnings

from lsst.resources import ResourcePath

from activator.raw import (
    is_path_consistent,
    get_prefix_from_snap,
    get_exp_id_from_oid,
    get_group_id_from_oid,
    LSST_REGEXP,
    OTHER_REGEXP,
    get_raw_path,
)
from activator.visit import FannedOutVisit

try:
    import boto3
    from moto import mock_s3
except ImportError:
    boto3 = None


class RawBase:
    """Base class for raw path handling.
    """
    def setUp(self):
        super().setUp()
        self.ra = 134.5454
        self.dec = -65.3261
        self.rot = 135.0
        self.survey = "IMAGINARY"
        self.visit = FannedOutVisit(
            instrument=self.instrument,
            detector=self.detector,
            groupId=self.group,
            nimages=self.snaps,
            filters=self.filter,
            coordinateSystem=FannedOutVisit.CoordSys.ICRS,
            position=[self.ra, self.dec],
            startTime=1_674_516_900.0,
            rotationSystem=FannedOutVisit.RotSys.SKY,
            cameraAngle=self.rot,
            survey=self.survey,
            salIndex=42,
            scriptSalIndex=42,
            dome=FannedOutVisit.Dome.OPEN,
            duration=35.0,
            totalCheckpoints=1,
            private_sndStamp=1_674_516_794.0,
        )

    def test_snap(self):
        path = get_raw_path(self.instrument, self.detector, self.group,
                            self.snap, self.exposure, self.filter)
        assert is_path_consistent(path, self.visit)


@unittest.skipIf(not boto3, "Warning: boto3 AWS SDK not found!")
class LsstBase(RawBase):
    mock_s3 = mock_s3()

    def setUp(self):
        self.mock_s3.start()
        s3 = boto3.resource("s3")
        self.bucket = "test-bucket-test"
        s3.create_bucket(Bucket=self.bucket)
        os.environ["IMAGE_BUCKET"] = self.bucket

        self.group = "2022-03-21T00:01:00"
        self.snaps = 2
        self.filter = "k2022"
        super().setUp()

    def tearDown(self):
        self.mock_s3.stop()
        super().tearDown()

    def test_snap_matching(self):
        """Test that a JSON file can be used to match an image path with a
        group.
        """
        path = get_raw_path(self.instrument, self.detector, self.group,
                            self.snap, self.exposure, self.filter)
        fits_path = ResourcePath(f"s3://{self.bucket}").join(path)
        json_path = fits_path.updatedExtension("json")
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", "S3 does not support flushing objects", UserWarning)
            with json_path.open("w") as f:
                json.dump(dict(GROUPID=self.group, CURINDEX=self.snap + 1), f)
        assert is_path_consistent(path, self.visit)
        assert get_group_id_from_oid(path) == self.group

    def test_writeread(self):
        """Test that raw module can parse the paths it creates.
        """
        path = get_raw_path(self.instrument, self.detector, self.group, self.snap, self.exposure, self.filter)
        parsed = re.match(LSST_REGEXP, path)
        self.assertIsNotNone(parsed)
        self.assertEqual(parsed['instrument'], str(self.instrument))
        self.assertEqual(get_exp_id_from_oid(path), self.exposure)

    def test_get_prefix(self):
        """Test that get_prefix_from_snap returns None for now."""
        prefix = get_prefix_from_snap(self.instrument, self.group, self.detector, self.snap)
        self.assertIsNone(prefix)


class LatissTest(LsstBase, unittest.TestCase):
    def setUp(self):
        self.instrument = "LATISS"
        self.detector = 0
        self.snap = 0
        self.exposure = 2022032100002
        super().setUp()

    def test_get_raw_path(self):
        path = get_raw_path(self.instrument, self.detector, self.group,
                            self.snap, self.exposure, self.filter)
        self.assertEqual(
            path,
            "LATISS/20220321/AT_O_20220321_000002/AT_O_20220321_000002_R00_S00.fits"
        )


class LsstComCamTest(LsstBase, unittest.TestCase):
    def setUp(self):
        self.instrument = "LSSTComCam"
        self.detector = 4
        self.snap = 1
        self.exposure = 2022032100003
        super().setUp()

    def test_get_raw_path(self):
        path = get_raw_path(self.instrument, self.detector, self.group,
                            self.snap, self.exposure, self.filter)
        self.assertEqual(
            path,
            "LSSTComCam/20220321/CC_O_20220321_000003/CC_O_20220321_000003_R22_S11.fits"
        )


class LsstCamTest(LsstBase, unittest.TestCase):
    def setUp(self):
        self.instrument = "LSSTCam"
        self.detector = 42
        self.snap = 0
        self.exposure = 2022032100004
        super().setUp()

    def test_get_raw_path(self):
        path = get_raw_path(self.instrument, self.detector, self.group,
                            self.snap, self.exposure, self.filter)
        self.assertEqual(
            path,
            "LSSTCam/20220321/MC_O_20220321_000004/MC_O_20220321_000004_R11_S20.fits"
        )


class HscTest(RawBase, unittest.TestCase):
    def setUp(self):
        self.instrument = "HSC"
        self.group = "2022032100001"
        self.snaps = 1
        self.filter = "k2022"
        self.detector = 42
        self.snap = 0
        self.exposure = 404
        super().setUp()

    def test_writeread(self):
        """Test that raw module can parse the paths it creates.
        """
        path = get_raw_path(self.instrument, self.detector, self.group, self.snap, self.exposure, self.filter)
        parsed = re.match(OTHER_REGEXP, path)
        self.assertIsNotNone(parsed)
        self.assertEqual(parsed['instrument'], str(self.instrument))
        self.assertEqual(parsed['detector'], str(self.detector))
        self.assertEqual(parsed['group'], str(self.group))
        self.assertEqual(parsed['snap'], str(self.snap))
        self.assertEqual(parsed['expid'], str(self.exposure))
        self.assertEqual(parsed['filter'], str(self.filter))
        self.assertEqual(get_exp_id_from_oid(path), self.exposure)

    def test_get_raw_path(self):
        path = get_raw_path(self.instrument, self.detector, self.group, self.snap, self.exposure, self.filter)
        self.assertEqual(
            path,
            "HSC/42/2022032100001/0/404/k2022/HSC-2022032100001-0-404-k2022-42.fz"
        )

    def test_get_prefix(self):
        """Test that get_prefix_from_snap returns proper prefix."""
        prefix = get_prefix_from_snap(self.instrument, self.group, self.detector, self.snap)
        self.assertEqual(prefix, "HSC/42/2022032100001/0/")
