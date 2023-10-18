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

import calendar
import datetime
import tempfile
import unittest

import boto3
import botocore
from moto import mock_s3

import lsst.daf.butler.tests as butler_tests
import lsst.meas.base
from lsst.obs.subaru import HyperSuprimeCam

from activator.raw import get_raw_path
from tester.utils import (
    get_last_group,
    make_exposure_id,
    day_obs_to_unix_utc,
    make_group,
    decode_group,
    increment_group,
)


class TesterUtilsTest(unittest.TestCase):
    """Test components in tester.
    """
    mock_s3 = mock_s3()
    bucket_name = "testBucketName"

    def setUp(self):
        self.mock_s3.start()
        s3 = boto3.resource("s3")
        s3.create_bucket(Bucket=self.bucket_name)

        path = get_raw_path(
            "TestCam", 123, "2022-11-02T00:00:00.000001", 2, 30, "TestFilter"
        )
        obj = s3.Object(self.bucket_name, path)
        obj.put(Body=b'test1')
        path = get_raw_path(
            "TestCam", 123, "2022-11-02T00:00:00.000002", 2, 30, "TestFilter"
        )
        obj = s3.Object(self.bucket_name, path)
        obj.put(Body=b'test2')

    def tearDown(self):
        s3 = boto3.resource("s3")
        bucket = s3.Bucket(self.bucket_name)
        try:
            try:
                bucket.objects.all().delete()
            except botocore.exceptions.ClientError as e:
                if e.response["Error"]["Code"] == "404":
                    # the key was not reachable - pass
                    pass
                else:
                    raise
            finally:
                bucket = s3.Bucket(self.bucket_name)
                bucket.delete()
        finally:
            # Stop the S3 mock.
            self.mock_s3.stop()

    def test_get_last_group(self):
        s3 = boto3.resource("s3")
        bucket = s3.Bucket(self.bucket_name)

        last_group = get_last_group(bucket, "TestCam", "20221102")
        self.assertEqual(last_group, "2022-11-02T00:00:00.000002")

        # Test the case of no match
        last_group = get_last_group(bucket, "TestCam", "20110101")
        self.assertEqual(last_group, "2011-01-01T00:00:00.000000")

    def test_exposure_id_hsc(self):
        group = "2023011100026"
        # Need a Butler registry to test IdGenerator
        with tempfile.TemporaryDirectory() as repo:
            butler = butler_tests.makeTestRepo(repo)
            HyperSuprimeCam().register(butler.registry)
            instruments = list(butler.registry.queryDimensionRecords(
                "instrument", dataId={"instrument": "HSC"}))
            self.assertEqual(len(instruments), 1)
            exp_max = instruments[0].exposure_max

            exp_id, headers = make_exposure_id("HSC", group, 0)
            butler_tests.addDataIdValue(butler, "visit", exp_id)
            data_id = butler.registry.expandDataId({"instrument": "HSC", "visit": exp_id, "detector": 111})

        str_exp_id = headers["EXP-ID"]
        self.assertEqual(str_exp_id, "HSCE%08d" % exp_id)
        # Above assertion passes if exp_id has 9+ digits, but such IDs aren't valid.
        self.assertEqual(len(str_exp_id[4:]), 8)
        self.assertLessEqual(exp_id, exp_max)
        # test that IdGenerator.unpacker_from_config does not raise
        config = lsst.meas.base.DetectorVisitIdGeneratorConfig()
        lsst.meas.base.IdGenerator.unpacker_from_config(config, data_id)

    def test_exposure_id_hsc_limits(self):
        # Confirm that the exposure ID generator works as long as advertised:
        # until the end of September 2024.
        exp_id, _ = make_exposure_id("HSC", "2024-09-30T00:00:00.009999", 0)
        self.assertEqual(exp_id, 21309999)
        with self.assertRaises(RuntimeError):
            make_exposure_id("HSC", "2024-10-01T00:00:00.000000", 0)


class TesterDateHandlingTest(unittest.TestCase):
    def _check_day_obs(self, year, month, day):
        midnight = datetime.datetime(year, month, day, 4, 0, 0, tzinfo=datetime.timezone.utc) \
            + datetime.timedelta(days=1)  # At midnight, day_obs is the previous day
        day_obs = int(f"{year}{month:02d}{day:02d}")
        self.assertAlmostEqual(day_obs_to_unix_utc(day_obs), calendar.timegm(midnight.utctimetuple()))

    def test_day_obs_to_unit_utc(self):
        for (y, m, d) in [(2023, 8, 9),
                          (2025, 4, 30),
                          (1998, 12, 31),
                          (2000, 1, 1),
                          ]:
            self._check_day_obs(y, m, d)


class TesterGoupIdTest(unittest.TestCase):
    """Test the utilities on the made-up group ID"""

    def test_round_trip(self):
        for day_obs, seq_num in [
            ("20230123", 456),
            ("20241231", 1000),
        ]:
            self.assertEqual(
                (day_obs, seq_num), decode_group(make_group(day_obs, seq_num))
            )

        for group_id in ["2023-01-23T00:00:00.000456", "2024-12-31T00:00:00.001000"]:
            self.assertEqual(group_id, make_group(*decode_group(group_id)))

    def test_increment_group(self):
        group_base = "2024-12-31T00:00:00.001000"
        offsets = {
            1: "2024-12-31T00:00:00.001001",
            99: "2024-12-31T00:00:00.001099",
            1345: "2024-12-31T00:00:00.002345",
        }
        for amount in offsets:
            self.assertEqual(
                offsets[amount], increment_group("LATISS", group_base, amount)
            )
