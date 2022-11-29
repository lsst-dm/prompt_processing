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

import unittest

import boto3
import botocore
from moto import mock_s3

from activator.raw import get_raw_path
from tester.utils import get_last_group


class TesterUtilsTest(unittest.TestCase):
    """Test components in tester.
    """
    mock_s3 = mock_s3()
    bucket_name = "testBucketName"

    def setUp(self):
        self.mock_s3.start()
        s3 = boto3.resource("s3")
        s3.create_bucket(Bucket=self.bucket_name)

        path = get_raw_path("TestCam", 123, "2022110200001", 2, 30, "TestFilter")
        obj = s3.Object(self.bucket_name, path)
        obj.put(Body=b'test1')
        path = get_raw_path("TestCam", 123, "2022110200002", 2, 30, "TestFilter")
        obj = s3.Object(self.bucket_name, path)
        obj.put(Body=b'test2')

    def tearDown(self):
        s3 = boto3.resource("s3")
        bucket = s3.Bucket(self.bucket_name)
        try:
            bucket.objects.all().delete()
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "404":
                # the key was not reachable - pass
                pass
            else:
                raise

        bucket = s3.Bucket(self.bucket_name)
        bucket.delete()

        # Stop the S3 mock.
        self.mock_s3.stop()

    def test_get_last_group(self):
        s3 = boto3.resource("s3")
        bucket = s3.Bucket(self.bucket_name)

        last_group = get_last_group(bucket, "TestCam", "20221102")
        self.assertEqual(last_group, 2022110200002)

        # Test the case of no match
        last_group = get_last_group(bucket, "TestCam", "20110101")
        self.assertEqual(last_group, int(20110101) * 100_000)
