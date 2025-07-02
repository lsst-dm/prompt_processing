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

import os
import tempfile
import unittest
import zipfile

import boto3

from lsst.resources import ResourcePath, s3utils

from shared.astropy import import_iers_cache, LOCAL_CACHE

try:
    from moto import mock_aws
except ImportError:
    from moto import mock_s3 as mock_aws  # Backwards-compatible with moto 4


class IersCacheTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.mock_aws = mock_aws()

    @classmethod
    def _make_iers_cache(cls, path):
        """Create an IERS cache to use for testing.

        Parameters
        ----------
        path : `lsst.resources.ResourcePath`
            The location at which to create the cache.
        """
        with tempfile.NamedTemporaryFile(mode="wb", suffix=".zip") as temp:
            with zipfile.ZipFile(temp, mode="w"):
                # Don't actually need any contents
                pass
            path.transfer_from(ResourcePath(temp.name), transfer="copy")

    def setUp(self):
        super().setUp()
        self.enterContext(s3utils.clean_test_environment_for_s3())
        # Local cache should not exist before tests
        try:
            os.remove(LOCAL_CACHE)
        except FileNotFoundError:
            pass

        mock_endpoint = "https://this.is.a.test"
        self.bucket = "test-bucket-test"
        path = ResourcePath("test/test-cache.zip", root="s3://" + self.bucket)

        self.enterContext(unittest.mock.patch.dict(
            os.environ,
            {"MOTO_S3_CUSTOM_ENDPOINTS": mock_endpoint,
             "S3_ENDPOINT_URL": mock_endpoint,
             "CENTRAL_IERS_CACHE": path.geturl(),
             }))

        self.enterContext(self.mock_aws)
        s3 = boto3.resource("s3")
        s3.create_bucket(Bucket=self.bucket)

        self._make_iers_cache(path)

    def test_import_iers_cache(self):
        with unittest.mock.patch("astropy.utils.data.import_download_cache") as mock_import:
            import_iers_cache()
        mock_import.assert_called_once()
        self.assertEqual(mock_import.call_args.kwargs["update_cache"], True)

    def test_import_iers_cache_twice(self):
        with unittest.mock.patch("astropy.utils.data.import_download_cache") as mock_import:
            import_iers_cache()
            # Local cache should exist
            import_iers_cache()
        # Should import both times to ensure the latest version is used
        self.assertEqual(mock_import.call_count, 2)
        self.assertEqual(mock_import.call_args.kwargs["update_cache"], True)
