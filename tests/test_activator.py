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


import os.path
import signal
import time
import unittest

import astropy.coordinates
import astropy.time
import astropy.units as u

import shared.raw
from shared.visit import FannedOutVisit
import activator.setup

# Mandatory envvars are loaded at import time
os.environ["RUBIN_INSTRUMENT"] = "LSSTCam"
os.environ["SKYMAP"] = "lsst_cells_v1"
os.environ["CENTRAL_REPO"] = os.path.join(os.path.abspath(os.path.dirname(__file__)), "data", "central_repo")
os.environ["S3_ENDPOINT_URL"] = "https://this.is.a.test"
os.environ["IMAGE_BUCKET"] = "test-bucket-test"
os.environ["KAFKA_CLUSTER"] = ""
os.environ["PREPROCESSING_PIPELINES_CONFIG"] = "- pipelines: []"
os.environ["MAIN_PIPELINES_CONFIG"] = "- pipelines: []"

from activator.activator import _filter_exposures, _ingest_existing_raws, is_processable, \
    time_since, with_signal  # noqa: E402, no code before imports


# Use of @ServiceSetup.check_on_init in activator interferes with unit tests of ServiceSetup itself
# TODO: find a cleaner way to isolate the tests
activator.setup.ServiceSetup.reset()


# TODO: find a way to test functions that take `confluent_kafka.Message` inputs or mock Kafka consumers
# TODO: find a way to test functions that take S3 notification objects


class FilterExposuresTest(unittest.TestCase):
    def setUp(self):
        super().setUp()

        expid = 42
        rot = -45.0
        self.visit = FannedOutVisit(instrument=os.environ["RUBIN_INSTRUMENT"],
                                    detector=90,
                                    groupId=str(expid),
                                    nimages=1,
                                    filters="k0123",
                                    coordinateSystem=FannedOutVisit.CoordSys.ICRS,
                                    position=[-42.0, 38.2],
                                    startTime=1747891209.9,
                                    rotationSystem=FannedOutVisit.RotSys.SKY,
                                    cameraAngle=rot,
                                    survey="SURVEY",
                                    salIndex=3,
                                    scriptSalIndex=3,
                                    dome=FannedOutVisit.Dome.OPEN,
                                    duration=35.0,
                                    totalCheckpoints=1,
                                    private_sndStamp=1747891150.0,
                                    )

    def test_one_pass(self):
        exposures = {42}

        def _get_angle(id):
            if id == 42:
                return self.visit.get_rotation_sky()
            # Return correct value only if _filter_exposure passes the correct ID
            else:
                return astropy.coordinates.Angle(-40.0 * u.degree)

        self.assertEqual(_filter_exposures(exposures, self.visit, _get_angle), {42})

    def test_one_fail(self):
        exposures = {42}

        def _get_angle(id):
            return astropy.coordinates.Angle(42.0 * u.degree)

        self.assertEqual(_filter_exposures(exposures, self.visit, _get_angle), set())

    def test_two_mixed(self):
        exposures = {42, 52}

        def _get_angle(id):
            if id == 42:
                return self.visit.get_rotation_sky()
            else:
                return astropy.coordinates.Angle(-40.0 * u.degree)

        self.assertEqual(_filter_exposures(exposures, self.visit, _get_angle), {42})

    def test_empty(self):
        exposures = set()

        def _get_angle(id):
            if id == 42:
                return self.visit.get_rotation_sky()
            else:
                return astropy.coordinates.Angle(-40.0 * u.degree)

        self.assertEqual(_filter_exposures(exposures, self.visit, _get_angle), set())


class IngestExistingRawsTest(unittest.TestCase):
    def setUp(self):
        super().setUp()

        def _mock_ingest(oid):
            return shared.raw.get_exp_id_from_oid(oid)

        self.ingester = unittest.mock.Mock(side_effect=_mock_ingest)
        self.visit = FannedOutVisit(instrument=os.environ["RUBIN_INSTRUMENT"],
                                    detector=90,
                                    groupId="88",
                                    nimages=1,
                                    filters="k0123",
                                    coordinateSystem=FannedOutVisit.CoordSys.ICRS,
                                    position=[-42.0, 38.2],
                                    startTime=1747891209.9,
                                    rotationSystem=FannedOutVisit.RotSys.SKY,
                                    cameraAngle=0,
                                    survey="SURVEY",
                                    salIndex=3,
                                    scriptSalIndex=3,
                                    dome=FannedOutVisit.Dome.OPEN,
                                    duration=35.0,
                                    totalCheckpoints=1,
                                    private_sndStamp=1747891150.0,
                                    )

    def test_one_ingest(self):
        known_ids = set()
        oid = os.environ["RUBIN_INSTRUMENT"] + "/20250926/blah_blah_20250926_42" \
            "/blah_blah_20250926_42_R00_S00.fits"
        with unittest.mock.patch("activator.activator.check_for_snap", return_value=oid), \
                unittest.mock.patch("activator.activator._get_storage_client"):
            _ingest_existing_raws(self.visit, 1, self.ingester, known_ids)
        self.assertEqual(known_ids, {2025092600042})
        self.ingester.assert_called_once_with(oid)

    def test_one_ingest_missing(self):
        known_ids = set()
        with unittest.mock.patch("activator.activator.check_for_snap", return_value=None), \
                unittest.mock.patch("activator.activator._get_storage_client"):
            _ingest_existing_raws(self.visit, 1, self.ingester, known_ids)
        self.assertEqual(known_ids, set())
        self.ingester.assert_not_called()

    def test_one_ingest_duplicate(self):
        known_ids = {2025092600042}
        oid = os.environ["RUBIN_INSTRUMENT"] + "/20250926/blah_blah_20250926_42" \
            "/blah_blah_20250926_42_R00_S00.fits"
        with unittest.mock.patch("activator.activator.check_for_snap", return_value=oid), \
                unittest.mock.patch("activator.activator._get_storage_client"):
            _ingest_existing_raws(self.visit, 1, self.ingester, known_ids)
        self.assertEqual(known_ids, {2025092600042})
        self.ingester.assert_not_called()

    def test_one_ingest_truncated(self):
        known_ids = set()
        oids = [os.environ["RUBIN_INSTRUMENT"] + f"/20250926/blah_blah_20250926_{seq}"
                f"/blah_blah_20250926_{seq}_R00_S00.fits"
                for seq in [42, 43]]
        with unittest.mock.patch("activator.activator.check_for_snap", side_effect=oids), \
                unittest.mock.patch("activator.activator._get_storage_client"):
            _ingest_existing_raws(self.visit, 1, self.ingester, known_ids)
        # Second snap ignored
        self.assertEqual(known_ids, {2025092600042})
        self.ingester.assert_called_once_with(oids[0])

    def test_two_ingest(self):
        known_ids = set()
        oids = [os.environ["RUBIN_INSTRUMENT"] + f"/20250926/blah_blah_20250926_{seq}"
                f"/blah_blah_20250926_{seq}_R00_S00.fits"
                for seq in [42, 43]]
        with unittest.mock.patch("activator.activator.check_for_snap", side_effect=oids), \
                unittest.mock.patch("activator.activator._get_storage_client"):
            _ingest_existing_raws(self.visit, 2, self.ingester, known_ids)
        self.assertEqual(known_ids, {2025092600042, 2025092600043})
        self.assertEqual(self.ingester.call_count, 2)
        for oid in oids:
            self.ingester.assert_any_call(oid)

    def test_two_ingest_duplicate(self):
        known_ids = {2025092600042}
        oids = [os.environ["RUBIN_INSTRUMENT"] + f"/20250926/blah_blah_20250926_{seq}"
                f"/blah_blah_20250926_{seq}_R00_S00.fits"
                for seq in [42, 43]]
        with unittest.mock.patch("activator.activator.check_for_snap", side_effect=oids), \
                unittest.mock.patch("activator.activator._get_storage_client"):
            _ingest_existing_raws(self.visit, 2, self.ingester, known_ids)
        self.assertEqual(known_ids, {2025092600042, 2025092600043})
        self.ingester.assert_called_once_with(oids[1])


class IsProcessableTest(unittest.TestCase):
    def setUp(self):
        super().setUp()

        now = astropy.time.Time.now().replicate(format="unix_tai")
        # Generated 10 minutes before now, observed 8 minutes before
        self.visit = FannedOutVisit(instrument=os.environ["RUBIN_INSTRUMENT"],
                                    detector=90,
                                    groupId="88",
                                    nimages=1,
                                    filters="k0123",
                                    coordinateSystem=FannedOutVisit.CoordSys.ICRS,
                                    position=[-42.0, 38.2],
                                    startTime=float(now.value) - 500.0,
                                    rotationSystem=FannedOutVisit.RotSys.SKY,
                                    cameraAngle=0,
                                    survey="SURVEY",
                                    salIndex=3,
                                    scriptSalIndex=3,
                                    dome=FannedOutVisit.Dome.OPEN,
                                    duration=35.0,
                                    totalCheckpoints=1,
                                    private_sndStamp=float(now.value) - 600.0,
                                    )

    def test_long_expiration(self):
        self.assertTrue(is_processable(self.visit, 3600.0))
        self.assertTrue(is_processable(self.visit, 610.0))  # Pad for possible slow run time

    def test_short_expiration(self):
        self.assertFalse(is_processable(self.visit, 100.0))
        self.assertFalse(is_processable(self.visit, 499.9))

    def test_mid_expiration(self):
        # Right now, messages are rejected based on message age, not time since exposure.
        # This may change in the future.
        self.assertFalse(is_processable(self.visit, 550.0))
        self.assertFalse(is_processable(self.visit, 510.0))  # Pad for possible slow run time
        self.assertFalse(is_processable(self.visit, 599.9))


class TimeSinceTest(unittest.TestCase):
    def test_timed_time_since(self):
        start = time.time()
        time.sleep(2.0)
        self.assertGreater(time_since(start), 2.0)
        self.assertLess(time_since(start), 3.0)  # Pad for possible slow run time


class WithSignalTest(unittest.TestCase):
    def setUp(self):
        super().setUp()

        self.handler = unittest.mock.Mock()

    @unittest.removeHandler
    def test_no_default(self):
        # This test uses SIGPIPE because it's ignored by default
        def _ping():
            signal.raise_signal(signal.SIGPIPE)

        ping = with_signal(signal.SIGPIPE, self.handler)(_ping)

        _ping()
        self.handler.assert_not_called()
        self.handler.reset_mock()

        ping()
        self.handler.assert_called_once_with(signal.SIGPIPE, unittest.mock.ANY)
        self.handler.reset_mock()

        _ping()
        self.handler.assert_not_called()
        self.handler.reset_mock()

    @unittest.removeHandler
    def test_old_handler(self):
        old_handler = unittest.mock.Mock()
        signal.signal(signal.SIGUSR1, old_handler)
        self.addCleanup(signal.signal, signal.SIGUSR1, signal.SIG_DFL)

        def _ping():
            signal.raise_signal(signal.SIGUSR1)

        ping = with_signal(signal.SIGUSR1, self.handler)(_ping)

        _ping()
        old_handler.assert_called_once_with(signal.SIGUSR1, unittest.mock.ANY)
        self.handler.assert_not_called()
        old_handler.reset_mock()
        self.handler.reset_mock()

        ping()
        old_handler.assert_not_called()
        self.handler.assert_called_once_with(signal.SIGUSR1, unittest.mock.ANY)
        old_handler.reset_mock()
        self.handler.reset_mock()

        _ping()
        old_handler.assert_called_once_with(signal.SIGUSR1, unittest.mock.ANY)
        self.handler.assert_not_called()
        old_handler.reset_mock()
        self.handler.reset_mock()
