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

import tempfile
import os.path
import unittest
import unittest.mock

import astropy.units as u

import astro_metadata_translator
from lsst.daf.butler import Butler, DataCoordinate
from lsst.obs.base.formatters.fitsExposure import FitsImageFormatter
from lsst.obs.base.ingest import RawFileDatasetInfo, RawFileData
import lsst.resources

from activator.middleware_interface import MiddlewareInterface
from activator.visit import Visit


def fake_file_data(filename, dimensions, instrument):
    """Return file data for a mock file to be ingested.

    Parameters
    ----------
    filename : `str`
        Full path to the file to mock. Can be a non-existant file.
    dimensions : `lsst.daf.butler.DimensionsUniverse`
        The full set of dimensions for this butler.
    instrument : `lsst.obs.base.Instrument`
        The instrument the file is supposed to be from.

    Returns
    -------
    data_id, file_data, : `DataCoordinate`, `RawFileData`
        The id and descriptor for the mock file.
    """
    data_id = DataCoordinate.standardize({"exposure": 1, "detector": 1, "instrument": instrument.getName()},
                                         universe=dimensions)
    obs_info = astro_metadata_translator.makeObservationInfo(instrument=instrument.getName(),
                                                             exposure_id=1,
                                                             observation_id="1",
                                                             physical_filter="r",
                                                             exposure_time=30.0*u.second)
    dataset_info = RawFileDatasetInfo(data_id, obs_info)
    file_data = RawFileData([dataset_info],
                            lsst.resources.ResourcePath(filename),
                            FitsImageFormatter,
                            instrument)
    return data_id, file_data


class MiddlewareInterfaceTest(unittest.TestCase):
    """Test the MiddlewareInterface class with faked data.
    """
    def setUp(self):
        data_dir = os.path.join(os.path.abspath(os.path.dirname(__file__)), "data")
        input_repo = os.path.join(data_dir, "input_repo")
        instrument = "lsst.obs.lsst.LsstCam"
        self.input_data = os.path.join(data_dir, "input_data")
        self.repo = tempfile.TemporaryDirectory()
        butler = Butler(Butler.makeRepo(self.repo.name), writeable=True)
        self.interface = MiddlewareInterface(input_repo, self.input_data, instrument, butler,
                                             prefix="file://")
        self.next_visit = Visit(instrument,
                                detector=1,
                                group=1,
                                snaps=1,
                                filter="r",
                                ra=10,
                                dec=20,
                                kind="BIAS")
        self.logger_name = "lsst.activator.middleware_interface"

    def test_init(self):
        """Basic tests of the initialized interface object.
        """
        # Ideas for things to test:
        # * On init, does the right kind of butler get created, with the right
        #   collections, etc?
        # * On init, is the local butler repo purely in memory?

        # Check that the butler instance is properly configured.
        instruments = list(self.interface.butler.registry.queryDimensionRecords("instrument"))
        self.assertEqual("LSSTCam", instruments[0].name)

        # Check that the ingester is properly configured.
        self.assertEqual(self.interface.rawIngestTask.config.failFast, True)
        self.assertEqual(self.interface.rawIngestTask.config.transfer, "copy")

    def test_prep_butler(self):
        """Test that the butler has all necessary data for the next visit.
        """
        with self.assertLogs(self.logger_name, level="INFO") as cm:
            self.interface.prep_butler(self.next_visit)
        msg = f"INFO:{self.logger_name}:Preparing Butler for visit '{self.next_visit}'"
        self.assertEqual(cm.output, [msg])
        # TODO: Test that we have appropriate refcats?

    def test_ingest_image(self):
        filename = "fakeRawImage.fits"
        filepath = os.path.join(self.input_data, filename)
        data_id, file_data = fake_file_data(filepath,
                                            self.interface.butler.dimensions,
                                            self.interface.instrument)
        with unittest.mock.patch.object(self.interface.rawIngestTask, "extractMetadata") as mock:
            mock.return_value = file_data
            self.interface.ingest_image(filename)

            datasets = list(self.interface.butler.registry.queryDatasets('raw',
                                                                         collections=['LSSTCam/raw/all']))
            self.assertEqual(datasets[0].dataId, data_id)

    def test_ingest_image_fails_missing_file(self):
        """Trying to ingest a non-existent file should raise.

        NOTE: this is currently a bit of a placeholder: I suspect we'll want to
        change how errors are handled in the interface layer, raising custom
        exceptions so that the activator can deal with them better. So even
        though all this is demonstrating is that if the file doesn't exist,
        rawIngestTask.run raises FileNotFoundError and that gets passed up
        through ingest_image(), we'll want to have a test of "missing file
        ingestion", and this can serve as a starting point.
        """
        filename = "nonexistentImage.fits"
        filepath = os.path.join(self.input_data, filename)
        data_id, file_data = fake_file_data(filepath,
                                            self.interface.butler.dimensions,
                                            self.interface.instrument)
        with unittest.mock.patch.object(self.interface.rawIngestTask, "extractMetadata") as mock, \
                self.assertRaisesRegex(FileNotFoundError, "Resource at .* does not exist"):
            mock.return_value = file_data
            self.interface.ingest_image(filename)
        # There should not be any raw files in the registry.
        datasets = list(self.interface.butler.registry.queryDatasets('raw',
                                                                     collections=['LSSTCam/raw/all']))
        self.assertEqual(datasets, [])

    def test_run_pipeline(self):
        with self.assertLogs(self.logger_name, level="INFO") as cm:
            self.interface.run_pipeline(self.next_visit, 1)
        msg = f"INFO:{self.logger_name}:Running pipeline bias.yaml on visit '{self.next_visit}', snaps 1"
        self.assertEqual(cm.output, [msg])
