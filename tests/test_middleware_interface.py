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
import datetime
import itertools
import tempfile
import os.path
import unittest
import unittest.mock
import warnings

import astropy.coordinates
import astropy.time
import astropy.units as u
import psycopg2

import astro_metadata_translator
import lsst.pex.config
import lsst.afw.image
import lsst.afw.table
from lsst.daf.butler import Butler, CollectionType, DataCoordinate
import lsst.daf.butler.tests as butler_tests
from lsst.obs.base.formatters.fitsExposure import FitsImageFormatter
from lsst.obs.base.ingest import RawFileDatasetInfo, RawFileData
import lsst.resources

from activator.config import PipelinesConfig
from activator.exception import NonRetriableError
from activator.visit import FannedOutVisit
from activator.middleware_interface import get_central_butler, make_local_repo, _get_sasquatch_dispatcher, \
    MiddlewareInterface, \
    _filter_datasets, _prepend_collection, _remove_from_chain, _filter_calibs_by_date, _MissingDatasetError

# The short name of the instrument used in the test repo.
instname = "DECam"
# Full name of the physical filter for the test file.
filter = "g DECam SDSS c0001 4720.0 1520.0"
# The skymap name used in the test repo.
skymap_name = "decam_rings_v1"
# A pipelines config that returns the test pipelines.
# Unless a test imposes otherwise, the first pipeline should run, and
# the second should not be attempted.
pipelines = PipelinesConfig('''(survey="SURVEY")=[${PROMPT_PROCESSING_DIR}/tests/data/ApPipe.yaml,
                                                  ${PROMPT_PROCESSING_DIR}/tests/data/SingleFrame.yaml]
                            '''
                            )


def fake_file_data(filename, dimensions, instrument, visit):
    """Return file data for a mock file to be ingested.

    Parameters
    ----------
    filename : `str`
        Full path to the file to mock. Can be a non-existant file.
    dimensions : `lsst.daf.butler.DimensionsUniverse`
        The full set of dimensions for this butler.
    instrument : `lsst.obs.base.Instrument`
        The instrument the file is supposed to be from.
    visit : `FannedOutVisit`
        Group of snaps from one detector to be processed.

    Returns
    -------
    data_id, file_data, : `DataCoordinate`, `RawFileData`
        The id and descriptor for the mock file.
    """
    exposure_id = int(visit.groupId)
    data_id = DataCoordinate.standardize({"exposure": exposure_id,
                                          "detector": visit.detector,
                                          "instrument": instrument.getName()},
                                         universe=dimensions)

    start_time = astropy.time.Time("2015-02-18T05:28:18.716517500", scale="tai")
    obs_info = astro_metadata_translator.makeObservationInfo(
        instrument=instrument.getName(),
        datetime_begin=start_time,
        datetime_end=start_time + 30*u.second,
        exposure_id=exposure_id,
        visit_id=exposure_id,
        boresight_rotation_angle=astropy.coordinates.Angle(visit.cameraAngle*u.degree),
        boresight_rotation_coord=visit.rotationSystem.name.lower(),
        tracking_radec=astropy.coordinates.SkyCoord(*visit.position, frame="icrs", unit="deg"),
        observation_id=visit.groupId,
        physical_filter=filter,
        exposure_time=30.0*u.second,
        observation_type="science",
        group_counter_start=exposure_id,
        group_counter_end=exposure_id,
    )
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
        self.data_dir = os.path.join(os.path.abspath(os.path.dirname(__file__)), "data")
        self.central_repo = os.path.join(self.data_dir, "central_repo")
        self.umbrella = f"{instname}/defaults"
        self.central_butler = Butler(self.central_repo,
                                     collections=[self.umbrella],
                                     writeable=False,
                                     inferDefaults=False)
        self.input_data = os.path.join(self.data_dir, "input_data")
        self.local_repo = make_local_repo(tempfile.gettempdir(), self.central_butler, instname)

        env_patcher = unittest.mock.patch.dict(os.environ,
                                               {"URL_APDB": f"sqlite:///{self.local_repo.name}/apdb.db",
                                                "K_REVISION": "prompt-proto-service-042",
                                                })
        env_patcher.start()
        self.addCleanup(env_patcher.stop)

        # coordinates from DECam data in ap_verify_ci_hits2015 for visit 411371
        ra = 155.4702849608958
        dec = -4.950050405424033
        # DECam has no rotator; instrument angle is 90 degrees in our system.
        rot = 90.
        self.next_visit = FannedOutVisit(instrument=instname,
                                         detector=56,
                                         groupId="1",
                                         nimages=1,
                                         filters=filter,
                                         coordinateSystem=FannedOutVisit.CoordSys.ICRS,
                                         position=[ra, dec],
                                         startTime=1424237500.0,
                                         rotationSystem=FannedOutVisit.RotSys.SKY,
                                         cameraAngle=rot,
                                         survey="SURVEY",
                                         salIndex=42,
                                         scriptSalIndex=42,
                                         dome=FannedOutVisit.Dome.OPEN,
                                         duration=35.0,
                                         totalCheckpoints=1,
                                         private_sndStamp=1424237298.7165175,
                                         )
        self.logger_name = "lsst.activator.middleware_interface"
        self.interface = MiddlewareInterface(self.central_butler, self.input_data, self.next_visit,
                                             pipelines, skymap_name, self.local_repo.name,
                                             prefix="file://")

    def tearDown(self):
        super().tearDown()
        # TemporaryDirectory warns on leaks
        self.local_repo.cleanup()

    def test_get_butler(self):
        for butler in [get_central_butler(self.central_repo, "lsst.obs.decam.DarkEnergyCamera"),
                       get_central_butler(self.central_repo, instname),
                       ]:
            # TODO: better way to test repo location?
            self.assertTrue(
                butler.getURI("skyMap", skymap=skymap_name, run="foo", predict=True).ospath
                .startswith(self.central_repo))
            self.assertEqual(list(butler.collections), [f"{instname}/defaults"])
            self.assertTrue(butler.isWriteable())

    def test_make_local_repo(self):
        for inst in [instname, "lsst.obs.decam.DarkEnergyCamera"]:
            with make_local_repo(tempfile.gettempdir(), Butler(self.central_repo), inst) as repo_dir:
                self.assertTrue(os.path.exists(repo_dir))
                butler = Butler(repo_dir)
                self.assertEqual([x.dataId for x in butler.registry.queryDimensionRecords("instrument")],
                                 [DataCoordinate.standardize({"instrument": instname},
                                                             universe=butler.dimensions)])
                self.assertIn(f"{instname}/defaults", butler.registry.queryCollections())
            self.assertFalse(os.path.exists(repo_dir))

    def test_init(self):
        """Basic tests of the initialized interface object.
        """
        # Ideas for things to test:
        # * On init, does the right kind of butler get created, with the right
        #   collections, etc?
        # * On init, is the local butler repo purely in memory?

        # Check that the butler instance is properly configured.
        instruments = list(self.interface.butler.registry.queryDimensionRecords("instrument"))
        self.assertEqual(instname, instruments[0].name)
        self.assertEqual(set(self.interface.butler.collections), {self.umbrella})

        # Check that the ingester is properly configured.
        self.assertEqual(self.interface.rawIngestTask.config.failFast, True)
        self.assertEqual(self.interface.rawIngestTask.config.transfer, "copy")

    def _check_imports(self, butler, detector, expected_shards, expected_date):
        """Test that the butler has the expected supporting data.
        """
        self.assertEqual(butler.get('camera',
                                    instrument=instname,
                                    collections=[f"{instname}/calib/unbounded"]).getName(), instname)

        # Check that the right skymap is in the chained output collection.
        self.assertTrue(
            butler.exists("skyMap",
                          skymap=skymap_name,
                          full_check=True,
                          collections=self.umbrella)
        )

        # check that we got appropriate refcat shards
        loaded_shards = butler.registry.queryDataIds("htm7",
                                                     datasets="gaia_dr2_20200414",
                                                     collections="refcats")

        self.assertEqual(expected_shards, {x['htm7'] for x in loaded_shards})
        # Check that the right calibs are in the chained output collection.
        self.assertTrue(
            butler.exists('cpBias', detector=detector, instrument='DECam',
                          full_check=True,
                          # TODO: Have to use the exact run collection, because we can't
                          # query by validity range.
                          # collections=self.umbrella)
                          collections=f"DECam/calib/{expected_date}")
        )
        self.assertTrue(
            butler.exists('cpFlat', detector=detector, instrument='DECam',
                          physical_filter=filter,
                          full_check=True,
                          # TODO: Have to use the exact run collection, because we can't
                          # query by validity range.
                          # collections=self.umbrella)
                          collections=f"DECam/calib/{expected_date}")
        )
        # Check that we got a model (only one in the test data)
        self.assertTrue(
            butler.exists('pretrainedModelPackage',
                          full_check=True,
                          collections=self.umbrella)
        )

        # Check that the right templates are in the chained output collection.
        # Need to refresh the butler to get all the dimensions/collections.
        butler.registry.refresh()
        for patch in (7, 8):
            self.assertTrue(
                butler.exists('goodSeeingCoadd', tract=8604, patch=patch, band="g",
                              skymap=skymap_name,
                              full_check=True,
                              collections=self.umbrella)
            )
        self.assertFalse(
            butler.exists('goodSeeingCoadd', tract=8604, patch=0, band="g",
                          skymap=skymap_name,
                          full_check=True,
                          collections=self.umbrella)
        )

    def test_prep_butler(self):
        """Test that the butler has all necessary data for the next visit.
        """
        self.interface.prep_butler()

        # These shards were identified by plotting the objects in each shard
        # on-sky and overplotting the detector corners.
        # TODO DM-34112: check these shards again with some plots, once I've
        # determined whether ci_hits2015 actually has enough shards.
        expected_shards = {157394, 157401, 157405}
        self._check_imports(self.interface.butler, detector=56,
                            expected_shards=expected_shards, expected_date="20150218T000000Z")

    def test_prep_butler_olddate(self):
        """Test that prep_butler returns only calibs from a particular date range.
        """
        self.interface.visit = dataclasses.replace(
            self.interface.visit,
            private_sndStamp=datetime.datetime.fromisoformat("20150313T000000Z").timestamp(),
        )
        self.interface.prep_butler()

        # These shards were identified by plotting the objects in each shard
        # on-sky and overplotting the detector corners.
        # TODO DM-34112: check these shards again with some plots, once I've
        # determined whether ci_hits2015 actually has enough shards.
        expected_shards = {157394, 157401, 157405}
        with self.assertRaises((AssertionError, lsst.daf.butler.registry.MissingCollectionError)):
            # 20150218T000000Z run should not be imported
            self._check_imports(self.interface.butler, detector=56,
                                expected_shards=expected_shards, expected_date="20150218T000000Z")
        self._check_imports(self.interface.butler, detector=56,
                            expected_shards=expected_shards, expected_date="20150313T000000Z")

    # TODO: prep_butler doesn't know what kinds of calibs to expect, so can't
    # tell that there are specifically, e.g., no flats. This test should pass
    # as-is after DM-40245.
    @unittest.expectedFailure
    def test_prep_butler_novalid(self):
        """Test that prep_butler raises if no calibs are currently valid.
        """
        self.interface.visit = dataclasses.replace(
            self.interface.visit,
            private_sndStamp=datetime.datetime(2050, 1, 1).timestamp(),
        )

        with warnings.catch_warnings():
            # Avoid "dubious year" warnings from using a 2050 date
            warnings.simplefilter("ignore", category=astropy.utils.exceptions.ErfaWarning)
            with self.assertRaises(_MissingDatasetError):
                self.interface.prep_butler()

    def test_prep_butler_twice(self):
        """prep_butler should have the correct calibs (and not raise an
        exception!) on a second run with the same, or a different detector.
        This explicitly tests the "you can't import something that's already
        in the local butler" problem that's related to the "can't register
        the skymap in init" problem.
        """
        self.interface.prep_butler()

        # Second visit with everything same except group.
        second_visit = dataclasses.replace(self.next_visit, groupId=str(int(self.next_visit.groupId) + 1))
        second_interface = MiddlewareInterface(self.central_butler, self.input_data, second_visit,
                                               pipelines, skymap_name, self.local_repo.name,
                                               prefix="file://")

        second_interface.prep_butler()
        expected_shards = {157394, 157401, 157405}
        self._check_imports(second_interface.butler, detector=56,
                            expected_shards=expected_shards, expected_date="20150218T000000Z")

        # Third visit with different detector and coordinates.
        # Only 5, 10, 56, 60 have valid calibs.
        third_visit = dataclasses.replace(second_visit,
                                          detector=5,
                                          groupId=str(int(second_visit.groupId) + 1),
                                          # Offset to put detector=5 in same templates.
                                          position=[self.next_visit.position[0] + 0.2,
                                                    self.next_visit.position[1] - 1.2],
                                          )
        third_interface = MiddlewareInterface(self.central_butler, self.input_data, third_visit,
                                              pipelines, skymap_name, self.local_repo.name,
                                              prefix="file://")
        third_interface.prep_butler()
        expected_shards.update({157393, 157395})
        self._check_imports(third_interface.butler, detector=5,
                            expected_shards=expected_shards, expected_date="20150218T000000Z")

    def test_ingest_image(self):
        self.interface.prep_butler()  # Ensure raw collections exist.
        filename = "fakeRawImage.fits"
        filepath = os.path.join(self.input_data, filename)
        data_id, file_data = fake_file_data(filepath,
                                            self.interface.butler.dimensions,
                                            self.interface.instrument,
                                            self.next_visit)
        with unittest.mock.patch.object(self.interface.rawIngestTask, "extractMetadata") as mock:
            mock.return_value = file_data
            exp_id = self.interface.ingest_image(filename)
            self.assertEqual(exp_id, int(self.next_visit.groupId))

            datasets = list(self.interface.butler.registry.queryDatasets('raw',
                                                                         collections=[f'{instname}/raw/all']))
            self.assertEqual(datasets[0].dataId, data_id)
            # TODO: After raw ingest, we can define exposure dimension records
            # and check that the visits are defined

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
        self.interface.prep_butler()  # Ensure raw collections exist.
        filename = "nonexistentImage.fits"
        filepath = os.path.join(self.input_data, filename)
        data_id, file_data = fake_file_data(filepath,
                                            self.interface.butler.dimensions,
                                            self.interface.instrument,
                                            self.next_visit)
        with unittest.mock.patch.object(self.interface.rawIngestTask, "extractMetadata") as mock, \
                self.assertRaisesRegex(FileNotFoundError, "Resource at .* does not exist"):
            mock.return_value = file_data
            self.interface.ingest_image(filename)
        # There should not be any raw files in the registry.
        datasets = list(self.interface.butler.registry.queryDatasets('raw',
                                                                     collections=[f'{instname}/raw/all']))
        self.assertEqual(datasets, [])

    def _prepare_run_pipeline(self):
        # Have to setup the data so that we can create the pipeline executor.
        self.interface.prep_butler()
        filename = "fakeRawImage.fits"
        filepath = os.path.join(self.input_data, filename)
        data_id, file_data = fake_file_data(filepath,
                                            self.interface.butler.dimensions,
                                            self.interface.instrument,
                                            self.next_visit)
        with unittest.mock.patch.object(self.interface.rawIngestTask, "extractMetadata") as mock:
            mock.return_value = file_data
            self.interface.ingest_image(filename)

    def test_run_pipeline(self):
        """Test that running the pipeline uses the correct arguments.

        We can't run an actual pipeline because raw/calib/refcat/template data
        are all zeroed out.
        """
        self._prepare_run_pipeline()

        with unittest.mock.patch(
                "activator.middleware_interface.SeparablePipelineExecutor.pre_execute_qgraph") \
                as mock_preexec, \
             unittest.mock.patch("activator.middleware_interface.SeparablePipelineExecutor.run_pipeline") \
                as mock_run:
            with self.assertLogs(self.logger_name, level="INFO") as logs:
                self.interface.run_pipeline({1})
        # Pre-execution and execution should only run once, even if graph
        # generation is attempted for multiple pipelines.
        mock_preexec.assert_called_once()
        # Pre-execution may have other arguments as needed; no requirement either way.
        self.assertEqual(mock_preexec.call_args.kwargs["register_dataset_types"], True)
        mock_run.assert_called_once()
        # Check that we configured the right pipeline.
        self.assertIn("End to end Alert Production pipeline specialized for HiTS-2015",
                      "\n".join(logs.output))

    def _check_run_pipeline_fallback(self, pipe_files, graphs, final_label):
        """Generic test for different fallback scenarios.

        Parameters
        ----------
        pipe_files : sequence [`str`]
            The list of pipeline files configured for a visit.
        graphs : sequence [`collections.abc.Sized`]
            The list of quantum graphs (or suitable mocks) generated for each
            pipeline. Must have the same length as ``pipe_files``.
        final_label : `str`
            The description of the pipeline that should be run, given
            ``pipe_files`` and ``graphs``.
        """
        with unittest.mock.patch("activator.middleware_interface.MiddlewareInterface._get_pipeline_files",
                                 return_value=pipe_files), \
                unittest.mock.patch(
                    "activator.middleware_interface.SeparablePipelineExecutor.make_quantum_graph",
                    side_effect=graphs), \
                unittest.mock.patch(
                    "activator.middleware_interface.SeparablePipelineExecutor.pre_execute_qgraph"), \
                unittest.mock.patch("activator.middleware_interface.SeparablePipelineExecutor.run_pipeline") \
                as mock_run, \
                self.assertLogs(self.logger_name, level="INFO") as logs:
            self.interface.run_pipeline({1})
        mock_run.assert_called_once()
        # Check that we configured the right pipeline.
        self.assertIn(final_label, "\n".join(logs.output))

    def test_run_pipeline_fallback_1failof2(self):
        pipe_list = [os.path.join(self.data_dir, 'ApPipe.yaml'),
                     os.path.join(self.data_dir, 'SingleFrame.yaml')]
        graph_list = [[], ["node1", "node2"]]
        expected = "Test pipeline consisting only of single-frame steps."

        self._prepare_run_pipeline()
        self._check_run_pipeline_fallback(pipe_list, graph_list, expected)

    def test_run_pipeline_fallback_1failof2_inverse(self):
        pipe_list = [os.path.join(self.data_dir, 'ApPipe.yaml'),
                     os.path.join(self.data_dir, 'SingleFrame.yaml')]
        graph_list = [["node1", "node2"], []]
        expected = "End to end Alert Production pipeline specialized for HiTS-2015"

        self._prepare_run_pipeline()
        self._check_run_pipeline_fallback(pipe_list, graph_list, expected)

    def test_run_pipeline_fallback_2failof2(self):
        pipe_list = [os.path.join(self.data_dir, 'ApPipe.yaml'),
                     os.path.join(self.data_dir, 'SingleFrame.yaml')]
        graph_list = [[], []]
        expected = ""

        self._prepare_run_pipeline()
        with self.assertRaises(RuntimeError):
            self._check_run_pipeline_fallback(pipe_list, graph_list, expected)

    def test_run_pipeline_fallback_0failof3(self):
        pipe_list = [os.path.join(self.data_dir, 'ApPipe.yaml'),
                     os.path.join(self.data_dir, 'SingleFrame.yaml'),
                     os.path.join(self.data_dir, 'ISR.yaml')]
        graph_list = [["node1", "node2"], ["node3", "node4"], ["node5"]]
        expected = "End to end Alert Production pipeline specialized for HiTS-2015"

        self._prepare_run_pipeline()
        self._check_run_pipeline_fallback(pipe_list, graph_list, expected)

    def test_run_pipeline_fallback_1failof3(self):
        pipe_list = [os.path.join(self.data_dir, 'ApPipe.yaml'),
                     os.path.join(self.data_dir, 'SingleFrame.yaml'),
                     os.path.join(self.data_dir, 'ISR.yaml')]
        graph_list = [[], ["node3", "node4"], ["node5"]]
        expected = "Test pipeline consisting only of single-frame steps."

        self._prepare_run_pipeline()
        self._check_run_pipeline_fallback(pipe_list, graph_list, expected)

    def test_run_pipeline_fallback_2failof3(self):
        pipe_list = [os.path.join(self.data_dir, 'ApPipe.yaml'),
                     os.path.join(self.data_dir, 'SingleFrame.yaml'),
                     os.path.join(self.data_dir, 'ISR.yaml')]
        graph_list = [[], [], ["node5"]]
        expected = "Test pipeline consisting only of ISR."

        self._prepare_run_pipeline()
        self._check_run_pipeline_fallback(pipe_list, graph_list, expected)

    def test_run_pipeline_fallback_2failof3_inverse(self):
        pipe_list = [os.path.join(self.data_dir, 'ApPipe.yaml'),
                     os.path.join(self.data_dir, 'SingleFrame.yaml'),
                     os.path.join(self.data_dir, 'ISR.yaml')]
        graph_list = [[], ["node3", "node4"], []]
        expected = "Test pipeline consisting only of single-frame steps."

        self._prepare_run_pipeline()
        self._check_run_pipeline_fallback(pipe_list, graph_list, expected)

    def test_run_pipeline_bad_visits(self):
        """Test that running a pipeline that results in bad visit definition
        (because the exposure ids are wrong), raises.
        """
        # Have to setup the data so that we can create the pipeline executor.
        self.interface.prep_butler()
        filename = "fakeRawImage.fits"
        filepath = os.path.join(self.input_data, filename)
        data_id, file_data = fake_file_data(filepath,
                                            self.interface.butler.dimensions,
                                            self.interface.instrument,
                                            self.next_visit)
        with unittest.mock.patch.object(self.interface.rawIngestTask, "extractMetadata") as mock:
            mock.return_value = file_data
            self.interface.ingest_image(filename)

        with self.assertRaisesRegex(RuntimeError, "No data to process"):
            self.interface.run_pipeline({2})

    def test_run_pipeline_early_exception(self):
        """Test behavior when execution fails in single-frame processing.
        """
        self._prepare_run_pipeline()

        with unittest.mock.patch(
                "activator.middleware_interface.SeparablePipelineExecutor.pre_execute_qgraph"), \
             unittest.mock.patch("activator.middleware_interface.SeparablePipelineExecutor.run_pipeline") \
                as mock_run, \
             unittest.mock.patch("lsst.dax.apdb.ApdbSql.containsCcdVisit") as mock_query:
            mock_run.side_effect = RuntimeError("The pipeline doesn't like you.")
            mock_query.return_value = False
            with self.assertRaises(RuntimeError):
                self.interface.run_pipeline({1})

    def test_run_pipeline_late_exception(self):
        """Test behavior when execution fails in diaPipe cleanup.
        """
        self._prepare_run_pipeline()

        with unittest.mock.patch(
                "activator.middleware_interface.SeparablePipelineExecutor.pre_execute_qgraph"), \
             unittest.mock.patch("activator.middleware_interface.SeparablePipelineExecutor.run_pipeline") \
                as mock_run, \
             unittest.mock.patch("lsst.dax.apdb.ApdbSql.containsCcdVisit") as mock_query:
            mock_run.side_effect = RuntimeError("The pipeline doesn't like you.")
            mock_query.return_value = True
            with self.assertRaises(NonRetriableError):
                self.interface.run_pipeline({1})

    def test_run_pipeline_cascading_exception(self):
        """Test behavior when Butler and/or APDB access has failed completely.
        """
        self._prepare_run_pipeline()

        with unittest.mock.patch(
                "activator.middleware_interface.SeparablePipelineExecutor.pre_execute_qgraph"), \
             unittest.mock.patch("activator.middleware_interface.SeparablePipelineExecutor.run_pipeline") \
                as mock_run, \
             unittest.mock.patch("lsst.dax.apdb.ApdbSql.containsCcdVisit") as mock_query:
            mock_run.side_effect = RuntimeError("The pipeline doesn't like you.")
            mock_query.side_effect = psycopg2.OperationalError("Database? What database?")
            with self.assertRaises(NonRetriableError):
                self.interface.run_pipeline({1})

    def test_get_output_run(self):
        filename = "ApPipe.yaml"
        date = "2023-01-22"
        out_chain = self.interface._get_output_chain(date)
        self.assertEqual(out_chain, f"{instname}/prompt/output-2023-01-22")
        preload_run = self.interface._get_preload_run(date)
        self.assertEqual(preload_run, f"{instname}/prompt/output-2023-01-22/Preload/prompt-proto-service-042")
        out_run = self.interface._get_output_run(filename, date)
        self.assertEqual(out_run, f"{instname}/prompt/output-2023-01-22/ApPipe/prompt-proto-service-042")
        init_run = self.interface._get_init_output_run(filename, date)
        self.assertEqual(init_run, f"{instname}/prompt/output-2023-01-22/ApPipe/prompt-proto-service-042")

    def _assert_in_collection(self, butler, collection, dataset_type, data_id):
        # Pass iff any dataset matches the query, no need to check them all.
        for dataset in butler.registry.queryDatasets(dataset_type, collections=collection, dataId=data_id):
            return
        self.fail(f"No datasets found matching {dataset_type}@{data_id} in {collection}.")

    def _assert_not_in_collection(self, butler, collection, dataset_type, data_id):
        # Fail iff any dataset matches the query, no need to check them all.
        for dataset in butler.registry.queryDatasets(dataset_type, collections=collection, dataId=data_id):
            self.fail(f"{dataset} matches {dataset_type}@{data_id} in {collection}.")

    def test_clean_local_repo(self):
        """Test that clean_local_repo removes old datasets from the datastore.
        """
        # Safe to define custom dataset types and IDs, because the repository
        # is regenerated for each test.
        butler = self.interface.butler
        raw_data_id, _ = fake_file_data("foo.bar",
                                        butler.dimensions,
                                        self.interface.instrument,
                                        self.next_visit)
        processed_data_id = {(k if k != "exposure" else "visit"): v for k, v in raw_data_id.required.items()}
        butler_tests.addDataIdValue(butler, "exposure", raw_data_id["exposure"])
        butler_tests.addDataIdValue(butler, "visit", processed_data_id["visit"])
        butler_tests.addDatasetType(butler, "raw", raw_data_id.required.keys(), "Exposure")
        butler_tests.addDatasetType(butler, "src", processed_data_id.keys(), "SourceCatalog")
        butler_tests.addDatasetType(butler, "calexp", processed_data_id.keys(), "ExposureF")

        exp = lsst.afw.image.ExposureF(20, 20)
        cat = lsst.afw.table.SourceCatalog()
        raw_collection = self.interface.instrument.makeDefaultRawIngestRunName()
        butler.registry.registerCollection(raw_collection, CollectionType.RUN)
        out_collection = self.interface._get_output_run("ApPipe.yaml", self.interface._day_obs)
        butler.registry.registerCollection(out_collection, CollectionType.RUN)
        chain = "generic-chain"
        butler.registry.registerCollection(chain, CollectionType.CHAINED)
        butler.registry.setCollectionChain(chain, [out_collection, raw_collection])

        butler.put(exp, "raw", raw_data_id, run=raw_collection)
        butler.put(cat, "src", processed_data_id, run=out_collection)
        butler.put(exp, "calexp", processed_data_id, run=out_collection)
        self._assert_in_collection(butler, "*", "raw", raw_data_id)
        self._assert_in_collection(butler, "*", "src", processed_data_id)
        self._assert_in_collection(butler, "*", "calexp", processed_data_id)

        self.interface.clean_local_repo({raw_data_id["exposure"]})
        self._assert_not_in_collection(butler, "*", "raw", raw_data_id)
        self._assert_not_in_collection(butler, "*", "src", processed_data_id)
        self._assert_not_in_collection(butler, "*", "calexp", processed_data_id)

    @staticmethod
    def _make_expanded_ref(registry, dtype, data_id, run):
        """Make an expanded dataset ref with the given dataset type, data ID,
        and run, and the corresponding dimension records.
        """
        return lsst.daf.butler.DatasetRef(registry.getDatasetType(dtype), data_id, run=run) \
            .expanded(registry.expandDataId(data_id))

    def test_get_sasquatch_dispatcher(self):
        self.assertIsNone(_get_sasquatch_dispatcher())
        with unittest.mock.patch.dict(os.environ,
                                      {"SASQUATCH_URL": "https://localhost/dummy",
                                       }):
            self.assertIsNotNone(_get_sasquatch_dispatcher())

    def test_filter_datasets(self):
        """Test that _filter_datasets provides the correct values.
        """
        # Much easier to create DatasetRefs with a real repo.
        registry = self.central_butler.registry
        data1 = self._make_expanded_ref(registry, "cpBias", {"instrument": "DECam", "detector": 5}, "dummy")
        data2 = self._make_expanded_ref(registry, "cpBias", {"instrument": "DECam", "detector": 25}, "dummy")
        data3 = self._make_expanded_ref(registry, "cpBias", {"instrument": "DECam", "detector": 42}, "dummy")

        combinations = [{data1, data2}, {data1, data2, data3}]
        # Case where src is empty now covered in test_filter_datasets_nosrc.
        for src, existing in itertools.product(combinations, [set()] + combinations):
            diff = src - existing
            src_butler = unittest.mock.Mock(
                **{"registry.queryDatasets.return_value.expanded.return_value": src})
            existing_butler = unittest.mock.Mock(**{"registry.queryDatasets.return_value": existing})

            with self.subTest(src=sorted(ref.dataId["detector"] for ref in src),
                              existing=sorted(ref.dataId["detector"] for ref in existing)):
                result = set(_filter_datasets(src_butler, existing_butler,
                                              "cpBias", instrument="DECam"))
                src_butler.registry.queryDatasets.assert_called_once_with("cpBias", instrument="DECam")
                existing_butler.registry.queryDatasets.assert_called_once_with("cpBias", instrument="DECam")
                self.assertEqual(result, diff)

    def test_filter_datasets_nodim(self):
        """Test that _filter_datasets provides the correct values when
        the destination repository is missing not only datasets, but the
        dimensions to define them.
        """
        # Much easier to create DatasetRefs with a real repo.
        registry = self.central_butler.registry
        data1 = self._make_expanded_ref(registry, "skyMap", {"skymap": skymap_name}, "dummy")

        src_butler = unittest.mock.Mock(
            **{"registry.queryDatasets.return_value.expanded.return_value": {data1}})
        existing_butler = unittest.mock.Mock(
            **{"registry.queryDatasets.side_effect":
               lsst.daf.butler.registry.DataIdValueError(
                   f"Unknown values specified for governor dimension skymap: {{{skymap_name}}}")
               })

        result = set(_filter_datasets(src_butler, existing_butler, "skyMap", ..., skymap="mymap"))
        src_butler.registry.queryDatasets.assert_called_once_with("skyMap", ..., skymap="mymap")
        self.assertEqual(result, {data1})

    def test_filter_datasets_nosrc(self):
        """Test that _filter_datasets reports if the datasets are missing from
        the source repository, regardless of whether they are present in the
        destination repository.
        """
        # Much easier to create DatasetRefs with a real repo.
        registry = self.central_butler.registry
        data1 = self._make_expanded_ref(registry, "cpBias", {"instrument": "DECam", "detector": 42}, "dummy")

        src_butler = unittest.mock.Mock(
            **{"registry.queryDatasets.return_value.expanded.return_value": set()})
        for existing in [set(), {data1}]:
            existing_butler = unittest.mock.Mock(**{"registry.queryDatasets.return_value": existing})

            with self.subTest(existing=sorted(ref.dataId["detector"] for ref in existing)):
                with self.assertRaises(_MissingDatasetError):
                    _filter_datasets(src_butler, existing_butler, "cpBias", instrument="DECam")

    def test_prepend_collection(self):
        butler = self.interface.butler
        butler.registry.registerCollection("_prepend1", CollectionType.TAGGED)
        butler.registry.registerCollection("_prepend2", CollectionType.TAGGED)
        butler.registry.registerCollection("_prepend3", CollectionType.TAGGED)
        butler.registry.registerCollection("_prepend_base", CollectionType.CHAINED)

        # Empty chain.
        self.assertEqual(list(butler.registry.getCollectionChain("_prepend_base")), [])
        _prepend_collection(butler, "_prepend_base", ["_prepend1"])
        self.assertEqual(list(butler.registry.getCollectionChain("_prepend_base")), ["_prepend1"])

        # Non-empty chain.
        butler.registry.setCollectionChain("_prepend_base", ["_prepend1", "_prepend2"])
        _prepend_collection(butler, "_prepend_base", ["_prepend3"])
        self.assertEqual(list(butler.registry.getCollectionChain("_prepend_base")),
                         ["_prepend3", "_prepend1", "_prepend2"])

    def test_remove_from_chain(self):
        butler = self.interface.butler
        butler.registry.registerCollection("_remove1", CollectionType.TAGGED)
        butler.registry.registerCollection("_remove2", CollectionType.TAGGED)
        butler.registry.registerCollection("_remove33", CollectionType.TAGGED)
        butler.registry.registerCollection("_remove_base", CollectionType.CHAINED)

        # Empty chain.
        self.assertEqual(list(butler.registry.getCollectionChain("_remove_base")), [])
        _remove_from_chain(butler, "_remove_base", ["_remove1"])
        self.assertEqual(list(butler.registry.getCollectionChain("_remove_base")), [])

        # Non-empty chain.
        butler.registry.setCollectionChain("_remove_base", ["_remove1", "_remove2"])
        _remove_from_chain(butler, "_remove_base", ["_remove2", "_remove3"])
        self.assertEqual(list(butler.registry.getCollectionChain("_remove_base")), ["_remove1"])

    def test_filter_calibs_by_date_early(self):
        # _filter_calibs_by_date requires a collection, not merely an iterable
        all_calibs = list(self.central_butler.registry.queryDatasets("cpBias"))
        early_calibs = list(_filter_calibs_by_date(
            self.central_butler, "DECam/calib", all_calibs,
            astropy.time.Time("2015-02-26 00:00:00", scale="utc")
        ))
        self.assertEqual(len(early_calibs), 4)
        for calib in early_calibs:
            self.assertEqual(calib.run, "DECam/calib/20150218T000000Z")

    def test_filter_calibs_by_date_late(self):
        # _filter_calibs_by_date requires a collection, not merely an iterable
        all_calibs = list(self.central_butler.registry.queryDatasets("cpFlat"))
        late_calibs = list(_filter_calibs_by_date(
            self.central_butler, "DECam/calib", all_calibs,
            astropy.time.Time("2015-03-16 00:00:00", scale="utc")
        ))
        self.assertEqual(len(late_calibs), 4)
        for calib in late_calibs:
            self.assertEqual(calib.run, "DECam/calib/20150313T000000Z")

    def test_filter_calibs_by_date_never(self):
        # _filter_calibs_by_date requires a collection, not merely an iterable
        all_calibs = list(self.central_butler.registry.queryDatasets("cpBias"))
        with warnings.catch_warnings():
            # Avoid "dubious year" warnings from using a 2050 date
            warnings.simplefilter("ignore", category=astropy.utils.exceptions.ErfaWarning)
            future_calibs = list(_filter_calibs_by_date(
                self.central_butler, "DECam/calib", all_calibs,
                astropy.time.Time("2050-01-01 00:00:00", scale="utc")
            ))
        self.assertEqual(len(future_calibs), 0)

    def test_filter_calibs_by_date_unbounded(self):
        # _filter_calibs_by_date requires a collection, not merely an iterable
        all_calibs = set(self.central_butler.registry.queryDatasets(["camera", "crosstalk"]))
        valid_calibs = set(_filter_calibs_by_date(
            self.central_butler, "DECam/calib", all_calibs,
            astropy.time.Time("2015-03-15 00:00:00", scale="utc")
        ))
        self.assertEqual(valid_calibs, all_calibs)

    def test_filter_calibs_by_date_empty(self):
        valid_calibs = set(_filter_calibs_by_date(
            self.central_butler, "DECam/calib", [],
            astropy.time.Time("2015-03-15 00:00:00", scale="utc")
        ))
        self.assertEqual(len(valid_calibs), 0)


class MiddlewareInterfaceWriteableTest(unittest.TestCase):
    """Test the MiddlewareInterface class with faked data.

    This class creates a fresh test repository for writing to. This means test
    setup takes longer than for MiddlewareInterfaceTest, so it should be
    used sparingly.
    """
    def _create_copied_repo(self):
        """Create a fresh repository that's a copy of the test data.

        This method sets self.central_repo and arranges cleanup; cleanup would
        be awkward if this method returned a Butler instead.
        """
        # Copy test data to fresh Butler to allow write tests.
        data_dir = os.path.join(os.path.abspath(os.path.dirname(__file__)), "data")
        data_repo = os.path.join(data_dir, "central_repo")
        data_butler = Butler(data_repo, writeable=False)
        self.central_repo = tempfile.TemporaryDirectory()
        # TemporaryDirectory warns on leaks
        self.addCleanup(tempfile.TemporaryDirectory.cleanup, self.central_repo)

        # Butler.transfer_from can't easily copy collections, so use
        # export/import instead.
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml") as export_file:
            with data_butler.export(filename=export_file.name) as export:
                export.saveDatasets(data_butler.registry.queryDatasets(..., collections=...))
                for collection in data_butler.registry.queryCollections():
                    export.saveCollection(collection)
            dimension_config = data_butler.dimensions.dimensionConfig
            central_butler = Butler(Butler.makeRepo(self.central_repo.name, dimensionConfig=dimension_config),
                                    writeable=True,
                                    )
            central_butler.import_(directory=data_repo, filename=export_file.name, transfer="auto")

    def setUp(self):
        self._create_copied_repo()
        central_butler = Butler(self.central_repo.name,
                                instrument=instname,
                                skymap=skymap_name,
                                collections=[f"{instname}/defaults"],
                                writeable=True)
        data_dir = os.path.join(os.path.abspath(os.path.dirname(__file__)), "data")
        self.input_data = os.path.join(data_dir, "input_data")

        local_repo = make_local_repo(tempfile.gettempdir(), central_butler, instname)
        second_local_repo = make_local_repo(tempfile.gettempdir(), central_butler, instname)
        # TemporaryDirectory warns on leaks; addCleanup also keeps the TD from
        # getting garbage-collected.
        self.addCleanup(tempfile.TemporaryDirectory.cleanup, local_repo)
        self.addCleanup(tempfile.TemporaryDirectory.cleanup, second_local_repo)

        env_patcher = unittest.mock.patch.dict(os.environ,
                                               {"URL_APDB": f"sqlite:///{local_repo.name}/apdb.db",
                                                "K_REVISION": "prompt-proto-service-042",
                                                })
        env_patcher.start()
        self.addCleanup(env_patcher.stop)

        # coordinates from DECam data in ap_verify_ci_hits2015 for visit 411371
        ra = 155.4702849608958
        dec = -4.950050405424033
        # DECam has no rotator; instrument angle is 90 degrees in our system.
        rot = 90.
        self.next_visit = FannedOutVisit(instrument=instname,
                                         detector=56,
                                         groupId="1",
                                         nimages=1,
                                         filters=filter,
                                         coordinateSystem=FannedOutVisit.CoordSys.ICRS,
                                         position=[ra, dec],
                                         startTime=1424237500.0,
                                         rotationSystem=FannedOutVisit.RotSys.SKY,
                                         cameraAngle=rot,
                                         survey="SURVEY",
                                         salIndex=42,
                                         scriptSalIndex=42,
                                         dome=FannedOutVisit.Dome.OPEN,
                                         duration=35.0,
                                         totalCheckpoints=1,
                                         private_sndStamp=1424237298.716517500,
                                         )
        self.logger_name = "lsst.activator.middleware_interface"

        # Populate repository.
        self.interface = MiddlewareInterface(central_butler, self.input_data, self.next_visit,
                                             pipelines, skymap_name, local_repo.name,
                                             prefix="file://")
        self.interface.prep_butler()
        filename = "fakeRawImage.fits"
        filepath = os.path.join(self.input_data, filename)
        self.raw_data_id, file_data = fake_file_data(filepath,
                                                     self.interface.butler.dimensions,
                                                     self.interface.instrument,
                                                     self.next_visit)

        self.second_visit = dataclasses.replace(self.next_visit, groupId="2")
        self.second_data_id, second_file_data = fake_file_data(filepath,
                                                               self.interface.butler.dimensions,
                                                               self.interface.instrument,
                                                               self.second_visit)
        self.second_interface = MiddlewareInterface(central_butler, self.input_data, self.second_visit,
                                                    pipelines, skymap_name, second_local_repo.name,
                                                    prefix="file://")
        date = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=-12)))
        self.output_chain = f"{instname}/prompt/output-{date.year:04d}-{date.month:02d}-{date.day:02d}"
        self.output_run = f"{instname}/prompt/output-{date.year:04d}-{date.month:02d}-{date.day:02d}" \
                          "/ApPipe/prompt-proto-service-042"

        with unittest.mock.patch.object(self.interface.rawIngestTask, "extractMetadata") as mock:
            mock.return_value = file_data
            self.interface.ingest_image(filename)
        with unittest.mock.patch.object(self.second_interface.rawIngestTask, "extractMetadata") as mock:
            mock.return_value = second_file_data
            self.second_interface.ingest_image(filename)
        self.interface.define_visits.run([self.raw_data_id])
        self.second_interface.define_visits.run([self.second_data_id])

        self._simulate_run()

    def _simulate_run(self):
        """Create a mock pipeline execution that stores a calexp for self.raw_data_id.
        """
        exp = lsst.afw.image.ExposureF(20, 20)
        self.processed_data_id = {(k if k != "exposure" else "visit"): v
                                  for k, v in self.raw_data_id.required.items()}
        self.second_processed_data_id = {(k if k != "exposure" else "visit"): v
                                         for k, v in self.second_data_id.required.items()}
        # Dataset types defined for local Butler on pipeline run, but code
        # assumes output types already exist in central repo.
        butler_tests.addDatasetType(self.interface.central_butler, "calexp",
                                    {"instrument", "visit", "detector"},
                                    "ExposureF")
        butler_tests.addDatasetType(self.interface.butler, "calexp",
                                    {"instrument", "visit", "detector"},
                                    "ExposureF")
        butler_tests.addDatasetType(self.second_interface.butler, "calexp",
                                    {"instrument", "visit", "detector"},
                                    "ExposureF")
        self.interface.butler.put(exp, "calexp", self.processed_data_id, run=self.output_run)
        self.second_interface.butler.put(exp, "calexp", self.second_processed_data_id, run=self.output_run)

    def _count_datasets(self, butler, types, collections):
        return len(set(butler.registry.queryDatasets(types, collections=collections)))

    def _count_datasets_with_id(self, butler, types, collections, data_id):
        return len(set(butler.registry.queryDatasets(types, collections=collections, dataId=data_id)))

    def test_extra_collection(self):
        """Test that extra collections in the chain will not lead to MissingCollectionError
        even if they do not carry useful data.
        """
        central_butler = Butler(self.central_repo.name, writeable=True)
        central_butler.registry.registerCollection("emptyrun", CollectionType.RUN)
        _prepend_collection(central_butler, "refcats", ["emptyrun"])

        self.interface.prep_butler()

        self.assertEqual(
            self._count_datasets(self.interface.butler, "gaia_dr2_20200414", f"{instname}/defaults"),
            3)
        self.assertIn(
            "emptyrun",
            self.interface.butler.registry.queryCollections("refcats", flattenChains=True))

    def test_export_outputs(self):
        self.interface.export_outputs({self.raw_data_id["exposure"]})
        self.second_interface.export_outputs({self.second_data_id["exposure"]})

        central_butler = Butler(self.central_repo.name, writeable=False)
        self.assertEqual(self._count_datasets(central_butler, "calexp", self.output_run), 2)
        self.assertEqual(self._count_datasets(central_butler, "calexp", self.output_chain), 2)
        self.assertEqual(
            self._count_datasets_with_id(central_butler, "calexp", self.output_run, self.processed_data_id),
            1)
        self.assertEqual(
            self._count_datasets_with_id(central_butler, "calexp", self.output_run,
                                         self.second_processed_data_id),
            1)
        # Did not export calibs or other inputs.
        self.assertEqual(
            self._count_datasets(central_butler, ["cpBias", "gaia_dr2_20200414", "skyMap", "*Coadd"],
                                 self.output_run),
            0)
        # Nothing placed in "input" collections.
        self.assertEqual(
            self._count_datasets(central_butler, ["raw", "calexp"], f"{instname}/defaults"),
            0)
