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

__all__ = ["get_central_butler", "MiddlewareInterface"]

import collections.abc
import functools
import itertools
import logging
import os
import os.path
import re
import tempfile
import typing
import yaml

import astropy
import botocore.exceptions

import lsst.utils.timer
from lsst.resources import ResourcePath
import lsst.sphgeom
import lsst.afw.cameraGeom
from lsst.pipe.base.mp_graph_executor import MPGraphExecutor
from lsst.pipe.base.separable_pipeline_executor import SeparablePipelineExecutor
from lsst.pipe.base.single_quantum_executor import SingleQuantumExecutor
from lsst.daf.butler import Butler, CollectionType, DatasetType, DatasetRef, Timespan, \
    DimensionRecord, MissingDatasetTypeError, EmptyQueryResultError
import lsst.dax.apdb
import lsst.geom
import lsst.obs.base
import lsst.pipe.base
from lsst.pipe.base.quantum_graph_builder import QuantumGraphBuilderError
import lsst.analysis.tools
from lsst.analysis.tools.interfaces.datastore import SasquatchDispatcher, SasquatchDispatchFailure, \
    SasquatchDispatchPartialFailure  # Can't use fully-qualified names

from shared.config import PipelinesConfig
import shared.connect_utils as connect
import shared.run_utils as runs
from shared.visit import FannedOutVisit
from .exception import GracefulShutdownInterrupt, TimeoutInterrupt, NonRetriableError, RetriableError, \
    InvalidPipelineError, NoGoodPipelinesError, PipelinePreExecutionError, PipelineExecutionError, \
    ProvenanceDimensionsError
from .local_repo import LocalRepo
from .mw_retries import repo_retry, SQL_EXCEPTIONS, DATASTORE_EXCEPTIONS
from .timer import enforce_schema, time_this_to_bundle

_log = logging.getLogger("lsst." + __name__)
_log.setLevel(logging.DEBUG)
# See https://developer.lsst.io/stack/logging.html#logger-trace-verbosity
_log_trace = logging.getLogger("TRACE1.lsst." + __name__)
_log_trace.setLevel(logging.CRITICAL)  # Turn off by default.
_log_trace3 = logging.getLogger("TRACE3.lsst." + __name__)
_log_trace3.setLevel(logging.CRITICAL)  # Turn off by default.

# Whether or not to export to the central repo.
do_export = bool(int(os.environ.get("DEBUG_EXPORT_OUTPUTS", '1')))
# The number of arcseconds to pad the region in preloading spatial datasets.
padding = float(os.environ.get("PRELOAD_PADDING", 30))


@connect.retry(2, SQL_EXCEPTIONS, wait=repo_retry)
def get_central_butler(central_repo: str, writeable: bool):
    """Provide a Butler that can access the given repository.

    This function is guaranteed to return a new object on every call, and the
    caller is responsible for managing and cleaning it up.

    Parameters
    ----------
    central_repo : `str`
        The path or URI to the central repository.
    writeable : `bool`
        Whether or not it's safe to attempt writes to this Butler.

    Returns
    -------
    butler : `lsst.daf.butler.Butler`
        A new Butler for ``central_repo``.
    """
    return Butler(central_repo,
                  writeable=writeable,
                  inferDefaults=False,
                  )


def _get_sasquatch_dispatcher():
    """Get a SasquatchDispatcher object ready for use by Prompt Processing.

    Returns
    -------
    dispatcher : `lsst.analysis.tools.interfaces.datastore.SasquatchDispatcher` \
            or `None`
        The object to handle all Sasquatch uploads from this module. If `None`,
        the service is not configured to use Sasquatch.
    """
    url = os.environ.get("SASQUATCH_URL", "")
    if not url:
        return None
    token = os.environ.get("SASQUATCH_TOKEN", "")
    namespace = os.environ.get("DAF_BUTLER_SASQUATCH_NAMESPACE", "lsst.prompt")
    return SasquatchDispatcher(url=url, token=token, namespace=namespace)


GroupedDimensionRecords: typing.TypeAlias = dict[str, list[DimensionRecord]]
"""Dictionary from dimension name to list of dimension records for that
dimension.
"""


class ButlerWriter(typing.Protocol):
    """Interface defining functions for writing output datasets back to the central
    Butler repository.
    """

    def transfer_outputs(
        self, local_butler: Butler, dimension_records: GroupedDimensionRecords, datasets: list[DatasetRef]
    ) -> list[DatasetRef]:
        """Transfer outputs back to the central repository.

        Parameters
        ----------
        local_butler : `lsst.daf.butler.Butler`
            Local Butler repository from which output datasets will be
            transferred.
        dimension_records : `dict` [`str` , `list` [`lsst.daf.butler.DimensionRecord`]]
            Dimension records to write to the central Butler repository.
        datasets : `list` [`lsst.daf.butler.DatasetRef`]
            Datasets to transfer to the central Butler repository.

        Returns
        -------
        transferred : `list` [`lsst.daf.butler.DatasetRef`]
            List of datasets actually transferred.
        """


class DirectButlerWriter(ButlerWriter):
    def __init__(self, central_butler: Butler) -> None:
        """Writes Butler outputs back to the central repository by connecting
        directly to the Butler database.

        Parameters
        ----------
        central_butler : `lsst.daf.butler.Butler`
            Butler repo to which pipeline outputs should be written.
        """
        self._central_butler = central_butler

    def transfer_outputs(
        self, local_butler: Butler, dimension_records: GroupedDimensionRecords, datasets: list[DatasetRef]
    ) -> list[DatasetRef]:
        dimensions = local_butler.dimensions.sorted(dimension_records.keys())
        for dimension in dimensions:
            records = dimension_records[dimension.name]
            # If records don't match, this is not an error, and central takes precedence.
            self._central_butler.registry.insertDimensionData(dimension, *records, skip_existing=True)

        return self._central_butler.transfer_from(
            local_butler, datasets, transfer="copy", transfer_dimensions=False)


class MiddlewareInterface:
    """Interface layer between the Butler middleware and the prompt processing
    data handling system, to handle processing individual images.

    An instance of this class will accept an incoming group of single-detector
    snaps to process, using an instance-local butler repo. The instance can
    pre-load the necessary calibrations to process an incoming detector-visit,
    ingest the data when it is available, and run the difference imaging
    pipeline, all in that local butler.

    Each instance must be used for processing only one group-detector
    combination. The object may contain state that is unique to a particular
    processing run.

    ``MiddlewareInterface`` objects are not thread- or process-safe. It is up
    to the client to avoid conflicts from multiple objects trying to access the
    same local repo.

    Parameters
    ----------
    read_butler : `lsst.daf.butler.Butler`
        Butler repo containing the calibration and other data needed for
        processing images as they are received.
    butler_writer : `activator.middleware_interface.ButlerWriter`
        Object that will be used to write the pipeline outputs back to the
        central Butler repository.
    image_bucket : `str`
        Storage bucket where images will be written to as they arrive.
        See also ``prefix``.
    visit : `shared.visit.FannedOutVisit`
        The visit-detector combination to be processed by this object.
    pre_pipelines : `shared.config.PipelinesConfig`
        Information about which pipelines to run before a visit arrives.
    main_pipelines : `shared.config.PipelinesConfig`
        Information about which pipelines to run on ``visit``'s raws.
    skymap : `str`
        Name of the skymap in the central repo for querying templates.
    local_repo : `activator.local_repo.LocalRepo`
        An object representing this worker's unique local repo.
    prefix : `str`, optional
        URI scheme followed by ``://``; prepended to ``image_bucket`` when
        constructing URIs to retrieve incoming files. The default is
        appropriate for use in the USDF environment; typically only
        change this when running local tests.
    """
    DATASET_IDENTIFIER = "Live"
    """The dataset ID used for Sasquatch uploads.
    """

    @property
    def _collection_template(self):
        """The collection used for templates.

        This collection depends on initialization parameters, and must
        not be called from this object's constructor.
        """
        return self.instrument.makeCollectionName("templates")

    # Class invariants:
    # self.image_host is a valid URI with non-empty path and no query or fragment.
    # self._download_store is None if and only if self.image_host is a local URI.
    # self.visit, self.instrument, self.camera, self._deployment
    #   self._day_obs do not change after __init__.

    def __init__(self, read_butler: Butler, butler_writer: ButlerWriter, image_bucket: str,
                 visit: FannedOutVisit,
                 pre_pipelines: PipelinesConfig, main_pipelines: PipelinesConfig,
                 skymap: str, local_repo: LocalRepo,
                 prefix: str = "s3://"):
        self.visit = visit

        self._apdb_config = os.environ["CONFIG_APDB"]
        # Deployment/version ID -- potentially expensive to generate.
        self._deployment = runs.get_deployment(self._apdb_config)
        self.read_central_butler = read_butler
        self._butler_writer = butler_writer
        self.image_host = prefix + image_bucket
        # TODO: _download_store turns MWI into a tagged class; clean this up later
        if not self.image_host.startswith("file"):
            self._download_store = tempfile.TemporaryDirectory(prefix="holding-")
        else:
            self._download_store = None
        # TODO: how much overhead do we pick up from going through the registry?
        self.instrument = lsst.obs.base.Instrument.from_string(
            visit.instrument, self.read_central_butler.registry)
        self.pre_pipelines = pre_pipelines
        self.main_pipelines = main_pipelines

        now = astropy.time.Time.now()
        self._day_obs = runs.get_day_obs(now)

        self.repo = local_repo
        self._init_governor_datasets(now, skymap)
        self._prep_collections()
        self._define_dimensions()
        self._init_ingester()
        self._init_visit_definer()
        self._init_provenance_dataset_type()

        # How much to pad the spatial region we will copy over.
        self.padding = padding*lsst.geom.arcseconds

    def _init_ingester(self):
        """Prepare the raw file ingester to receive images into this butler.
        """
        config = lsst.obs.base.RawIngestConfig()
        self.instrument.applyConfigOverrides(lsst.obs.base.RawIngestTask._DefaultName, config)
        config.transfer = "copy"  # Copy files into the local butler.
        config.failFast = True  # We want failed ingests to fail immediately.
        self.rawIngestTask = lsst.obs.base.RawIngestTask(config=config,
                                                         butler=self.repo.butler)

    def _init_visit_definer(self):
        """Prepare the visit definer to define visits for this butler.
        """
        define_visits_config = lsst.obs.base.DefineVisitsConfig()
        self.instrument.applyConfigOverrides(lsst.obs.base.DefineVisitsTask._DefaultName,
                                             define_visits_config)
        define_visits_config.groupExposures = "one-to-one"
        self.define_visits = lsst.obs.base.DefineVisitsTask(config=define_visits_config,
                                                            butler=self.repo.butler)

    @connect.retry(2, DATASTORE_EXCEPTIONS, wait=repo_retry)
    def _init_governor_datasets(self, timestamp, skymap):
        """Load and store the camera for later use, and record the skymap name.

        Parameters
        ----------
        timestamp : `astropy.time.Time`
            The time at which the camera must be valid.
        skymap : `str`
            The name of the skymap.
        """
        # Camera is time-dependent, in principle, and may be available only
        # through a calibration collection.
        camera_ref = self.read_central_butler.find_dataset(
            "camera",
            instrument=self.instrument.getName(),
            collections=self.instrument.makeCalibrationCollectionName(),
            timespan=Timespan.fromInstant(timestamp)
        )
        self.camera = self.read_central_butler.get(camera_ref)

        self.skymap_name = skymap

    def _init_provenance_dataset_type(self):
        """Register the dataset types used to store provenance information.
        """
        self._provenance_dataset_type = DatasetType(
            "prompt_provenance",
            self.repo.butler.dimensions.conform(["group", "detector"]),
            "ProvenanceQuantumGraph",
        )
        self.repo.butler.registry.registerDatasetType(self._provenance_dataset_type)

    def _define_dimensions(self):
        """Define any dimensions that must be computed from this object's visit.
        """
        self.repo.butler.registry.syncDimensionData("group",
                                                    {"name": self.visit.groupId,
                                                     "instrument": self.instrument.getName(),
                                                     })

    def _pad_region(self,
                    initial_region: lsst.sphgeom.Region,
                    wcs: lsst.afw.geom.SkyWcs,
                    ) -> lsst.sphgeom.Region:
        """Pad the expected footprint to allow for slew errors.

        This method emits a warning if the preload padding is too small.

        Parameters
        ----------
        initial_region : `lsst.sphgeom.Region`
            The unpadded region to expand.
        wcs : `lsst.afw.geom.SkyWcs`
            A WCS for the current image. Only needs to be good enough to get
            the plate scale.

        Returns
        -------
        region : `lsst.sphgeom.Region`
            The padded region.

        Raises
        ------
        TypeError
            Raised if padding is not supported for ``initial_region``.
        """
        # Compare the preload region padding versus the visit region padding
        # in the middleware visit definition.
        visit_definition_padding = (
            self.define_visits.config.computeVisitRegions["single-raw-wcs"].padding
            * wcs.getPixelScale().asArcseconds()
        )
        preload_region_padding = self.padding.asArcseconds()
        if preload_region_padding < visit_definition_padding:
            _log.warning("Preload padding (%.1f arcsec) is smaller than "
                         "visit definition's region padding (%.1f arcsec).",
                         preload_region_padding, visit_definition_padding)

        if isinstance(initial_region, lsst.sphgeom.ConvexPolygon):
            center = lsst.geom.SpherePoint(initial_region.getCentroid())
            corners = [lsst.geom.SpherePoint(c) for c in initial_region.getVertices()]
            padded = [c.offset(center.bearingTo(c), self.padding) for c in corners]
            return lsst.sphgeom.ConvexPolygon.convexHull([c.getVector() for c in padded])
        elif isinstance(initial_region, lsst.sphgeom.Circle):
            return lsst.sphgeom.Circle(initial_region.getCenter(),
                                       initial_region.getOpeningAngle() + self.padding)
        else:
            raise TypeError(f"Cannot pad region {initial_region!r}.")

    def prep_butler(self) -> None:
        """Prepare a temporary butler repo for processing the incoming data.

        After this method returns, the internal butler is guaranteed to contain
        all data and all dimensions needed to run the appropriate pipeline on
        this object's visit, except for ``raw`` and the ``exposure`` and
        ``visit`` dimensions, respectively. It may contain other data that would
        not be loaded when processing the visit.
        """
        action_id = "prepButlerTimeMetric"  # For consistency with analysis_tools outputs
        bundle = lsst.analysis.tools.interfaces.MetricMeasurementBundle(
            dataset_identifier=self.DATASET_IDENTIFIER,
        )
        with time_this_to_bundle(bundle, action_id, "prep_butlerTotalTime"):
            with lsst.utils.timer.time_this(_log, msg="prep_butler", level=logging.DEBUG):
                _log.info(f"Preparing Butler for visit {self.visit!r}")

                wcs = self.visit.predict_wcs(self.camera)
                if wcs:
                    region = self._pad_region(self.visit.get_detector_icrs_region(self.camera), wcs)
                    _log.debug(
                        f"Preload region {region} including padding {self.padding.asArcseconds()} arcsec.")
                    self._write_region_time(region)  # Must be done before preprocessing pipeline
                else:
                    _log.warning("Could not get sky position from visit %s. "
                                 "Spatial datasets won't be loaded.", self.visit)
                    region = None

                with time_this_to_bundle(bundle, action_id, "prep_butlerSearchTime"):
                    all_datasets, calib_datasets = self._find_pipeline_inputs(region)

                with time_this_to_bundle(bundle, action_id, "prep_butlerTransferTime"):
                    output_runs = {runs.get_output_run(self.instrument, self._deployment, f, self._day_obs)
                                   for f in self.get_combined_pipeline_files()}
                    transferred = self.repo.load_from(self.read_central_butler, all_datasets,
                                                      no_cache_runs=output_runs)
                    missing = _check_transfer_completion(all_datasets, transferred, "Downloaded")

                    with lsst.utils.timer.time_this(_log,
                                                    msg="prep_butler (transfer collections)",
                                                    level=logging.DEBUG):
                        self.repo.sync_collections(self.read_central_butler, self._collection_template)
                        self.repo.sync_collections(self.read_central_butler,
                                                   self.instrument.makeUmbrellaCollectionName())

                    # Must be called after collections have been exported
                    # TODO: find a way to encapsulate collection sync + association sync in LocalRepo
                    # while still letting MWI specify the collection names.
                    with lsst.utils.timer.time_this(_log,
                                                    msg="prep_butler (transfer associations)",
                                                    level=logging.DEBUG):
                        self.repo.export_calib_associations(
                            self.read_central_butler,
                            self.instrument.makeCalibrationCollectionName(),
                            calib_datasets - missing)

                    # Temporary workarounds until we have a prompt-processing default top-level collection
                    # in shared repos, and raw collection in dev repo, and then we can organize collections
                    # without worrying about DRP use cases.
                    self.repo.butler.collections.prepend_chain(
                        self.instrument.makeUmbrellaCollectionName(),
                        [self._collection_template,
                         self.instrument.makeDefaultRawIngestRunName(),
                         ])

                with time_this_to_bundle(bundle, action_id, "prep_butlerPreprocessTime"):
                    try:
                        self._run_preprocessing()
                    except NoGoodPipelinesError:
                        _log.exception("Preprocessing pipelines not runnable, trying main pipelines anyway.")
                    except (PipelinePreExecutionError, PipelineExecutionError):
                        _log.exception("Preprocessing pipeline failed, trying main pipelines anyway.")

        # IMPORTANT: do not remove or rename entries in this list. New entries can be added as needed.
        enforce_schema(bundle, {action_id: ["prep_butlerTotalTime",
                                            "prep_butlerSearchTime",
                                            "prep_butlerTransferTime",
                                            "prep_butlerPreprocessTime",
                                            ]})
        self.repo.butler.registry.registerDatasetType(DatasetType(
            "promptPreload_metrics",
            dimensions={"instrument", "group", "detector"},
            storageClass="MetricMeasurementBundle",
            universe=self.repo.butler.dimensions,
        ))
        self.repo.butler.put(bundle,
                             "promptPreload_metrics",
                             run=runs.get_preload_run(self.instrument, self._deployment, self._day_obs),
                             instrument=self.instrument.getName(),
                             detector=self.visit.detector,
                             group=self.visit.groupId)

    @connect.retry(2, SQL_EXCEPTIONS, wait=repo_retry)
    def _find_pipeline_inputs(self, region):
        """Identify the datasets needed for pipeline execution.

        The returned datasets are a superset of those needed by any pipeline.

        Parameters
        ----------
        region : `lsst.sphgeom.Region` or None
            The region to find data to preload.

        Returns
        -------
        datasets : set [`~lsst.daf.butler.DatasetRef`]
            The datasets needed by at least one pipeline.
        calibs : set [`~lsst.daf.butler.DatasetRef`]
            The subset of ``datasets`` representing calibs.
        """
        net_types = set().union(*self._get_preloadable_types().values())
        # Filter outputs made by preprocessing and consumed by main.
        for pipeline_file in self.get_pre_pipeline_files():
            net_types.difference_update(self._get_pipeline_output_types(pipeline_file))

        with lsst.utils.timer.time_this(_log, msg="prep_butler (find init-outputs)", level=logging.DEBUG):
            all_datasets = set(self._find_init_outputs())
        calib_datasets = set()

        present_types = net_types.copy()
        with lsst.utils.timer.time_this(_log, msg="prep_butler (find inputs)", level=logging.DEBUG):
            for type_name in net_types:
                dstype = self.read_central_butler.get_dataset_type(type_name)
                try:
                    if dstype.isCalibration():
                        new_calibs = self._find_calibs(dstype, self.visit.detector, self.visit.filters)
                        calib_datasets.update(new_calibs)
                        all_datasets.update(new_calibs)
                    elif "htm7" in dstype.dimensions or "skypix" in dstype.dimensions:
                        if region is not None:
                            all_datasets.update(self._find_refcats(dstype, region))
                    elif "tract" in dstype.dimensions:
                        if region is not None:
                            all_datasets.update(self._find_templates(dstype, region, self.visit.filters))
                    else:
                        all_datasets.update(self._find_generic_datasets(
                            dstype, self.visit.detector, self.visit.filters))
                except EmptyQueryResultError:
                    _log.warning("Found no source datasets of type %s.", type_name)
                    present_types.remove(type_name)

        if self._is_main_pipeline_runnable(present_types):
            return (all_datasets, calib_datasets)
        else:
            raise NoGoodPipelinesError("Cannot run any main pipeline.")

    def _get_preloadable_types(self):
        """Identify all types to attempt to preload.

        Returns
        -------
        types : mapping [`str`, set [`str`]]
            A mapping from each pipeline's path to the types to preload for
            that pipeline.
        """
        input_types = {}
        for pipeline_file in self.get_combined_pipeline_files():
            inputs = self._get_pipeline_input_types(pipeline_file)
            # Not preloaded
            inputs.discard("regionTimeInfo")
            inputs.discard("raw")
            input_types[pipeline_file] = inputs
        return input_types

    def _is_main_pipeline_runnable(self, present_types):
        """Determine if at least one pipeline can be run with the available data.

        This method emits diagnostic logs as a side effect.

        Parameters
        ----------
        present_types : set [`str`]
            The types that are accounted for, either already present in the
            local repo or marked for download.

        Returns
        -------
        runnable : `bool`
            `True` if and only if at least one pipeline has all inputs.
        """
        pre_outputs = set()
        for pipeline_file in self.get_pre_pipeline_files():
            input_types = self._get_pipeline_input_types(pipeline_file, include_optional=False)
            input_types.discard("regionTimeInfo")
            if input_types <= present_types:
                _log.debug("Found inputs for %s.", pipeline_file)
                pre_outputs.update(self._get_pipeline_output_types(pipeline_file))
            else:
                _log.debug("Missing inputs for %s: %s.", pipeline_file, input_types - present_types)
        main_inputs = present_types | pre_outputs
        for pipeline_file in self.get_main_pipeline_files():
            input_types = self._get_pipeline_input_types(pipeline_file, include_optional=False)
            input_types.discard("regionTimeInfo")
            input_types.discard("raw")
            if input_types <= main_inputs:
                _log.debug("Found inputs for %s.", pipeline_file)
                return True
            else:
                _log.debug("Missing inputs for %s: %s.", pipeline_file, input_types - main_inputs)
        return False

    def _get_pipeline_input_types(self, pipeline_file, include_optional=True):
        """Identify the dataset types needed as inputs for a pipeline.

        Parameters
        ----------
        pipeline_file : `str`
            The pipeline whose inputs are desired.
        include_optional : `bool`, optional
            Whether to report optional inputs (the default) or only required
            ones.

        Returns
        -------
        input_types : set [`str`]
            The types of preexisting datasets needed to run the pipeline.
        """
        try:
            pipeline = self._prep_pipeline_graph(pipeline_file)
        except FileNotFoundError as e:
            raise RuntimeError(f"Could not find pipeline {pipeline_file}.") from e

        if include_optional:
            task_inputs = {edge.parent_dataset_type_name
                           for task in pipeline.tasks.values() for edge in task.iter_all_inputs()}
        else:
            task_inputs = {edge.parent_dataset_type_name
                           for task in pipeline.tasks.values() for edge in task.iter_all_inputs()
                           if not task.is_optional(edge.key[2])}
        # Ignore inputs produced internally.
        task_outputs = {edge.parent_dataset_type_name
                        for task in pipeline.tasks.values() for edge in task.iter_all_outputs()}
        return task_inputs - task_outputs

    def _get_pipeline_output_types(self, pipeline_file):
        """Identify the dataset types produced as outputs by a pipeline.

        Parameters
        ----------
        pipeline_file : `str`
            The pipeline whose inputs are desired.

        Returns
        -------
        input_types : set [`str`]
            The types of preexisting datasets needed to run the pipeline.
        """
        try:
            pipeline = self._prep_pipeline_graph(pipeline_file)
        except FileNotFoundError as e:
            raise RuntimeError(f"Could not find pipeline {pipeline_file}.") from e

        return {edge.parent_dataset_type_name
                for task in pipeline.tasks.values() for edge in task.iter_all_outputs()}

    def _find_refcats(self, dataset_type, region):
        """Identify the refcats needed for pipeline runs.

        Parameters
        ----------
        dataset_type : `lsst.daf.butler.DatasetType`
            The type of refcat to search for.
        region : `lsst.sphgeom.Region`
            The region to find refcat shards in.

        Returns
        -------
        refcats : iterable [`lsst.daf.butler.DatasetRef`]
            The refcats needed to run pipelines on ``region``.
        """
        src_datasets = set(self.read_central_butler.query_datasets(
            dataset_type,
            collections=self.instrument.makeRefCatCollectionName(),
            where="htm7.region OVERLAPS search_region",
            bind={"search_region": region},
            find_first=True,
            explain=True,
            with_dimension_records=True,
        ))
        # Trace3 because, in many contexts, src_datasets is too large to print.
        _log_trace3.debug("%s: %s", dataset_type.name, src_datasets)
        _log.debug("Found %d refcat datasets from catalog '%s'.", len(src_datasets), dataset_type.name)
        return src_datasets

    def _find_templates(self, dataset_type, region, physical_filter):
        """Identify the templates needed for pipeline runs.

        Parameters
        ----------
        dataset_type : `lsst.daf.butler.DatasetType`
            The type of template to search for.
        region : `lsst.sphgeom.Region`
            The region to load the templates tract/patches for.
        physical_filter : `str`
            Physical filter for which to export templates. May be empty to
            indicate no specific filter.

        Returns
        -------
        templates : iterable [`lsst.daf.butler.DatasetRef`]
            The templates needed to run pipelines on ``region``.
        """
        if not physical_filter:
            _log.warning("Preloading templates is not supported for visits without a specific filter.")
            return set()

        data_id = {"instrument": self.instrument.getName(),
                   "skymap": self.skymap_name,
                   "physical_filter": physical_filter,
                   }
        src_datasets = set(self.read_central_butler.query_datasets(
            dataset_type,
            collections=self._collection_template,
            data_id=data_id,
            where="patch.region OVERLAPS search_region",
            bind={"search_region": region},
            find_first=True,
            explain=True,
            with_dimension_records=True,
        ))
        # Trace3 because, in many contexts, src_datasets is too large to print.
        _log_trace3.debug("%s: %s", dataset_type.name, src_datasets)
        _log.debug("Found %d template datasets of type %s.", len(src_datasets), dataset_type.name)
        return src_datasets

    def _find_calibs(self, dataset_type, detector_id, physical_filter):
        """Identify the calibs needed for pipeline runs.

        Parameters
        ----------
        dataset_type : `lsst.daf.butler.DatasetType`
            The type of calib to search for.
        detector_id : `int`
            Identifier of the detector to load calibs for.
        physical_filter : `str`
            Physical filter name of the upcoming visit. May be empty to indicate
            no specific filter.

        Returns
        -------
        calibs : iterable [`lsst.daf.butler.DatasetRef`]
            The calibs needed for running pipelines on the current visit.
        """
        # TAI observation start time should be used for calib validity range.
        calib_date = astropy.time.Time(self.visit.private_sndStamp, format="unix_tai")
        data_id = {"instrument": self.instrument.getName(), "detector": detector_id}
        if physical_filter:
            data_id["physical_filter"] = physical_filter
        elif "physical_filter" in dataset_type.dimensions or "band" in dataset_type.dimensions:
            _log.warning("Preloading filter-dependent calibs is not supported for visits "
                         "without a specific filter.")
            return set()

        with self.read_central_butler.query() as query:
            expr = query.expression_factory
            query = query.where(data_id)
            src_datasets = set(
                query.datasets(dataset_type,
                               self.instrument.makeCalibrationCollectionName(),
                               find_first=True)
                # where needs to come after datasets to pick up the type
                .where(expr[dataset_type.name].timespan.overlaps(calib_date))
                .with_dimension_records()
            )
            if not src_datasets:
                raise EmptyQueryResultError(list(query.explain_no_results()))
        # Trace3 because, in many contexts, datasets is too large to print.
        _log_trace3.debug("%s: %s", dataset_type.name, src_datasets)
        _log.debug("Found %d calib datasets of type '%s'.", len(src_datasets), dataset_type.name)
        return src_datasets

    def _find_generic_datasets(self, dataset_type, detector_id, physical_filter):
        """Identify non-calib, non-spatial datasets needed for pipeline runs.

        Parameters
        ----------
        dataset_type : `lsst.daf.butler.DatasetType`
            The type of dataset to search for.
        detector_id : `int`
            Identifier of the detector to load calibs for.
        physical_filter : `str`
            Physical filter name of the upcoming visit. May be empty to indicate
            no specific filter.

        Returns
        -------
        datasets : iterable [`lsst.daf.butler.DatasetRef`]
            The datasets needed for running pipelines on the current visit.
        """
        data_id = {"instrument": self.instrument.getName(),
                   "skymap": self.skymap_name,
                   "detector": detector_id,
                   }
        if physical_filter:
            data_id["physical_filter"] = physical_filter

        src_datasets = set(self.read_central_butler.query_datasets(
            dataset_type,
            collections=self.instrument.makeUmbrellaCollectionName(),
            data_id=data_id,
            find_first=True,
            explain=True,
            with_dimension_records=True,
        ))
        # Trace3 because, in many contexts, src_datasets is too large to print.
        _log_trace3.debug("%s: %s", dataset_type.name, src_datasets)
        _log.debug("Found %d datasets of type %s.", len(src_datasets), dataset_type.name)
        return src_datasets

    def _get_init_output_types(self, pipeline_file):
        """Identify the specific init-output types to query.

        Parameters
        ----------
        pipeline_file : `str`
            The pipeline of interest.

        Returns
        -------
        init_types : collection [`str`]
            The init-output types of interest to Prompt Processing.
        """
        try:
            pipeline = self._prep_pipeline_graph(pipeline_file)
        except FileNotFoundError as e:
            raise RuntimeError(f"Could not find pipeline {pipeline_file}.") from e

        return {edge.parent_dataset_type_name
                for task in pipeline.tasks.values() for edge in task.init.iter_all_outputs()}

    def _find_init_outputs(self):
        """Identify the init-output datasets to export from the central butler.

        Returns
        -------
        init_outputs : iterable [`lsst.daf.butler.DatasetRef`]
            The datasets to be exported.
        """
        datasets = set()
        for pipeline_file in self.get_combined_pipeline_files():
            run = runs.get_output_run(self.instrument, self._deployment, pipeline_file, self._day_obs)
            for dataset_type in self._get_init_output_types(pipeline_file):
                new_datasets = set(self.read_central_butler.query_datasets(
                    dataset_type,
                    collections=run,
                    explain=True,
                    with_dimension_records=True,
                ))
                datasets.update(new_datasets)
        # Trace3 because, in many contexts, datasets is too large to print.
        _log_trace3.debug("Init datasets: %s", datasets)

        for run, n_datasets in self._count_by_key(datasets, lambda ref: ref.run):
            _log.debug("Found %d init-output datasets from %s.", n_datasets, run)
        return datasets

    @staticmethod
    def _count_by_key(refs, keyfunc):
        """Count the number of dataset references of each type.

        Parameters
        ----------
        refs : iterable [`lsst.daf.butler.DatasetRef`]
            The references to classify.
        keyfunc : callable [tuple[`lsst.daf.butler.DatasetRef`], `str`]
            A callable that extracts the key to group and count by.

        Yields
        ------
        key : `str`
            A unique value returned by ``keyfunc`` from ``refs``.
        count : `int`
            The number of elements having ``key`` in ``refs``.
        """
        ordered = sorted(refs, key=keyfunc)
        for k, g in itertools.groupby(ordered, key=keyfunc):
            yield k, len(list(g))

    def _write_region_time(self, region):
        """Store the preload sky region and timespan for this
        object's visit.

        Parameters
        ----------
        region : `lsst.sphgeom.Region`
            Region for preload.
        """
        # Assume a padded interval that's centered on the most probable time
        # TODO: replace with self.visit.startTime after DM-38635
        start = astropy.time.Time(self.visit.private_sndStamp, format="unix_tai")
        end = start + 3.0 * self.visit.duration * astropy.units.second
        timespan = Timespan(start, end)

        self.repo.butler.registry.registerDatasetType(DatasetType(
            "regionTimeInfo",
            dimensions={"instrument", "group", "detector"},
            storageClass="RegionTimeInfo",
            universe=self.repo.butler.dimensions,
        ))
        self.repo.butler.put(lsst.pipe.base.utils.RegionTimeInfo(region=region, timespan=timespan),
                             "regionTimeInfo",
                             run=runs.get_preload_run(self.instrument, self._deployment, self._day_obs),
                             instrument=self.instrument.getName(),
                             detector=self.visit.detector,
                             group=self.visit.groupId)

    def _prep_collections(self):
        """Pre-register output collections in advance of running the pipeline.

        ``self._init_governor_datasets`` must have already been run.
        """
        self.repo.butler.collections.register(
            runs.get_preload_run(self.instrument, self._deployment, self._day_obs),
            CollectionType.RUN)
        for pipeline_file in self.get_combined_pipeline_files():
            self.repo.butler.collections.register(
                runs.get_output_run(self.instrument, self._deployment, pipeline_file, self._day_obs),
                CollectionType.RUN)

    def get_combined_pipeline_files(self) -> collections.abc.Collection[str]:
        """Identify the pipelines to be run at any point, based on the
        configured instrument and visit.

        Returns
        -------
        pipelines : collection [`str`]
            The paths to a configured pipeline file. The order is undefined.
        """
        # A pipeline appearing in both configs is unlikely, but not impossible.
        all = set(self.get_pre_pipeline_files())
        all.update(self.get_main_pipeline_files())
        return all

    def get_pre_pipeline_files(self) -> collections.abc.Sequence[str]:
        """Identify the pipelines to be run during preprocessing, based on the
        configured instrument and visit.

        Returns
        -------
        pipelines : sequence [`str`]
            A sequence of paths to a configured pipeline file, in order from
            most preferred to least preferred.
        """
        return self.pre_pipelines.get_pipeline_files(self.visit, self.camera)

    def get_main_pipeline_files(self) -> collections.abc.Sequence[str]:
        """Identify the pipelines to be run on the raws, based on the
        configured instrument and visit.

        Returns
        -------
        pipelines : sequence [`str`]
            A sequence of paths to a configured pipeline file, in order from
            most preferred to least preferred.
        """
        return self.main_pipelines.get_pipeline_files(self.visit, self.camera)

    @functools.cache
    def _prep_pipeline(self, pipeline_file) -> lsst.pipe.base.Pipeline:
        """Setup the pipeline to be run, based on the configured instrument and
        details of the incoming visit.

        Parameters
        ----------
        pipeline_file : `str`
            The pipeline file to run.

        Returns
        -------
        pipeline : `lsst.pipe.base.Pipeline`
            The fully configured pipeline.
        """
        pipeline = lsst.pipe.base.Pipeline.fromFile(pipeline_file)

        # Config overrides are not validated until graph generation.
        pipeline.addConfigOverride("parameters", "apdb_config", self._apdb_config)

        return pipeline

    @functools.cache
    def _prep_pipeline_graph(self, pipeline_file) -> lsst.pipe.base.PipelineGraph:
        """Setup the pipeline to be run, based on the configured instrument and
        details of the incoming visit.

        Parameters
        ----------
        pipeline_file : `str`
            The pipeline file to run.

        Returns
        -------
        pipeline : `lsst.pipe.base.PipelineGraph`
            The fully configured pipeline, in graph form.
        """
        return self._prep_pipeline(pipeline_file).to_graph()

    @connect.retry(2, botocore.exceptions.ClientError, wait=5)
    def _download(self, remote):
        """Download an image located on a remote store.

        Parameters
        ----------
        remote : `lsst.resources.ResourcePath`
            The location from which to download the file. Must not be a
            file:// URI.

        Returns
        -------
        local : `lsst.resources.ResourcePath`
            The location to which the file has been downloaded.
        """
        local = ResourcePath(os.path.join(self._download_store.name, remote.basename()))
        local.transfer_from(remote, "copy")
        return local

    def ingest_image(self, oid: str) -> None:
        """Ingest an image into the temporary butler.

        The temporary butler must not already contain a ``raw`` dataset
        corresponding to ``oid``. After this method returns, the temporary
        butler contains one ``raw`` dataset corresponding to ``oid``, and the
        appropriate ``exposure`` dimension.

        Parameters
        ----------
        oid : `str`
            Identifier for incoming image, relative to the image bucket.

        Returns
        -------
        exposure_id : `int`
            The exposure ID of the image that was just ingested.
        """
        _log.info(f"Ingesting image id '{oid}'")
        file = ResourcePath(f"{self.image_host}/{oid}")
        if not file.isLocal:
            # TODO: RawIngestTask doesn't currently support remote files.
            file = self._download(file)
        result = self.rawIngestTask.run([file])
        # We only ingest one image at a time.
        # TODO: replace this assert with a custom exception, once we've decided
        # how we plan to handle exceptions in this code.
        assert len(result) == 1, "Should have ingested exactly one image."
        _log.info("Ingested one %s with dataId=%s", result[0].datasetType.name, result[0].dataId)
        return result[0].dataId["exposure"]

    def get_observed_skyangle(self, exposure: int) -> astropy.coordinates.Angle:
        """Determine the sky rotation angle with which an image was taken.

        Parameters
        ----------
        exposure : `int`
            The exposure to test. Must have already been ingested into the
            local repo.

        Returns
        -------
        angle : `astropy.coordinates.Angle` or `None`
            The observed rotation angle.
        """
        records = self.repo.butler.query_dimension_records("exposure",
                                                           instrument=self.instrument.getName(),
                                                           exposure=exposure,
                                                           )
        if not records:
            raise ValueError(f"Unknown exposure {exposure}.")
        elif len(records) > 1:
            _log.warning("Found %d records for exposure %s.", len(records), exposure)
        raw_angle = records[0].sky_angle
        return astropy.coordinates.Angle(raw_angle, "deg") if raw_angle is not None else raw_angle

    def _get_graph_executor(self, butler, factory):
        """Create a QuantumGraphExecutor suitable for Prompt Processing.

        Parameters
        ----------
        butler : `lsst.daf.butler.Butler`
            The Butler for which the quantum graph will be generated
            and executed. Should match the Butler passed to
            SeparablePipelineExecutor.
        factory : `lsst.pipe.base.TaskFactory`
            The task factory used for pipeline execution. Should match
            the factory passed to SeparablePipelineExecutor.

        Returns
        -------
        executor : `lsst.pipe.base.quantum_graph_executor.QuantumGraphExecutor`
            The executor to use.
        """
        quantum_executor = SingleQuantumExecutor(
            butler=butler,
            task_factory=factory,
            assume_no_existing_outputs=True,  # Outputs cleared out on success *or* failure
            raise_on_partial_outputs=True,  # Only way to detect that partial outputs happened
        )
        graph_executor = MPGraphExecutor(
            # TODO: re-enable parallel execution once we can log as desired with CliLog or a successor
            # (see issues linked from DM-42063) AND once provenance is supported with multiprocessing.
            num_proc=1,  # Avoid spawning processes, because they bypass our logger
            timeout=2_592_000.0,  # In practice, timeout is never helpful; set to 30 days.
            quantum_executor=quantum_executor,
        )
        return graph_executor

    def _try_pipelines(self, pipelines, in_collections, data_ids, *, label):
        """Attempt to run pipelines from a prioritized list.

        On success, exactly one of the pipelines is run, with outputs going to
        a run named after the pipeline file.

        Parameters
        ----------
        pipelines : sequence [`str`]
            The pipeline file(s) to run, in decreasing order of preference.
        in_collections : sequence [`str`]
            Collections, usually containing previous outputs, to search (in
            order) when reading pipeline inputs. This list is prepended to the
            collections in ``self.repo.butler``.
        data_ids : `str`
            A query string, in the format of the ``where`` parameter to
            `lsst.daf.butler.query_data_ids`, specifying the data IDs
            over which to run the pipelines.
        label : `str`
            A unique name to disambiguate this pipeline run for logging
            purposes.

        Returns
        -------
        output_run : `str`
            The run to which the successful pipeline wrote its outputs.

        Raises
        ------
        activator.exception.InvalidPipelineError
            Raised if any pipeline could not be loaded/configured.
        activator.exception.NoGoodPipelinesError
            Raised if graph generation failed for all pipelines.
        activator.exception.PipelinePreExecutionError
            Raised if pipeline execution was attempted but pre-execution failed.
        activator.exception.PipelineExecutionError
            Raised if pipeline execution was attempted but failed.
        """
        # Try pipelines in order until one works.
        for pipeline_file in pipelines:
            try:
                pipeline = self._prep_pipeline(pipeline_file)
            except FileNotFoundError as e:
                raise InvalidPipelineError(f"Could not load {pipeline_file}.") from e
            output_run = runs.get_output_run(self.instrument, self._deployment, pipeline_file, self._day_obs)
            factory = lsst.pipe.base.TaskFactory()
            all_collections = [output_run] + in_collections + list(self.repo.butler.collections.defaults)
            with Butler(butler=self.repo.butler, collections=all_collections, run=output_run) as exec_butler:
                executor = SeparablePipelineExecutor(
                    exec_butler,
                    clobber_output=False,
                    skip_existing_in=[output_run],
                    task_factory=factory,
                )
                try:
                    with lsst.utils.timer.time_this(
                            _log, msg=f"executor.make_quantum_graph ({label})", level=logging.DEBUG):
                        qgraph = executor.build_quantum_graph(pipeline, where=data_ids)
                        # If this is a fresh (local) repo, then types like calexp,
                        # *Diff_diaSrcTable, etc. have not been registered.
                        qgraph.pipeline_graph.register_dataset_types(exec_butler)
                except (QuantumGraphBuilderError, FileNotFoundError, MissingDatasetTypeError):
                    _log.exception(f"Building quantum graph for {pipeline_file} failed.")
                    continue
                if len(qgraph) == 0:
                    # Diagnostic logs are the responsibility of GraphBuilder.
                    _log.error(f"Empty quantum graph for {pipeline_file}; see previous logs for details.")
                    continue
                try:
                    provenance_ref = self._make_provenance_ref(data_ids, output_run)
                except ProvenanceDimensionsError:
                    _log.exception(f"Failed to determine data ID for provenance for {pipeline_file}.")
                    continue
                # Past this point, partial execution creates datasets.
                # Don't retry -- either fail (raise) or break.

                _log.info(f"Running '{pipeline_file}' on {data_ids}")
                input_dataset_info = {
                    dataset_type_name: list(qgraph.datasets_by_type[dataset_type_name].keys())
                    for dataset_type_name, _ in qgraph.pipeline_graph.iter_overall_inputs()
                }
                _log.debug(f"Running with input datasets {input_dataset_info}.")
                try:
                    with lsst.utils.timer.time_this(
                            _log, msg=f"executor.run_pipeline ({label})", level=logging.DEBUG):
                        executor.run_pipeline(
                            qgraph,
                            graph_executor=self._get_graph_executor(exec_butler, factory),
                            provenance_dataset_ref=provenance_ref,
                        )
                        _log.info(f"{label.capitalize()} pipeline successfully run.")
                        return output_run
                except Exception as e:
                    raise PipelineExecutionError(f"Execution failed for {pipeline_file}.") from e
                finally:
                    # Refresh so that registry queries know the processed products.
                    self.repo.butler.registry.refresh()
            break
        else:
            raise NoGoodPipelinesError(f"No {label} pipeline graph could be built.")

    def _make_provenance_ref(self, where, output_run):
        """Make the provenance DatasetRef for a quantum graph.

        Parameters
        ----------
        where : `str`
            Butler query expression that can be related to a single
            ``{group, detector}`` data ID.
        output_run : `str`
            Output RUN collection.

        Returns
        -------
        ref : `lsst.daf.butler.DatasetRef`
            A reference to a to-be-written provenance dataset in ``output_run``.
        """
        query_results = self.repo.butler.query_data_ids(
            self._provenance_dataset_type.dimensions, where=where, explain=False
        )
        try:
            (data_id,) = query_results
        except ValueError:
            raise ProvenanceDimensionsError(
                f"Expected exactly one data ID for {self._provenance_dataset_type}; got {query_results}."
            ) from None
        return DatasetRef(self._provenance_dataset_type, data_id, run=output_run)

    def _run_preprocessing(self) -> None:
        """Preprocess a visit ahead of incoming image(s).

        The internal butler must contain all data and all dimensions needed to
        run the appropriate pipeline on this visit.

        Raises
        ------
        activator.exception.InvalidPipelineError
            Raised if any pipeline could not be loaded/configured.
        activator.exception.NoGoodPipelinesError
            Raised if graph generation failed for all pipelines.
        activator.exception.PipelinePreExecutionError
            Raised if pipeline execution was attempted but pre-execution failed.
        activator.exception.PipelineExecutionError
            Raised if pipeline execution was attempted but failed.
        """
        pipeline_files = self.get_pre_pipeline_files()
        if not pipeline_files:
            _log.info(f"No preprocessing pipeline configured for {self.visit}, skipping.")
            return

        # Inefficient, but most graph builders can't take equality constraints
        where = (
            f"instrument='{self.visit.instrument}' and detector={self.visit.detector} "
            f"and group='{self.visit.groupId}'"
        )
        preload_run = runs.get_preload_run(self.instrument, self._deployment, self._day_obs)

        self._try_pipelines(self.get_pre_pipeline_files(),
                            in_collections=[preload_run],
                            data_ids=where,
                            label="preprocessing",
                            )

    def _check_permanent_changes(self, where: str) -> bool:
        """Test whether the APDB, alert stream, or other external state has
        changed in a way that makes retries unsafe.

        Parameters
        ----------
        where : `str`
            A :ref:`Butler query string <daf_butler_queries>` identifying the
            current visit. The query should return exactly one visit.

        Returns
        ----------
        changes : `bool`
            `True` if changes have been made, `False` if retries are safe.
        """
        # Need dimension records to determine region.
        data_ids = self.repo.butler.query_data_ids(
            ["instrument", "visit", "detector"], where=where, with_dimension_records=True, explain=False
        )
        if len(data_ids) == 1:
            data_id = data_ids[0]
            # Check if processing happened already, this needs visit timestamp.
            # Use begin or end of timespan, whichever is defined.
            visit_time: astropy.time.Time | None = None
            if data_id.timespan is not None:
                visit_time = data_id.timespan.begin or data_id.timespan.end
            if visit_time is None:
                # Without timespan cannot call containsVisitDetector.
                _log.warning("No timespan defined for visit: %s. Assuming APDB modified.", data_id)
                return True
            apdb = lsst.dax.apdb.Apdb.from_uri(self._apdb_config)
            with lsst.utils.timer.time_this(_log, msg="Apdb.containsVisitDetector", level=logging.DEBUG):
                return apdb.containsVisitDetector(
                    data_id["visit"], self.visit.detector, region=data_id.region, visit_time=visit_time
                )
        elif not data_ids:
            # Engineering exposures don't produce visits, but they also can't write to the APDB.
            return False
        else:
            # Don't know how this could happen, so won't try to handle it gracefully.
            _log.warning("Unexpected visit ids: %s. Assuming APDB modified.", data_ids)
            return True

    def run_pipeline(self, exposure_ids: set[int]) -> None:
        """Process the received image(s).

        The internal butler must contain all data and all dimensions needed to
        run the appropriate pipeline on this visit and ``exposure_ids``, except
        for the ``visit`` dimension itself.

        Parameters
        ----------
        exposure_ids : `set` [`int`]
            Identifiers of the exposures that were received.

        Raises
        ------
        activator.exception.InvalidPipelineError
            Raised if any pipeline could not be loaded/configured.
        activator.exception.NoGoodPipelinesError
            Raised if graph generation failed for all pipelines.
        activator.exception.PipelinePreExecutionError
            Raised if pipeline execution was attempted but pre-execution failed.
        activator.exception.PipelineExecutionError
            Raised if pipeline execution was attempted but failed, and neither
            `~activator.exception.NonRetriableError` nor
            `~activator.exception.RetriableError` apply.
        activator.exception.NonRetriableError
            Raised if external resources (such as the APDB or alert stream)
            may have been left in a state that makes it unsafe to retry
            failures. This exception is always chained to another exception
            representing the original error.
        activator.exception.RetriableError
            Raised if the conditions for NonRetriableError are not met, *and*
            the pipeline fails in a way that is expected to be transient. This
            exception is always chained to another exception representing the
            original error.
        """
        # TODO: we want to define visits earlier, but we have to ingest a
        # faked raw file and appropriate SSO data during prep (and then
        # cleanup when ingesting the real data).
        try:
            self.define_visits.run({"instrument": self.instrument.getName(),
                                    "exposure": exp} for exp in exposure_ids)
        except lsst.daf.butler.registry.DataIdError as e:
            # TODO: a good place for a custom exception?
            raise RuntimeError("No data to process.") from e

        # Inefficient, but most graph builders can't take equality constraints
        where = (
            f"instrument='{self.visit.instrument}' and detector={self.visit.detector}"
            f" and exposure in ({','.join(str(x) for x in exposure_ids)})"
        )
        preload_run = runs.get_preload_run(self.instrument, self._deployment, self._day_obs)
        pre_runs = [runs.get_output_run(self.instrument, self._deployment, f, self._day_obs)
                    for f in self.get_pre_pipeline_files()]

        try:
            self._try_pipelines(self.get_main_pipeline_files(),
                                in_collections=pre_runs + [preload_run],
                                data_ids=where,
                                label="main",
                                )
        # Catch Exception just in case there's a surprise -- raising
        # NonRetriableError on *all* irrevocable changes is important.
        except (Exception, GracefulShutdownInterrupt, TimeoutInterrupt) as e:
            try:
                state_changed = self._check_permanent_changes(where)
            except (Exception, GracefulShutdownInterrupt, TimeoutInterrupt):
                # Failure in registry or APDB queries
                _log.exception("Could not determine APDB state, assuming modified.")
                raise NonRetriableError("APDB potentially modified") from e
            else:
                if state_changed:
                    raise NonRetriableError("APDB modified") from e
                elif isinstance(e, GracefulShutdownInterrupt):
                    raise RetriableError("External interrupt") from e
                elif isinstance(e, TimeoutInterrupt):
                    raise RetriableError("Processing timed out, assuming transient problem.") from e
                else:
                    raise

    def export_outputs(self, exposure_ids: set[int]) -> None:
        """Copy pipeline outputs from processing a set of images back
        to the central Butler.

        Parameters
        ----------
        exposure_ids : `set` [`int`]
            Identifiers of the exposures that were processed.
        """
        if not do_export:
            _log.info("Skipping central repo export for exposures %s.", exposure_ids)
            return

        env_export_patterns = os.environ.get("EXPORT_TYPE_REGEXP", "- .*")
        try:
            export_patterns = yaml.safe_load(env_export_patterns)
            if not isinstance(export_patterns, list):
                raise ValueError
        except ValueError:
            _log.error(
                "Invalid EXPORT_TYPE_REGEXP=%s. Export all dataset types.",
                env_export_patterns,
            )
            export_patterns = [".*"]
        export_types = set(
            data_type
            for data_type in self._get_safe_dataset_types(self.repo.butler)
            for pattern in export_patterns
            if re.fullmatch(pattern, data_type)
        )
        _log.debug(f"Will export datasets {export_types}")
        # Rather than determining which pipeline was run, just try to export all of them.
        output_runs = [runs.get_preload_run(self.instrument, self._deployment, self._day_obs)]
        for f in self.get_combined_pipeline_files():
            output_runs.append(runs.get_output_run(self.instrument, self._deployment, f, self._day_obs))
        try:
            with lsst.utils.timer.time_this(_log, msg="export_outputs", level=logging.DEBUG):
                exports = self._export_subset(exposure_ids,
                                              # TODO: find a way to merge datasets like *_config
                                              # or *_schema that are duplicated across multiple
                                              # workers.
                                              export_types,
                                              in_collections=output_runs,
                                              )
                if exports:
                    populated_runs = {ref.run for ref in exports}
                    _log.info(f"Pipeline products saved to collections {populated_runs}.")
                else:
                    _log.warning("No output datasets match visit=%s and exposures=%s.",
                                 self.visit, exposure_ids)

        finally:
            # TODO: can we use SasquatchDatastore to streamline this?
            dispatcher = _get_sasquatch_dispatcher()
            if dispatcher:
                with lsst.utils.timer.time_this(_log, msg="upload metrics", level=logging.DEBUG):
                    # Making bundles a collection makes debug log simpler, and it should be short.
                    bundles = list(self._query_datasets_by_storage_class(
                        self.repo.butler, exposure_ids, output_runs, "MetricMeasurementBundle"))
                    for bundle in bundles:
                        try:
                            _log_trace.debug("Uploading %s...", bundle)
                            dispatcher.dispatchRef(self.repo.butler.get(bundle), bundle)
                        except (SasquatchDispatchFailure, SasquatchDispatchPartialFailure):
                            # Retries can get messy with multiple bundles, so just abort.
                            _log.exception("Failed to upload %s to Sasquatch: ", bundle)
                _log.debug("Uploaded %d metrics to %s.", len(bundles), dispatcher.url)

    @staticmethod
    def _get_safe_dataset_types(butler):
        """Return the set of dataset types that can be safely merged from a worker.

        Parameters
        ----------
        butler : `lsst.daf.butler.Butler`
            The butler in which to search for dataset types.

        Returns
        -------
        types : iterable [`str`]
            The dataset types to return.
        """
        return [dstype.name for dstype in butler.registry.queryDatasetTypes(...)
                if "detector" in dstype.dimensions]

    @connect.retry(2, DATASTORE_EXCEPTIONS, wait=repo_retry)
    def _export_subset(self, exposure_ids: set[int],
                       dataset_types: typing.Any, in_collections: typing.Any
                       ) -> collections.abc.Collection[DatasetRef]:
        """Copy datasets associated with a processing run back to the
        central Butler.

        Parameters
        ----------
        exposure_ids : `set` [`int`]
            Identifiers of the exposures that were processed.
        dataset_types
            The dataset type(s) to transfer; can be any expression described in
            :ref:`daf_butler_dataset_type_expressions`.
        in_collections
            The collections to transfer from; can be any expression described
            in :ref:`daf_butler_collection_expressions`.

        Returns
        -------
        datasets : collection [`~lsst.daf.butler.DatasetRef`]
            The datasets exported (may be empty).
        """
        with lsst.utils.timer.time_this(_log, msg="export_outputs (find outputs)", level=logging.DEBUG):
            try:
                datasets = set()
                for dataset_type in dataset_types:
                    datasets |= set(self.repo.butler.query_datasets(
                        dataset_type,
                        collections=in_collections,
                        # in_collections may include other runs, so need to filter.
                        # Since AP processing is strictly visit-detector, these three
                        # dimensions should suffice.
                        # DO NOT assume that visit == exposure!
                        where="exposure in (exposure_ids)",
                        bind={"exposure_ids": exposure_ids},
                        with_dimension_records=True,
                        find_first=False,  # Transfer ALL output datasets
                        explain=False,  # Failed runs might not have datasets of every type.
                        instrument=self.instrument.getName(),
                        detector=self.visit.detector,
                    ))
            except lsst.daf.butler.registry.DataIdError as e:
                raise ValueError("Invalid visit or exposures.") from e

            # Transfer dimensions created by ingest in case it was never done in
            # central repo (which is normal for dev).
            # Transferring governor dimensions in parallel can cause deadlocks in
            # central registry. We need to transfer our exposure/visit dimensions,
            # so handle those manually.
            dimension_records = self._get_dimension_records_to_export(
                self.repo.butler,
                where="exposure in (exposure_ids)",
                bind={"exposure_ids": exposure_ids},
                instrument=self.instrument.getName(),
                detector=self.visit.detector,
            )

        with lsst.utils.timer.time_this(_log, msg="export_outputs (transfer)", level=logging.DEBUG):
            transferred = self._butler_writer.transfer_outputs(self.repo.butler,
                                                               dimension_records,
                                                               list(datasets))
            _check_transfer_completion(datasets, transferred, "Uploaded")

        return transferred

    @staticmethod
    def _get_dimension_records_to_export(butler: Butler, **kwargs) -> GroupedDimensionRecords:
        """Retrieve dimension records generated from an exposure that need to
        be transferred to the central repo.

        In many cases the exposure records retrieved here will already exist in
        the central repo, but this is not guaranteed (especially in dev
        environments).

        Parameters
        ----------
        butler : `lsst.daf.butler.Butler`
            The butler from which to retrieve dimension records.
        **kwargs
            Any data ID parameters to select specific records. They have the
            same meanings as the parameters of
            `lsst.daf.butler.Butler.query_dimension_records`.

        Returns
        -------
        dimension_records : `dict` [`str` , `list` [`lsst.daf.butler.DimensionRecord`]]
            Dictionary from dimension name to list of dimension records for that dimension.
        """
        core_dimensions = ["group",
                           "day_obs",
                           "exposure",
                           "visit",
                           "visit_system",
                           ]
        universe = butler.dimensions

        full_dimensions = [universe[d] for d in core_dimensions if d in universe]
        extra_dimensions = []
        for d in full_dimensions:
            extra_dimensions.extend(universe.get_elements_populated_by(universe[d]))
        dimensions = full_dimensions + extra_dimensions

        records = {}
        for dimension in dimensions:
            records[dimension.name] = butler.query_dimension_records(dimension, explain=False, **kwargs)
        return records

    def _query_datasets_by_storage_class(self, butler, exposure_ids, collections, storage_class):
        """Identify all datasets with a particular storage class, regardless of
        dataset type.

        Parameters
        ----------
        butler : `lsst.daf.butler.Butler`
            The Butler in which to query for datasets.
        exposure_ids : `set` [`int`]
            Exposure IDs for which to return datasets.
        collections : iterable [`str`]
            The collections in which to query for datasets.
        storage_class : `str`
            The name of the storage class by which to query.

        Yields
        ------
        dataset : `lsst.daf.butler.DatasetRef`
            A dataset in ``collections`` of type ``storage_class``. Guaranteed
            to include values for implied dimensions, but need not include
            dimension records. The order in which datasets are returned
            is undefined.
        """
        matching_types = {dtype for dtype in butler.registry.queryDatasetTypes(...)
                          if dtype.storageClass_name == storage_class}
        _log.debug("Found dataset types matching %s: %s", storage_class, {t.name for t in matching_types})
        yield from itertools.chain.from_iterable(
            butler.query_datasets(t,
                                  collections=collections,
                                  find_first=True,
                                  # collections may include other runs, so need to filter.
                                  # Since AP processing is strictly visit-detector, these three
                                  # dimensions should suffice.
                                  # DO NOT assume that visit == exposure!
                                  where="exposure in (exposure_ids)",
                                  bind={"exposure_ids": exposure_ids},
                                  explain=False,
                                  instrument=self.instrument.getName(),
                                  detector=self.visit.detector,
                                  ) for t in matching_types
        )


_DatasetResults: typing.TypeAlias = collections.abc.Iterable[lsst.daf.butler.DatasetRef]
"""Type alias for dataset query results, to simplify annotations."""


def _check_transfer_completion(expected, transferred, transfer_type):
    """Test whether a Butler transfer ran to completion.

    Current behavior on an incomplete transfer is to log a warning.

    Parameters
    ----------
    expected : collection [`lsst.daf.butler.DatasetRef`]
        The datasets that were marked for transfer.
    transferred : collection [`lsst.daf.butler.DatasetRef`]
        The datasets that were successfully transferred.
    transfer_type : `str`
        A brief description of the transfer. Should be a past-tense verb.

    Returns
    -------
    missing : set `lsst.daf.butler.DatasetRef`
        The datasets that were not transferred.
    """
    # Count only unique datasets
    expected_s = set(expected)
    transferred_s = set(transferred)
    missing = expected_s - transferred_s
    if missing:
        _log.warning("%s only %d datasets out of %d; missing %s.",
                     transfer_type.capitalize(),
                     len(transferred_s), len(expected_s),
                     missing)
    return missing
