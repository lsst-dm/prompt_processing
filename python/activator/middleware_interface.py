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

__all__ = ["get_central_butler", "MiddlewareInterface"]

import collections.abc
import itertools
import logging
import os
import os.path
import tempfile
import typing

from lsst.utils import getPackageDir
from lsst.resources import ResourcePath
import lsst.afw.cameraGeom
from lsst.ctrl.mpexec import SimplePipelineExecutor
from lsst.daf.butler import Butler, CollectionType
import lsst.geom
from lsst.meas.algorithms.htmIndexer import HtmIndexer
import lsst.obs.base
import lsst.pipe.base

from .visit import Visit

_log = logging.getLogger("lsst." + __name__)
_log.setLevel(logging.DEBUG)


def get_central_butler(central_repo: str, instrument_class: str):
    """Provide a Butler that can access the given repository and read and write
    data for the given instrument.

    Parameters
    ----------
    central_repo : `str`
        The path or URI to the central repository.
    instrument_class : `str`
        The name of the instrument whose data will be retrieved or written. May
        be either the fully qualified class name or the short name.

    Returns
    -------
    butler : `lsst.daf.butler.Butler`
        A Butler for ``central_repo`` pre-configured to load and store
        ``instrument_name`` data.
    """
    # TODO: how much overhead do we take on from asking the central repo about
    # the instrument instead of handling it internally?
    registry = Butler(central_repo).registry
    instrument = lsst.obs.base.Instrument.from_string(instrument_class, registry)
    return Butler(central_repo,
                  collections=[instrument.makeCollectionName("defaults")],
                  writeable=True,
                  inferDefaults=False,
                  )


class MiddlewareInterface:
    """Interface layer between the Butler middleware and the prompt processing
    data handling system, to handle processing individual images.

    An instance of this class will accept an incoming group of single-detector
    snaps to process, using an instance-local butler repo. The instance can
    pre-load the necessary calibrations to process an incoming detector-visit,
    ingest the data when it is available, and run the difference imaging
    pipeline, all in that local butler.

    Each instance may be used for processing more than one group-detector
    combination, designated by the `Visit` parameter to certain methods. There
    is no guarantee that a processing run may, or may not, share a group,
    detector, or both with a previous run handled by the same object.

    ``MiddlewareInterface`` objects are not thread- or process-safe, and must
    not share any state with other instances (see ``butler`` in the parameter
    list). They may, however, share a ``central_butler``, and concurrent
    operations on this butler are guaranteed to be appropriately synchronized.

    Parameters
    ----------
    central_butler : `lsst.daf.butler.Butler`
        Butler repo containing the calibration and other data needed for
        processing images as they are received. This butler must be created
        with the default instrument, skymap, and collections assigned.
    image_bucket : `str`
        Storage bucket where images will be written to as they arrive.
        See also ``prefix``.
    instrument : `str`
        Name of the instrument taking the data, for populating
        butler collections and dataIds. May be either the fully qualified class
        name or the short name. Examples: "LsstCam", "lsst.obs.lsst.LsstCam".
        TODO: this arg can probably be removed and replaced with internal
        use of the butler.
    local_storage : `str`
        An absolute path to a space where this object can create a local
        Butler repo. The repo is guaranteed to be unique to this object.
    prefix : `str`, optional
        URI scheme followed by ``://``; prepended to ``image_bucket`` when
        constructing URIs to retrieve incoming files. The default is
        appropriate for use in the USDF environment; typically only
        change this when running local tests.
    """
    _COLLECTION_TEMPLATE = "templates"
    """The collection used for templates.
    """
    _COLLECTION_SKYMAP = "skymaps"
    """The collection used for skymaps.
    """

    # Class invariants:
    # self._apdb_uri is a valid URI that unambiguously identifies the APDB
    # self.image_host is a valid URI with non-empty path and no query or fragment.
    # self._download_store is None if and only if self.image_host is a local URI.
    # self.instrument, self.camera, and self.skymap do not change after __init__.
    # self._repo is the only reference to its TemporaryDirectory object.
    # self.butler defaults to a chained collection named
    #   self.output_collection, which contains zero or more output runs,
    #   pre-made inputs, and raws, in that order. However, self.butler is not
    #   guaranteed to contain concrete data, or even the dimensions
    #   corresponding to self.camera and self.skymap.
    # if it exists, the latest run in self.output_collection is always the first
    # in the chain.

    def __init__(self, central_butler: Butler, image_bucket: str, instrument: str,
                 local_storage: str,
                 prefix: str = "s3://"):
        self._apdb_uri = self._make_apdb_uri()
        self._apdb_namespace = os.environ.get("NAMESPACE_APDB", None)
        self.central_butler = central_butler
        self.image_host = prefix + image_bucket
        # TODO: _download_store turns MWI into a tagged class; clean this up later
        if not self.image_host.startswith("file"):
            self._download_store = tempfile.TemporaryDirectory(prefix="holding-")
        else:
            self._download_store = None
        # TODO: how much overhead do we pick up from going through the registry?
        self.instrument = lsst.obs.base.Instrument.from_string(instrument, central_butler.registry)

        self.output_collection = self.instrument.makeCollectionName("prompt")

        self._init_local_butler(local_storage)
        self._init_ingester()
        self._init_visit_definer()

        # HACK: explicit collection gets around the fact that we don't have any
        # timestamp/exposure information in a form we can pass to the Butler.
        # This code will break once cameras start being versioned.
        self.camera = self.central_butler.get(
            "camera", instrument=self.instrument.getName(),
            collections=self.instrument.makeUnboundedCalibrationRunName()
        )
        # TODO: is central_butler guaranteed to have only one skymap dimension?
        skymaps = list(self.central_butler.registry.queryDataIds("skymap"))
        assert len(skymaps) == 1, "Ambiguous or missing skymap in central repo."
        self.skymap_name = skymaps[0]["skymap"]
        self.skymap = self.central_butler.get("skyMap", skymap=self.skymap_name)

        # How much to pad the refcat region we will copy over.
        self.padding = 30*lsst.geom.arcseconds

    def _make_apdb_uri(self):
        """Generate a URI for accessing the APDB.
        """
        # TODO: merge this code with make_pgpass.py
        ip_apdb = os.environ["IP_APDB"]  # Also includes port
        db_apdb = os.environ["DB_APDB"]
        user_apdb = os.environ.get("USER_APDB", "postgres")
        return f"postgresql://{user_apdb}@{ip_apdb}/{db_apdb}"

    def _init_local_butler(self, base_path: str):
        """Prepare the local butler to ingest into and process from.

        ``self.instrument`` must already exist. ``self.butler`` is correctly
        initialized after this method returns, and is guaranteed to be unique
        to this object.

        Parameters
        ----------
        base_path : `str`
            An absolute path to a space where the repo can be created.
        """
        # Directory has same lifetime as this object.
        self._repo = tempfile.TemporaryDirectory(dir=base_path, prefix="butler-")
        butler = Butler(Butler.makeRepo(self._repo.name), writeable=True)
        _log.info("Created local Butler repo at %s.", self._repo.name)

        self.instrument.register(butler.registry)

        # Will be populated in prep_butler.
        butler.registry.registerCollection(self.instrument.makeUmbrellaCollectionName(),
                                           CollectionType.CHAINED)
        # Will be populated on ingest.
        butler.registry.registerCollection(self.instrument.makeDefaultRawIngestRunName(),
                                           CollectionType.CHAINED)
        # Will be populated on pipeline execution.
        butler.registry.registerCollection(self.output_collection, CollectionType.CHAINED)
        collections = [self.instrument.makeUmbrellaCollectionName(),
                       self.instrument.makeDefaultRawIngestRunName(),
                       ]
        butler.registry.setCollectionChain(self.output_collection, collections)

        # Internal Butler keeps a reference to the newly prepared collection.
        # This reference makes visible any inputs for query purposes. Output
        # runs are execution-specific and must be provided explicitly to the
        # appropriate calls.
        self.butler = Butler(butler=butler,
                             collections=[self.output_collection],
                             )

    def _init_ingester(self):
        """Prepare the raw file ingester to receive images into this butler.

        ``self._init_local_butler`` must have already been run.
        """
        config = lsst.obs.base.RawIngestConfig()
        config.transfer = "copy"  # Copy files into the local butler.
        # TODO: Could we use the `on_ingest_failure` and `on_success` callbacks
        # to send information back to this interface?
        config.failFast = True  # We want failed ingests to fail immediately.
        self.rawIngestTask = lsst.obs.base.RawIngestTask(config=config,
                                                         butler=self.butler)

    def _init_visit_definer(self):
        """Prepare the visit definer to define visits for this butler.

        ``self._init_local_butler`` must have already been run.
        """
        define_visits_config = lsst.obs.base.DefineVisitsConfig()
        self.define_visits = lsst.obs.base.DefineVisitsTask(config=define_visits_config, butler=self.butler)

    def _predict_wcs(self, detector: lsst.afw.cameraGeom.Detector, visit: Visit) -> lsst.afw.geom.SkyWcs:
        """Calculate the expected detector WCS for an incoming observation.

        Parameters
        ----------
        detector : `lsst.afw.cameraGeom.Detector`
            The detector for which to generate a WCS.
        visit : `Visit`
            Predicted observation metadata for the detector.

        Returns
        -------
        wcs : `lsst.afw.geom.SkyWcs`
            An approximate WCS for ``visit``.
        """
        boresight_center = lsst.geom.SpherePoint(visit.ra, visit.dec, lsst.geom.degrees)
        orientation = lsst.geom.Angle(visit.rot, lsst.geom.degrees)
        flip_x = True if self.instrument.getName() == "DECam" else False
        return lsst.obs.base.createInitialSkyWcsFromBoresight(boresight_center,
                                                              orientation,
                                                              detector,
                                                              flipX=flip_x)

    def _detector_bounding_circle(self, detector: lsst.afw.cameraGeom.Detector,
                                  wcs: lsst.afw.geom.SkyWcs
                                  ) -> (lsst.geom.SpherePoint, lsst.geom.Angle):
        # Could return a sphgeom.Circle, but that would require a lot of
        # sphgeom->geom conversions downstream. Even their Angles are different!
        """Compute a small sky circle that contains the detector.

        Parameters
        ----------
        detector : `lsst.afw.cameraGeom.Detector`
            The detector for which to compute an on-sky bounding circle.
        wcs : `lsst.afw.geom.SkyWcs`
            The conversion from detector to sky coordinates.

        Returns
        -------
        center : `lsst.geom.SpherePoint`
            The center of the bounding circle.
        radius : `lsst.geom.Angle`
            The opening angle of the bounding circle.
        """
        radii = []
        center = wcs.pixelToSky(detector.getCenter(lsst.afw.cameraGeom.PIXELS))
        for corner in detector.getCorners(lsst.afw.cameraGeom.PIXELS):
            radii.append(wcs.pixelToSky(corner).separation(center))
        return center, max(radii)

    def prep_butler(self, visit: Visit) -> None:
        """Prepare a temporary butler repo for processing the incoming data.

        After this method returns, the internal butler is guaranteed to contain
        all data and all dimensions needed to run the appropriate pipeline on ``visit``,
        except for ``raw`` and the ``exposure`` and ``visit`` dimensions,
        respectively. It may contain other data that would not be loaded when
        processing ``visit``.

        Parameters
        ----------
        visit : `Visit`
            Group of snaps from one detector to prepare the butler for.
        """
        _log.info(f"Preparing Butler for visit {visit!r}")

        detector = self.camera[visit.detector]
        wcs = self._predict_wcs(detector, visit)
        center, radius = self._detector_bounding_circle(detector, wcs)

        with tempfile.NamedTemporaryFile(mode="w+b", suffix=".yaml") as export_file:
            with self.central_butler.export(filename=export_file.name, format="yaml") as export:
                self._export_refcats(export, center, radius)
                self._export_skymap_and_templates(export, center, detector, wcs, visit.filter)
                self._export_calibs(export, visit.detector, visit.filter)

                # CHAINED collections
                export.saveCollection(self.instrument.makeRefCatCollectionName())
                export.saveCollection(self._COLLECTION_TEMPLATE)
                export.saveCollection(self.instrument.makeUmbrellaCollectionName())

            self.butler.import_(filename=export_file.name,
                                directory=self.central_butler.datastore.root,
                                transfer="copy")

        # Create unique run to store this visit's raws. This makes it easier to
        # clean up the central repo later.
        # TODO: not needed after DM-36051, or if we find a way to give all test
        # raws unique exposure IDs.
        input_raws = self._get_raw_run_name(visit)
        self.butler.registry.registerCollection(input_raws, CollectionType.RUN)
        _prepend_collection(self.butler, self.instrument.makeDefaultRawIngestRunName(), [input_raws])

    def _export_refcats(self, export, center, radius):
        """Export the refcats for this visit from the central butler.

        Parameters
        ----------
        export : `Iterator[RepoExportContext]`
            Export context manager.
        center : `lsst.geom.SpherePoint`
            Center of the region to find refcat shards in.
        radius : `lst.geom.Angle`
            Radius to search for refcat shards in.
        """
        indexer = HtmIndexer(depth=7)
        shard_ids, _ = indexer.getShardIds(center, radius+self.padding)
        htm_where = f"htm7 in ({','.join(str(x) for x in shard_ids)})"
        # Get shards from all refcats that overlap this detector.
        # TODO: `...` doesn't work for this queryDatasets call
        # currently, and we can't queryDatasetTypes in just the refcats
        # collection, so we have to specify a list here. Replace this
        # with another solution ASAP.
        possible_refcats = ["gaia", "panstarrs", "gaia_dr2_20200414", "ps1_pv3_3pi_20170110"]
        refcats = set(_query_missing_datasets(
                      self.central_butler, self.butler,
                      possible_refcats,
                      collections=self.instrument.makeRefCatCollectionName(),
                      where=htm_where,
                      findFirst=True))
        _log.debug("Found %d new refcat datasets.", len(refcats))
        export.saveDatasets(refcats)

    def _export_skymap_and_templates(self, export, center, detector, wcs, filter):
        """Export the skymap and templates for this visit from the central
        butler.

        Parameters
        ----------
        export : `Iterator[RepoExportContext]`
            Export context manager.
        center : `lsst.geom.SpherePoint`
            Center of the region to load the skyamp tract/patches for.
        detector : `lsst.afw.cameraGeom.Detector`
            Detector we are loading data for.
        wcs : `lsst.afw.geom.SkyWcs`
            Rough WCS for the upcoming visit, to help finding patches.
        filter : `str`
            Physical filter for which to export templates.
        """
        # TODO: This exports the whole skymap, but we want to only export the
        # subset of the skymap that covers this data.
        skymaps = set(_query_missing_datasets(self.central_butler, self.butler,
                                              "skyMap",
                                              skymap=self.skymap_name,
                                              collections=self._COLLECTION_SKYMAP,
                                              findFirst=True))
        _log.debug("Found %d new skymap datasets.", len(skymaps))
        export.saveDatasets(skymaps)
        # Getting only one tract should be safe: we're getting the
        # tract closest to this detector, so we should be well within
        # the tract bbox.
        tract = self.skymap.findTract(center)
        points = [center]
        for corner in detector.getCorners(lsst.afw.cameraGeom.PIXELS):
            points.append(wcs.pixelToSky(corner))
        patches = tract.findPatchList(points)
        patches_str = ','.join(str(p.sequential_index) for p in patches)
        template_where = f"patch in ({patches_str}) and tract={tract.tract_id} and physical_filter='{filter}'"
        # TODO: do we need to have the coadd name used in the pipeline
        # specified as a class kwarg, so that we only load one here?
        # TODO: alternately, we need to extract it from the pipeline? (best?)
        # TODO: alternately, can we just assume that there is exactly
        # one coadd type in the central butler?
        templates = set(_query_missing_datasets(self.central_butler, self.butler,
                                                "*Coadd",
                                                collections=self._COLLECTION_TEMPLATE,
                                                instrument=self.instrument.getName(),
                                                skymap=self.skymap_name,
                                                where=template_where))
        _log.debug("Found %d new template datasets.", len(templates))
        export.saveDatasets(templates)

    def _export_calibs(self, export, detector_id, filter):
        """Export the calibs for this visit from the central butler.

        Parameters
        ----------
        export : `Iterator[RepoExportContext]`
            Export context manager.
        detector_id : `int`
            Identifier of the detector to load calibs for.
        filter : `str`
            Physical filter name of the upcoming visit.
        """
        # TODO: we can't filter by validity range because it's not
        # supported in queryDatasets yet.
        calib_where = f"detector={detector_id} and physical_filter='{filter}'"
        calibs = set(_query_missing_datasets(
            self.central_butler, self.butler,
            ...,
            collections=self.instrument.makeCalibrationCollectionName(),
            instrument=self.instrument.getName(),
            where=calib_where))
        if calibs:
            for dataset_type, n_datasets in self._count_by_type(calibs):
                _log.debug("Found %d new calib datasets of type '%s'.", n_datasets, dataset_type)
        else:
            _log.debug("Found 0 new calib datasets.")
        export.saveDatasets(
            calibs,
            elements=[])  # elements=[] means do not export dimension records
        target_types = {CollectionType.CALIBRATION}
        for collection in self.central_butler.registry.queryCollections(...,
                                                                        collectionTypes=target_types):
            export.saveCollection(collection)

    @staticmethod
    def _count_by_type(refs):
        """Count the number of dataset references of each type.

        Parameters
        ----------
        refs : iterable [`lsst.daf.butler.DatasetRef`]
            The references to classify.

        Yields
        ------
        type : `str`
            The name of a dataset type in ``refs``.
        count : `int`
            The number of elements of type ``type`` in ``refs``.
        """
        def get_key(ref):
            return ref.datasetType.name

        ordered = sorted(refs, key=get_key)
        for k, g in itertools.groupby(ordered, key=get_key):
            yield k, len(list(g))

    def _get_raw_run_name(self, visit):
        """Define a run collection specific to a particular visit.

        Parameters
        ----------
        visit : Visit
            The visit whose raws will be stored in this run.

        Returns
        -------
        run : `str`
            The name of a run collection to use for raws.
        """
        return self.instrument.makeCollectionName("raw", visit.group)

    def _prep_collections(self):
        """Pre-register output collections in advance of running the pipeline.

        Returns
        -------
        run : `str`
            The name of a new run collection to use for outputs. The run name
            is prefixed by ``self.output_collection``.
        """
        # NOTE: Because we receive a butler on init, we can't use this
        # prep_butler() because it takes a repo path.
        # butler = SimplePipelineExecutor.prep_butler(
        #     self.repo,
        #     inputs=[self.calibration_collection,
        #             self.instrument.makeDefaultRawIngestRunName(),
        #             'refcats'],
        #     output=self.output_collection)
        # The below is taken from SimplePipelineExecutor.prep_butler.
        # TODO: currently the run **must** be unique for each unit of
        # processing. It must be unique per group because in the prototype the
        # same exposures may be rerun under different group IDs, and they must
        # be unique per detector because the same worker may be tasked with
        # different detectors from the same group. Replace with a single
        # universal run on DM-36586.
        output_run = f"{self.output_collection}/{self.instrument.makeCollectionTimestamp()}"
        self.butler.registry.registerCollection(output_run, CollectionType.RUN)
        _prepend_collection(self.butler, self.output_collection, [output_run])
        return output_run

    def _prep_pipeline(self, visit: Visit) -> None:
        """Setup the pipeline to be run, based on the configured instrument and
        details of the incoming visit.

        Parameters
        ----------
        visit : Visit
            Group of snaps from one detector to prepare the pipeline for.

        Returns
        -------
        pipeline : `lsst.pipe.base.Pipeline`
            The pipeline to run for ``visit``.

        Raises
        ------
        RuntimeError
            Raised if there is no AP pipeline file for this configuration.
            TODO: could be a good case for a custom exception here.
        """
        # TODO: We hacked the basepath in the Dockerfile so this works both in
        # development and in service container, but it would be better if there
        # were a path that's valid in both.
        ap_pipeline_file = os.path.join(getPackageDir("prompt_prototype"),
                                        "pipelines", self.instrument.getName(), "ApPipe.yaml")
        try:
            pipeline = lsst.pipe.base.Pipeline.fromFile(ap_pipeline_file)
        except FileNotFoundError:
            raise RuntimeError(f"No ApPipe.yaml defined for camera {self.instrument.getName()}")
        pipeline.addConfigOverride("diaPipe", "apdb.db_url", self._apdb_uri)
        pipeline.addConfigOverride("diaPipe", "apdb.namespace", self._apdb_namespace)
        return pipeline

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
        # TODO: this requires the service account to have the otherwise admin-ish
        # storage.buckets.get permission (DM-34188). Once that's resolved, see if
        # prompt-service can do without the "Storage Legacy Bucket Reader" role.
        local.transfer_from(remote, "copy")
        return local

    def ingest_image(self, visit: Visit, oid: str) -> None:
        """Ingest an image into the temporary butler.

        The temporary butler must not already contain a ``raw`` dataset
        corresponding to ``oid``. After this method returns, the temporary
        butler contains one ``raw`` dataset corresponding to ``oid``, and the
        appropriate ``exposure`` dimension.

        Parameters
        ----------
        visit : Visit
            The visit for which the image was taken.
        oid : `str`
            Identifier for incoming image, relative to the image bucket.
        """
        # TODO: consider allowing pre-existing raws, as may happen when a
        # pipeline is rerun (see DM-34141).
        _log.info(f"Ingesting image id '{oid}'")
        file = ResourcePath(f"{self.image_host}/{oid}")
        if not file.isLocal:
            # TODO: RawIngestTask doesn't currently support remote files.
            file = self._download(file)
        result = self.rawIngestTask.run([file], run=self._get_raw_run_name(visit))
        # We only ingest one image at a time.
        # TODO: replace this assert with a custom exception, once we've decided
        # how we plan to handle exceptions in this code.
        assert len(result) == 1, "Should have ingested exactly one image."
        _log.info("Ingested one %s with dataId=%s", result[0].datasetType.name, result[0].dataId)

    def run_pipeline(self, visit: Visit, exposure_ids: set[int]) -> None:
        """Process the received image(s).

        The internal butler must contain all data and all dimensions needed to
        run the appropriate pipeline on ``visit`` and ``exposure_ids``, except
        for the ``visit`` dimension itself.

        Parameters
        ----------
        visit : Visit
            Group of snaps from one detector to be processed.
        exposure_ids : `set` [`int`]
            Identifiers of the exposures that were received.
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

        output_run = self._prep_collections()

        where = f"instrument='{visit.instrument}' and detector={visit.detector} " \
                f"and exposure in ({','.join(str(x) for x in exposure_ids)})"
        pipeline = self._prep_pipeline(visit)
        executor = SimplePipelineExecutor.from_pipeline(pipeline,
                                                        where=where,
                                                        butler=Butler(butler=self.butler,
                                                                      collections=self.butler.collections,
                                                                      run=output_run))
        if len(executor.quantum_graph) == 0:
            # TODO: a good place for a custom exception?
            raise RuntimeError("No data to process.")
        _log.info(f"Running '{pipeline._pipelineIR.description}' on {where}")
        # If this is a fresh (local) repo, then types like calexp,
        # *Diff_diaSrcTable, etc. have not been registered.
        result = executor.run(register_dataset_types=True)
        _log.info(f"Pipeline successfully run on {len(result)} quanta for "
                  f"detector {visit.detector} of {exposure_ids}.")

    def export_outputs(self, visit: Visit, exposure_ids: set[int]) -> None:
        """Copy raws and pipeline outputs from processing a set of images back
        to the central Butler.

        The copied raws can be found in the collection ``<instrument>/raw/all``,
        while the outputs can be found in ``"<instrument>/prompt-results"``.

        Parameters
        ----------
        visit : Visit
            The visit whose outputs need to be exported.
        exposure_ids : `set` [`int`]
            Identifiers of the exposures that were processed.
        """
        # TODO: this method will not be responsible for raws after DM-36051.
        self._export_subset(visit, exposure_ids, "raw",
                            in_collections=self._get_raw_run_name(visit),
                            out_collection=self.instrument.makeDefaultRawIngestRunName())

        umbrella = self.instrument.makeCollectionName("prompt-results")
        latest_run = self.butler.registry.getCollectionChain(self.output_collection)[0]
        if latest_run.startswith(self.output_collection + "/"):
            self._export_subset(visit, exposure_ids,
                                # TODO: find a way to merge datasets like *_config
                                # or *_schema that are duplicated across multiple
                                # workers.
                                self._get_safe_dataset_types(self.butler),
                                in_collections=latest_run,
                                out_collection=umbrella)
            _log.info(f"Pipeline products saved to collection '{umbrella}' for "
                      f"detector {visit.detector} of {exposure_ids}.")
        else:
            _log.warning(f"No output runs to save! Called for {visit.detector} of {exposure_ids}.")

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
        return [dstype for dstype in butler.registry.queryDatasetTypes(...)
                if "detector" in dstype.dimensions]

    def _export_subset(self, visit: Visit, exposure_ids: set[int],
                       dataset_types: typing.Any, in_collections: typing.Any, out_collection: str) -> None:
        """Copy datasets associated with a processing run back to the
        central Butler.

        Parameters
        ----------
        visit : Visit
            The visit whose outputs need to be exported.
        exposure_ids : `set` [`int`]
            Identifiers of the exposures that were processed.
        dataset_types
            The dataset type(s) to transfer; can be any expression described in
            :ref:`daf_butler_dataset_type_expressions`.
        in_collections
            The collections to transfer from; can be any expression described
            in :ref:`daf_butler_collection_expressions`.
        out_collection : `str`
            The chained collection in which to include the datasets. Need not
            exist before the call.
        """
        try:
            # Need to iterate over datasets at least twice, so list.
            datasets = list(self.butler.registry.queryDatasets(
                dataset_types,
                collections=in_collections,
                # in_collections may include other runs, so need to filter.
                # Since AP processing is strictly visit-detector, these three
                # dimensions should suffice.
                # DO NOT assume that visit == exposure!
                where=f"exposure in ({', '.join(str(x) for x in exposure_ids)})",
                instrument=self.instrument.getName(),
                detector=visit.detector,
            ))
        except lsst.daf.butler.registry.DataIdError as e:
            raise ValueError("Invalid visit or exposures.") from e
        if not datasets:
            raise ValueError(f"No datasets match visit={visit} and exposures={exposure_ids}.")

        with tempfile.NamedTemporaryFile(mode="w+b", suffix=".yaml") as export_file:
            # MUST NOT export governor dimensions, as this causes deadlocks in
            # central registry. Can omit most other dimensions (all dimensions,
            # after DM-36051) to avoid locks or redundant work.
            # TODO: saveDatasets(elements={"exposure", "visit"}) doesn't work.
            # Use import(skip_dimensions) until DM-36062 is fixed.
            with self.butler.export(filename=export_file.name) as export:
                export.saveDatasets(datasets)
            self.central_butler.import_(filename=export_file.name,
                                        directory=self.butler.datastore.root,
                                        skip_dimensions={"instrument", "detector",
                                                         "skymap", "tract", "patch"},
                                        transfer="copy")
        # No-op if collection already exists.
        self.central_butler.registry.registerCollection(out_collection, CollectionType.CHAINED)
        runs = {ref.run for ref in datasets}
        # Don't unlink any previous runs.
        # TODO: need to secure this against concurrent modification
        _prepend_collection(self.central_butler, out_collection, runs)


def _query_missing_datasets(src_repo: Butler, dest_repo: Butler,
                            *args, **kwargs) -> collections.abc.Iterable[lsst.daf.butler.DatasetRef]:
    """Return datasets that are present in one repository but not another.

    Parameters
    ----------
    src_repo : `lsst.daf.butler.Butler`
        The repository in which a dataset must be present.
    dest_repo : `lsst.daf.butler.Butler`
        The repository in which a dataset must not be present.
    *args, **kwargs
        Parameters for describing the dataset query. They have the same
        meanings as the parameters of `lsst.daf.butler.Registry.queryDatasets`.
        The query must be valid for both ``src_repo`` and ``dest_repo``.

    Returns
    -------
    datasets : iterable [`lsst.daf.butler.DatasetRef`]
        The datasets that exist in ``src_repo`` but not ``dest_repo``.
    """
    try:
        known_datasets = set(dest_repo.registry.queryDatasets(*args, **kwargs))
    except lsst.daf.butler.registry.DataIdValueError as e:
        _log.debug("Pre-export query with args '%s, %s' failed with %s",
                   ", ".join(repr(a) for a in args),
                   ", ".join(f"{k}={v!r}" for k, v in kwargs.items()),
                   e)
        # If dimensions are invalid, then *any* such datasets are missing.
        known_datasets = set()

    # Let exceptions from src_repo query raise: if it fails, that invalidates
    # this operation.
    return itertools.filterfalse(lambda ref: ref in known_datasets,
                                 src_repo.registry.queryDatasets(*args, **kwargs))


def _prepend_collection(butler: Butler, chain: str, new_collections: collections.abc.Iterable[str]) -> None:
    """Add a specific collection to the front of an existing chain.

    Parameters
    ----------
    butler : `lsst.daf.butler.Butler`
        The butler in which the collections exist.
    chain : `str`
        The chained collection to prepend to.
    new_collections : iterable [`str`]
        The collections to prepend to ``chain``.

    Notes
    -----
    This function is not safe against concurrent modifications to ``chain``.
    """
    old_chain = butler.registry.getCollectionChain(chain)  # May be empty
    butler.registry.setCollectionChain(chain, list(new_collections) + list(old_chain), flatten=False)
