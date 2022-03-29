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

__all__ = ["MiddlewareInterface"]

import logging
import os
import os.path
import tempfile

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


class MiddlewareInterface:
    """Interface layer between the Butler middleware and the prompt processing
    data handling system, to handle processing individual images.

    An instance of this class will accept an incoming group of single-detector
    snaps to process, using an instance-local butler repo. The instance can
    pre-load the necessary calibrations to process an incoming detector-visit,
    ingest the data when it is available, and run the difference imaging
    pipeline, all in that local butler.

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
        Full class name of the instrument taking the data, for populating
        butler collections and dataIds. Example: "lsst.obs.lsst.LsstCam"
        TODO: this arg can probably be removed and replaced with internal
        use of the butler.
    butler : `lsst.daf.butler.Butler`
        Local butler to process data in and hold calibrations, etc.; must be
        writeable.
    prefix : `str`, optional
        URI identification prefix; prepended to ``image_bucket`` when
        constructing URIs to retrieve incoming files. The default is
        appropriate for use in the Google Cloud environment; typically only
        change this when running local tests.
    """

    def __init__(self, central_butler: Butler, image_bucket: str, instrument: str,
                 butler: Butler,
                 prefix: str = "gs://"):
        self.ip_apdb = os.environ["IP_APDB"]
        self.prefix = prefix
        self.central_butler = central_butler
        self.image_bucket = image_bucket
        self.instrument = lsst.obs.base.utils.getInstrument(instrument)

        self.output_collection = f"{self.instrument.getName()}/prompt"

        self._init_local_butler(butler)
        self._init_ingester()
        # TODO DM-34098: note that we currently need to supply instrument here.
        # HACK: explicit collection gets around the fact that we don't have any
        # timestamp/exposure information in a form we can pass to the Butler.
        # This code will break once cameras start being versioned.
        self.camera = self.central_butler.get(
            "camera", instrument=self.instrument.getName(),
            collections=self.instrument.makeCalibrationCollectionName("unbounded")
        )
        self.skymap = self.central_butler.get("skyMap")

        # How much to pad the refcat region we will copy over.
        self.padding = 30*lsst.geom.arcseconds

    def _init_local_butler(self, butler: Butler):
        """Prepare the local butler to ingest into and process from.

        Parameters
        ----------
        butler : `lsst.daf.butler.Butler`
            Local butler to process data in and hold calibrations, etc.; must
            be writeable.
        """
        self.instrument.register(butler.registry)

        # Refresh butler after configuring it, to ensure all required
        # dimensions and collections are available.
        butler.registry.refresh()
        self.butler = butler

    def _init_ingester(self):
        """Prepare the raw file ingester to receive images into this butler.
        """
        config = lsst.obs.base.RawIngestConfig()
        config.transfer = "copy"  # Copy files into the local butler.
        # TODO: Could we use the `on_ingest_failure` and `on_success` callbacks
        # to send information back to this interface?
        config.failFast = True  # We want failed ingests to fail immediately.
        self.rawIngestTask = lsst.obs.base.RawIngestTask(config=config,
                                                         butler=self.butler)

    def prep_butler(self, visit: Visit) -> None:
        """Prepare a temporary butler repo for processing the incoming data.

        Parameters
        ----------
        visit : Visit
            Group of snaps from one detector to prepare the butler for.
        """
        _log.info(f"Preparing Butler for visit '{visit}'")

        with tempfile.NamedTemporaryFile(mode="w+b", suffix=".yaml") as export_file:
            with self.central_butler.export(filename=export_file.name, format="yaml") as export:
                boresight_center = lsst.geom.SpherePoint(visit.ra, visit.dec, lsst.geom.degrees)
                orientation = lsst.geom.Angle(visit.rot, lsst.geom.degrees)
                detector = self.camera[visit.detector]
                flip_x = True if self.instrument.getName() == "DECam" else False
                wcs = lsst.obs.base.createInitialSkyWcsFromBoresight(boresight_center,
                                                                     orientation,
                                                                     detector,
                                                                     flipX=flip_x)
                # Compute the maximum sky circle that contains the detector.
                radii = []
                center = wcs.pixelToSky(detector.getCenter(lsst.afw.cameraGeom.PIXELS))
                for corner in detector.getCorners(lsst.afw.cameraGeom.PIXELS):
                    radii.append(wcs.pixelToSky(corner).separation(center))
                radius = max(radii)

                self._export_refcats(export, center, radius)
                self._export_skymap_and_templates(export, center, detector, wcs)
                self._export_calibs(export, visit.detector, visit.filter)

                # CHAINED collections
                export.saveCollection("refcats")
                export.saveCollection("templates")
                export.saveCollection(self.instrument.makeCollectionName("defaults"))

            self.butler.import_(filename=export_file.name,
                                directory=self.central_butler.datastore.root,
                                transfer="copy")

        self._prep_collections()
        self._prep_pipeline(visit)

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
        shards = indexer.getShardIds(center, radius+self.padding)
        # getShardIds returns a tuple, the first item is the ids list.
        htm_where = f"htm7 in ({','.join(str(x) for x in shards[0])})"
        # Get shards from all refcats that overlap this detector.
        # TODO: `...` doesn't work for this queryDatasets call
        # currently, and we can't queryDatasetTypes in just the refcats
        # collection, so we have to specify a list here. Replace this
        # with another solution ASAP.
        possible_refcats = ["gaia", "panstarrs", "gaia_dr2_20200414", "ps1_pv3_3pi_20170110"]
        export.saveDatasets(self.central_butler.registry.queryDatasets(possible_refcats,
                                                                       collections="refcats",
                                                                       where=htm_where,
                                                                       findFirst=True))

    def _export_skymap_and_templates(self, export, center, detector, wcs):
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
        """
        # TODO: This exports the whole skymap, but we want to only export the
        # subset of the skymap that covers this data.
        # TODO: We only want to import the skymap dimension once in init,
        # otherwise we get a UNIQUE constraint error when prepping for the
        # second visit.
        export.saveDatasets(self.central_butler.registry.queryDatasets("skyMap",
                                                                       collections="skymaps",
                                                                       findFirst=True))
        # Getting only one tract should be safe: we're getting the
        # tract closest to this detector, so we should be well within
        # the tract bbox.
        tract = self.skymap.findTract(center)
        points = [center]
        for corner in detector.getCorners(lsst.afw.cameraGeom.PIXELS):
            points.append(wcs.pixelToSky(corner))
        patches = tract.findPatchList(points)
        patches_str = ','.join(str(p.sequential_index) for p in patches)
        template_where = f"patch in ({patches_str}) and tract={tract.tract_id}"
        # TODO: do we need to have the coadd name used in the pipeline
        # specified as a class kwarg, so that we only load one here?
        # TODO: alternately, we need to extract it from the pipeline? (best?)
        # TODO: alternately, can we just assume that there is exactly
        # one coadd type in the central butler?
        export.saveDatasets(self.central_butler.registry.queryDatasets("*Coadd",
                                                                       collections="templates",
                                                                       where=template_where))

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
        export.saveDatasets(
            self.central_butler.registry.queryDatasets(
                ...,
                collections=self.instrument.makeCalibrationCollectionName(),
                where=calib_where),
            elements=[])  # elements=[] means do not export dimension records
        target_types = {CollectionType.CALIBRATION}
        for collection in self.central_butler.registry.queryCollections(...,
                                                                        collectionTypes=target_types):
            export.saveCollection(collection)

    def _prep_collections(self):
        """Pre-register output collections in advance of running the pipeline.
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
        # TODO DM-34202: save run collection in self for now, but we won't need
        # it when we no longer need to work around DM-34202.
        self.output_run = f"{self.output_collection}/{self.instrument.makeCollectionTimestamp()}"
        self.butler.registry.registerCollection(self.instrument.makeDefaultRawIngestRunName(),
                                                CollectionType.RUN)
        self.butler.registry.registerCollection(self.output_run, CollectionType.RUN)
        self.butler.registry.registerCollection(self.output_collection, CollectionType.CHAINED)
        collections = [self.instrument.makeCollectionName("defaults"),
                       self.instrument.makeDefaultRawIngestRunName(),
                       self.output_run]
        self.butler.registry.setCollectionChain(self.output_collection, collections)

        # Need to create a new butler with all the output collections.
        self.butler = Butler(butler=self.butler,
                             collections=[self.output_collection],
                             # TODO DM-34202: hack around a middleware bug.
                             run=None)

    def _prep_pipeline(self, visit: Visit) -> None:
        """Setup the pipeline to be run, based on the configured instrument and
        details of the incoming visit.

        Parameters
        ----------
        visit : Visit
            Group of snaps from one detector to prepare the pipeline for.

        Raises
        ------
        RuntimeError
            Raised if there is no AP pipeline file for this configuration.
            TODO: could be a good case for a custom exception here.
        """
        # TODO: we probably want to be able to configure this per-instrument?
        # TODO: DM-33453 will remap ap_pipe/pipelines to use getName convention
        camera_paths = {"DECam": "DarkEnergyCamera", "HSC": "HyperSuprimeCam"}
        try:
            camera_path = camera_paths[self.instrument.getName()]
        except KeyError:
            raise RuntimeError(f"No ApPipe.yaml defined for camera {self.instrument.getName()}")
        # TODO: DECam test data uses `panstarrs/gaia` for the refcat names.
        yaml_file = "ApPipe.yaml" if self.instrument.getName() != "DECam" else "ApPipe_hits2015-3.yaml"
        ap_pipeline_file = os.path.join(lsst.utils.getPackageDir("ap_pipe"),
                                        "pipelines", camera_path, yaml_file)
        self.pipeline = lsst.pipe.base.Pipeline.fromFile(ap_pipeline_file)
        # TODO: Can we write to a configurable apdb schema, rather than
        # "postgres"?
        self.pipeline.addConfigOverride("diaPipe", "apdb.db_url",
                                        f"postgresql://postgres@{self.ip_apdb}/postgres")

    def ingest_image(self, oid: str) -> None:
        """Ingest an image into the temporary butler.

        Parameters
        ----------
        oid : `str`
            Google storage identifier for incoming image, relative to the
            image bucket.
        """
        _log.info(f"Ingesting image id '{oid}'")
        file = f"{self.prefix}{self.image_bucket}/{oid}"
        result = self.rawIngestTask.run([file])
        # We only ingest one image at a time.
        # TODO: replace this assert with a custom exception, once we've decided
        # how we plan to handle exceptions in this code.
        assert len(result) == 1, "Should have ingested exactly one image."
        _log.info("Ingested one %s with dataId=%s", result[0].datasetType.name, result[0].dataId)

    def run_pipeline(self, visit: Visit, snaps: set) -> None:
        """Process the received image(s).

        Parameters
        ----------
        visit : Visit
            Group of snaps from one detector to be processed.
        snaps : `set`
            Identifiers of the snaps that were received.
            TODO: I believe this is unnecessary because it should be encoded
            in the `visit` object, but we'll have to test how that works once
            we implemented this with actual data.
        """
        # TODO: can we move this from_pipeline call to prep_butler?
        where = f"detector={visit.detector} and exposure in ({','.join(str(x) for x in snaps)})"
        executor = SimplePipelineExecutor.from_pipeline(self.pipeline, where=where, butler=self.butler)
        # TODO DM-34202: hack around a middleware bug.
        executor.butler = Butler(butler=self.butler,
                                 collections=[self.output_collection],
                                 run=self.output_run)
        _log.info(f"Running '{self.pipeline._pipelineIR.description}' on {where}")
        # If this is a fresh (local) repo, then types like calexp,
        # *Diff_diaSrcTable, etc. have not been registered.
        result = executor.run(register_dataset_types=True)
        _log.info(f"Pipeline produced {len(result)} output datasets.")


def filter_calibs(dataset_ref, visit_info):
    for dimension in ("instrument", "detector", "physical_filter"):
        if dimension in dataset_ref.dataId:
            if dataset_ref.dataId[dimension] != visit_info[dimension]:
                return False
    return True
