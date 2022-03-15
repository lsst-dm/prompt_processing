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
import shutil

from astropy.time import Time

from lsst.daf.butler import Butler
import lsst.obs.base

from .visit import Visit

PIPELINE_MAP = dict(
    BIAS="bias.yaml",
    DARK="dark.yaml",
    FLAT="flat.yaml",
)

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
    input_repo : `str`
        Path to a butler repo containing the calibration and other data needed
        for processing images as they are received.
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
    def __init__(self, input_repo: str, image_bucket: str, instrument: str,
                 butler: Butler,
                 prefix: str = "gs://"):
        self.prefix = prefix
        # self.src = Butler(input_repo, writeable=False)
        _log.debug(f"Butler({input_repo}, writeable=False)")
        self.image_bucket = image_bucket
        self.instrument = lsst.obs.base.utils.getInstrument(instrument)

        self._init_local_butler(butler)
        self._init_ingester()

        # self.r = self.src.registry
        self.calibration_collection = f"{instrument}/calib"
        refcat_collection = "refcats/DM-28636"

        export_collections = set()
        export_collections.add(self.calibration_collection)
        _log.debug("Finding secondary collections")
        # calib_collections = list(
        #     self.r.queryCollections(
        #         self.calibration_collection,
        #         flattenChains=True,
        #         includeChains=True,
        #         collectionTypes={CollectionType.CALIBRATION, CollectionType.CHAINED},
        #     )
        # )
        # for collection in calib_collections:
        #     export_collections.add(collection)
        export_collections.add(refcat_collection)
        # NOTE: I don't think we need this at all: we can just use ingest_files
        # for the refcats. See jointcalTestBase.importRepository().

        _log.debug("Finding refcats")
        # for dataset in self.r.queryDatasets(
        #     "gaia_dr2_20200414",
        #     where=f"htm7 IN ({htm7})",
        #     collections=refcat_collection,
        # ):
        #     export_datasets.add(dataset)
        # for dataset in self.r.queryDatasets(
        #     "ps1_pv3_3pi_20170110",
        #     where=f"htm7 IN ({htm7})",
        #     collections=refcat_collection,
        # ):
        #     export_datasets.add(dataset)

        prep_dir = "/tmp/butler-export"
        os.makedirs(prep_dir)
        _log.debug(f"Exporting to {prep_dir}")
        # with self.src.export(directory=prep_dir, format="yaml", transfer="copy") as e:
        #     for collection in export_collections:
        #         e.saveCollection(collection)
        #     e.saveDatasets(export_datasets)
        _log.debug(f"Importing from {prep_dir}")
        # self.butler.import_(directory=prep_dir, format="yaml", transfer="hardlink")
        shutil.rmtree(prep_dir, ignore_errors=True)

        # self.calib_types = [
        #     dataset_type
        #     for dataset_type in self.src.registry.queryDatasetTypes(...)
        #     if dataset_type.isCalibration()
        # ]
        self.calib_types = ["bias", "dark", "defects", "flat", "fringe", ]

    def _init_local_butler(self, butler: Butler):
        """Prepare the local butler to ingest into and process from.

        Parameters
        ----------
        butler : `lsst.daf.butler.Butler`
            Local butler to process data in and hold calibrations, etc.; must
            be writeable.
        """
        # TODO: Replace registring the instrument with importing a "base" repo
        # structure from an export.yaml file.
        self.instrument.register(butler.registry)
        # Refresh butler after configuring it, to ensure all required
        # dimensions are available.
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
        visit_info = visit.__dict__
        for calib_type in self.calib_types:
            _log.debug(f"Finding {calib_type} datasets dataId={visit_info}"
                       f" collections={self.calibration_collection}"
                       f" timespan={Time.now()}")
            # dataset = self.r.findDataset(
            #     calib_type,
            #     dataId=visit_info,
            #     collections=self.calibration_collection,
            #     timespan=Timespan(Time.now(), Time.now()),
            # )
            # if dataset is not None:
            #     export_datasets.add(dataset)
        # Optimization: look for datasets in destination repo to avoid copy.

        for calib_type in self.calib_types:
            _log.debug(f"Finding {calib_type} associations")
            # for association in r.queryDatasetAssociations(
            #     calib_type,
            #     collections=self.calibration_collection,
            #     collectionTypes=[CollectionType.CALIBRATION],
            #     flattenChains=True,
            # ):
            #     if filter_calibs(association.ref, visit_info):
            #         export_collections.add(association.ref.run)

        visit_dir = f"/tmp/visit-{visit.group}-export"
        _log.debug(f"Exporting to {visit_dir}")
        os.makedirs(visit_dir)
        # with self.src.export(directory=visit_dir, format="yaml", transfer="copy") as e:
        #     for collection in export_collections:
        #         e.saveCollection(collection)
        #     e.saveDatasets(export_datasets)
        _log.debug(f"Importing from {visit_dir}")
        # self.butler.import_(directory=visit_dir, format="yaml", transfer="hardlink")
        shutil.rmtree(visit_dir, ignore_errors=True)

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
        """Process the received image.

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
        pipeline = PIPELINE_MAP[visit.kind]
        _log.info(f"Running pipeline {pipeline} on visit '{visit}', snaps {snaps}")
        cmd = [
            "echo",
            "pipetask",
            "run",
            "-b",
            self.butler,
            "-p",
            pipeline,
            "-i",
            f"{self.instrument}/raw/all",
        ]
        _log.debug("pipetask command line: %s", cmd)
        # subprocess.run(cmd, check=True)


def filter_calibs(dataset_ref, visit_info):
    for dimension in ("instrument", "detector", "physical_filter"):
        if dimension in dataset_ref.dataId:
            if dataset_ref.dataId[dimension] != visit_info[dimension]:
                return False
    return True
