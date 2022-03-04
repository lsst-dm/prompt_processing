import logging
import os
import shutil
import subprocess

from astropy.time import Time
from lsst.daf.butler import Butler, CollectionType, Timespan

from visit import Visit

PIPELINE_MAP = dict(
    BIAS="bias.yaml",
    DARK="dark.yaml",
    FLAT="flat.yaml",
)

_log = logging.getLogger(__name__)
_log.setLevel(logging.DEBUG)

class MiddlewareInterface:
    def __init__(self, input_repo: str, image_bucket: str, instrument: str):

        # self.src = Butler(input_repo, writeable=False)
        _log.info(f"Butler({input_repo}, writeable=False)")
        self.image_bucket = image_bucket
        self.instrument = instrument

        self.repo = "/tmp/butler-{os.getpid()}"
        if not os.path.exists(self.repo):
            _log.info(f"Making local Butler {self.repo}")
            Butler.makeRepo(self.repo)
        else:
            _log.info(f"Local Butler {self.repo} exists")
        self.dest = Butler(self.repo, writeable=True)

        # self.r = self.src.registry
        self.calibration_collection = f"{instrument}/calib"
        refcat_collection = "refcats/DM-28636"
        skymap = "hsc_rings_v1"

        export_collections = set()
        export_datasets = set()
        export_collections.add(self.calibration_collection)
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
        # self.dest.import_(directory=prep_dir, format="yaml", transfer="hardlink")
        shutil.rmtree(prep_dir, ignore_errors=True)

        # self.calib_types = [
        #     dataset_type
        #     for dataset_type in self.src.registry.queryDatasetTypes(...)
        #     if dataset_type.isCalibration()
        # ]
        self.calib_types = ["bias", "dark", "defects", "flat", "fringe",]

    def filter_calibs(dataset_ref, visit_info):
        for dimension in ("instrument", "detector", "physical_filter"):
            if dimension in dataset_ref.dataId:
                if dataset_ref.dataId[dimension] != visit_info[dimension]:
                    return False
        return True

    def prep_butler(self, visit: Visit) -> None:
        _log.info(f"Preparing Butler for visit '{visit}'")
        visit_info = visit.__dict__
        export_datasets = set()
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
        # self.dest.import_(directory=visit_dir, format="yaml", transfer="hardlink")
        shutil.rmtree(visit_dir, ignore_errors=True)

    def ingest_image(self, oid: str) -> None:
        _log.info(f"Ingesting image '{oid}'")
        run = f"{self.instrument}/raw/all"
        cmd = [
            "butler",
            "ingest-raws",
            "-t",
            "copy",
            "--output_run",
            run,
            self.repo,
            f"gs://{self.image_bucket}/{oid}",
        ]
        _log.debug(str(cmd))
        # subprocess.run(cmd, check=True)

    def run_pipeline(self, visit: Visit, snaps) -> None:
        pipeline = PIPELINE_MAP[visit.kind]
        _log.info(f"Running pipeline {pipeline} on visit '{visit}', snaps {snaps}")
        cmd = [
            "echo",
            "pipetask",
            "run",
            "-b",
            self.repo,
            "-p",
            pipeline,
            "-i",
            f"{self.instrument}/raw/all",
        ]
        _log.debug(str(cmd))
        # subprocess.run(cmd, check=True)
