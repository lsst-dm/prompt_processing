#!/usr/bin/env python
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


"""Selectively export the contents of the LATISS dataset.

This script selects some LATISS data in a source butler repo, and makes an export
file for making a test central prompt processing repository.
"""


import argparse
import logging
import sys
import time

import lsst.daf.butler as daf_butler


def _make_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--src-repo",
        required=True,
        help="The location of the repository to be exported.",
    )
    return parser


def main():
    logging.basicConfig(level=logging.INFO, stream=sys.stdout)

    args = _make_parser().parse_args()
    src_butler = daf_butler.Butler(args.src_repo)

    logging.info("Exporting Gen 3 registry to configure new repos...")
    start = time.time_ns()
    _export_for_copy(src_butler)
    end = time.time_ns()
    logging.info("Export finished in %.3fs.", 1e-9 * (end - start))


def _export_for_copy(butler):
    """Export selected data to make copies in another butler repo.

    Parameters
    ----------
    butler: `lsst.daf.butler.Butler`
        The source Butler from which datasets are exported.
    """
    with butler.export(format="yaml") as contents:
        logging.debug("Selecting deepCoadd datasets")
        records = butler.registry.queryDatasets(
            datasetType="deepCoadd",
            collections="LATISS/runs/AUXTEL_DRP_IMAGING_2023-07AB-05AB/w_2023_19/PREOPS-3598/20230726T202836Z",
        )
        contents.saveDatasets(records)


        logging.debug("Selecting refcats datasets")
        records = butler.registry.queryDatasets(datasetType=..., collections="refcats")
        contents.saveDatasets(records)

        logging.debug("Selecting skymaps dataset")
        records = butler.registry.queryDatasets(
            datasetType="skyMap", collections="skymaps", dataId={"skymap": "latiss_v1"}
        )
        contents.saveDatasets(records)

        logging.debug("Selecting datasets in LATISS/calib")
        records = butler.registry.queryDatasets(
            datasetType=..., collections="LATISS/calib"
        )
        contents.saveDatasets(records)

        # Save calibration collection
        for collection in butler.registry.queryCollections(
            expression="LATISS/calib*",
            collectionTypes=daf_butler.CollectionType.CALIBRATION,
        ):
            contents.saveCollection(collection)
        # Do not export chains, as they will need to be reworked to satisfy
        # prompt processing's assumptions.


if __name__ == "__main__":
    main()
