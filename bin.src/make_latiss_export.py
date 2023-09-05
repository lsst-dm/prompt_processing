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
    parser.add_argument(
        "--target-repo", required=False, help="The URI of the repository to create."
    )
    return parser


def main():
    logging.basicConfig(level=logging.INFO, stream=sys.stdout)

    args = _make_parser().parse_args()
    src_butler = daf_butler.Butler(args.src_repo)

    logging.info("Exporting Gen 3 registry to configure new repos...")
    start = time.time_ns()
    if args.target_repo:
        target_butler = daf_butler.Butler(args.target_repo)
        _export_for_copy(src_butler, target_butler)
    else:
        _export_for_copy(src_butler)
    end = time.time_ns()
    logging.info("Export finished in %.3fs.", 1e-9 * (end - start))


def _export_for_copy(butler, target_butler=None):
    """Export selected data to make copies in another butler repo.

    Parameters
    ----------
    butler: `lsst.daf.butler.Butler`
        The source Butler from which datasets are exported.
    target_butler: `lsst.daf.butler.Butler`
        The target Butler to which datasets are exported. If given,
        it is checked to avoid exporting existing datasets, but no
        checking is done to verify if datasets are the same.
    """
    with butler.export(format="yaml") as contents:
        logging.debug("Selecting deepCoadd datasets")
        records = butler.registry.queryDatasets(
            datasetType="deepCoadd",
            collections="LATISS/runs/AUXTEL_DRP_IMAGING_2023-07AB-05AB/w_2023_19/PREOPS-3598/20230726T202836Z",
        )
        for record in records:
            if not target_butler or not target_butler.exists(record):
                contents.saveDatasets({record})

        logging.debug("Selecting refcats datasets")
        records = butler.registry.queryDatasets(datasetType=..., collections="refcats")
        for record in records:
            if not target_butler or not target_butler.exists(record):
                contents.saveDatasets({record})

        logging.debug("Selecting skymaps dataset")
        records = butler.registry.queryDatasets(
            datasetType="skyMap", collections="skymaps", dataId={"skymap": "latiss_v1"}
        )
        for record in records:
            if not target_butler or not target_butler.exists(record):
                contents.saveDatasets({record})

        logging.debug("Selecting datasets in LATISS/calib")
        records = butler.registry.queryDatasets(
            datasetType=..., collections="LATISS/calib"
        )
        for record in records:
            if not target_butler or not target_butler.exists(record):
                contents.saveDatasets({record})

        # Save calibration collection
        for collection in butler.registry.queryCollections(
            expression="LATISS/calib*",
            collectionTypes=daf_butler.CollectionType.CALIBRATION,
        ):
            if not target_butler:
                contents.saveCollection(collection)
                return
            try:
                target_butler.registry.queryCollections(collection)
            except daf_butler.registry.MissingCollectionError:
                contents.saveCollection(collection)
        # Do not export chains, as they will need to be reworked to satisfy
        # prompt processing's assumptions.


if __name__ == "__main__":
    main()
