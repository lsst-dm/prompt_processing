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
file for importing to the test central prompt processing repository.
"""


import argparse
import logging
import sys

import lsst.daf.butler as daf_butler
from lsst.utils.timer import time_this


def _make_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--src-repo",
        required=True,
        help="The location of the repository from which datasets are exported.",
    )
    return parser


def main():
    logging.basicConfig(level=logging.INFO, stream=sys.stdout)

    args = _make_parser().parse_args()
    src_butler = daf_butler.Butler(args.src_repo)

    with time_this(msg="Datasets and collections exported", level=logging.INFO):
        _export_for_copy(src_butler)


def _export_for_copy(butler):
    """Export selected data to make copies in another butler repo.

    Parameters
    ----------
    butler : `lsst.daf.butler.Butler`
        The source Butler from which datasets are exported.
    """
    with butler.export(format="yaml") as contents:
        logging.debug("Selecting deepCoadd datasets")
        records = butler.registry.queryDatasets(
            datasetType="deepCoadd",
            collections="LATISS/runs/AUXTEL_DRP_IMAGING_2023-07AB-05AB/"
                        "w_2023_19/PREOPS-3598/20230726T202836Z",
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

        # Save selected collections and chains
        for collection in butler.registry.queryCollections(
            expression="LATISS/calib",
            flattenChains=True,
        ) + [
            "LATISS/calib",
            "LATISS/calib/DM-36719",
            "LATISS/calib/DM-38946",
            "LATISS/calib/DM-39505",
            "LATISS/templates",
        ]:
            contents.saveCollection(collection)


if __name__ == "__main__":
    main()
