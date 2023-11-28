#!/usr/bin/env python
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


"""Selectively export the contents of the LATISS dataset.

This script selects some LATISS data in a source butler repo, and makes an export
file for importing to the test central prompt processing repository.
"""


import argparse
import logging
import sys
import tempfile

import lsst.daf.butler as daf_butler
from lsst.utils.timer import time_this

from activator.middleware_interface import _filter_datasets


def _make_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--src-repo",
        required=True,
        help="The location of the repository from which datasets are exported.",
    )
    parser.add_argument(
        "--target-repo",
        required=False,
        help="The location of the repository to which datasets are exported. "
             "Datasets already existing in the target repo will not be "
             "exported from the source repo. If no target repo is given, all "
             "selected datasets in the source repo will be exported.",
    )
    return parser


def main():
    logging.basicConfig(level=logging.INFO, stream=sys.stdout)

    args = _make_parser().parse_args()
    src_butler = daf_butler.Butler(args.src_repo)

    with tempfile.TemporaryDirectory() as temp_repo:
        if args.target_repo:
            target_butler = daf_butler.Butler(args.target_repo, writeable=False)
        else:
            # If no target_butler is given, create an empty one.
            config = daf_butler.Butler.makeRepo(temp_repo)
            target_butler = daf_butler.Butler(config)

        with time_this(msg="Datasets and collections exported", level=logging.INFO):
            _export_for_copy(src_butler, target_butler)


def _export_for_copy(butler, target_butler):
    """Export selected data to make copies in another butler repo.

    Parameters
    ----------
    butler : `lsst.daf.butler.Butler`
        The source Butler from which datasets are exported.
    target_butler : `lsst.daf.butler.Butler`
        The target Butler to which datasets are exported. It is checked
        to avoid exporting existing datasets. No checks are done to
        verify if datasets are really identical.
    """
    with butler.export(format="yaml") as contents:
        logging.debug("Selecting goodSeeingCoadd datasets")
        records = _filter_datasets(
            butler,
            target_butler,
            datasetType="goodSeeingCoadd",
            collections="LATISS/templates",
        )
        contents.saveDatasets(records)

        refcats = {"atlas_refcat2_20220201", "gaia_dr3_20230707"}
        logging.debug(f"Selecting refcats datasets {refcats}")
        records = _filter_datasets(
            butler, target_butler, datasetType=refcats, collections="refcats*"
        )
        contents.saveDatasets(records)

        logging.debug("Selecting skymaps dataset")
        records = _filter_datasets(
            butler, target_butler, datasetType="skyMap", collections="skymaps"
        )
        contents.saveDatasets(records)

        logging.debug("Selecting datasets in LATISS/calib")
        records = _filter_datasets(
            butler,
            target_butler,
            datasetType=...,
            # Workaround: use a matching expression rather than a specific
            # string "LATISS/calib" for the collection argument, so to avoid
            # MissingCollectionError when the collection does not exist in
            # the target repo.
            collections="*LATISS/calib",
        )
        contents.saveDatasets(records)

        # Save selected collections and chains
        for collection in butler.registry.queryCollections(
            expression="LATISS/calib",
            flattenChains=True,
            includeChains=True,
        ) + [
            "LATISS/templates",
            "LATISS/calib/unbounded",
        ]:
            logging.debug(f"Selecting collection {collection}")
            try:
                target_butler.registry.queryCollections(collection)
            except daf_butler.registry.MissingCollectionError:
                # MissingCollectionError is raised if the collection does not exist in target_butler.
                contents.saveCollection(collection)


if __name__ == "__main__":
    main()
