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


"""Selectively export the contents of an ap_verify dataset.

This script selects the subset of an ap_verify dataset's preloaded repository that
matches what the central prompt processing repository ought to look like.
"""


import argparse
import logging
import os
import sys

import lsst.skymap
import lsst.daf.butler as daf_butler


def _make_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("--src-repo", required=True,
                        help="The location of the repository to be exported.")
    return parser


def main():
    logging.basicConfig(level=logging.INFO, stream=sys.stdout)

    args = _make_parser().parse_args()
    gen3_repo = os.path.abspath(args.src_repo)

    logging.info("Exporting Gen 3 registry to configure new repos...")
    _export_for_copy(gen3_repo)


def _export_for_copy(repo):
    """Export a Gen 3 repository so that a dataset can make copies later.

    Parameters
    ----------
    repo : `str`
        The location of the Gen 3 repository.
    """
    butler = daf_butler.Butler(repo)
    with butler.export(format="yaml") as contents:
        # Need all detectors, even those without data, for visit definition
        contents.saveDataIds(butler.registry.queryDataIds({"detector"}).expanded())
        contents.saveDatasets(butler.registry.queryDatasets(datasetType=..., collections=...))
        # Explicitly save the calibration and chained collections.
        # Do _not_ include the RUN collections here because that will export
        # an empty raws collection, which ap_verify assumes does not exist
        # before ingest.
        target_types = {daf_butler.CollectionType.CALIBRATION, daf_butler.CollectionType.CHAINED}
        for collection in butler.registry.queryCollections(..., collectionTypes=target_types):
            contents.saveCollection(collection)
        # Export skymap collection even if it is empty
        contents.saveCollection(lsst.skymap.BaseSkyMap.SKYMAP_RUN_COLLECTION_NAME)
        # Dataset export exports visits, but need matching visit definitions as
        # well (DefineVisitsTask won't add them back in).
        contents.saveDimensionData("exposure",
                                   butler.registry.queryDimensionRecords("exposure"))
        contents.saveDimensionData("visit_definition",
                                   butler.registry.queryDimensionRecords("visit_definition"))
        contents.saveDimensionData("visit_detector_region",
                                   butler.registry.queryDimensionRecords("visit_detector_region"))


if __name__ == "__main__":
    main()
