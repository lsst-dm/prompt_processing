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

import argparse
import logging
import os
import sys

import lsst.log
import lsst.skymap
import lsst.daf.butler as daf_butler
import lsst.ap.verify as ap_verify


def _make_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", required=True,
                        help="The name of the dataset as recognized by ap_verify.py.")
    return parser


def main():
    # Ensure logs from tasks are visible
    logging.basicConfig(level=logging.INFO, stream=sys.stdout)
    lsst.log.configure_pylog_MDC("DEBUG", MDC_class=None)

    args = _make_parser().parse_args()
    dataset = ap_verify.dataset.Dataset(args.dataset)
    gen3_repo = os.path.join(dataset.datasetRoot, "preloaded")

    logging.info("Exporting Gen 3 registry to configure new repos...")
    _export_for_copy(dataset, gen3_repo)


def _export_for_copy(dataset, repo):
    """Export a Gen 3 repository so that a dataset can make copies later.

    Parameters
    ----------
    dataset : `lsst.ap.verify.dataset.Dataset`
        The dataset needing the ability to copy the repository.
    repo : `str`
        The location of the Gen 3 repository.
    """
    butler = daf_butler.Butler(repo)
    with butler.export(directory=dataset.configLocation, format="yaml") as contents:
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
