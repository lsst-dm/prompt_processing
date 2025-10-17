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


"""Selectively export the contents of an ap_verify dataset.

This script selects the subset of an ap_verify dataset's preloaded repository that
matches what the central prompt processing repository ought to look like.
"""


import argparse
import logging
import os
import re
import sys
import time

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
    start = time.time_ns()
    _export_for_copy(gen3_repo)
    end = time.time_ns()
    logging.info("Export finished in %.3fs.", 1e-9 * (end - start))


def _get_dataset_types():
    """Identify the dataset types that should be marked for export.

    Returns
    -------
    types : iterable [`str` or `re.Pattern`]
        The dataset types to include
    """
    # Everything except raws and SS ephemerides
    return [re.compile("^(?!raw|visitSsObjects).*")]


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
        contents.saveDataIds(butler.query_data_ids({"detector"}, with_dimension_records=True))
        contents.saveDatasets(butler.query_all_datasets(
            name=_get_dataset_types(), collections=butler.collections.query("*"),
            find_first=False, limit=None,
        ))
        # Save calibration collection
        for collection in butler.collections.query(
                collection_types=daf_butler.CollectionType.CALIBRATION):
            contents.saveCollection(collection)
        # Do not export chains, as they will need to be reworked to satisfy
        # prompt processing's assumptions.


if __name__ == "__main__":
    main()
