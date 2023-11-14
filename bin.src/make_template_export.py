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


"""Export goodSeeingCoadd from a collection and make an export file
for importing those data to a central prompt processing repository.
All of the goodSeeingCoadd datasets in this collection are exported
without selection.
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
        "--collection",
        required=True,
        help="The collection to query data to be exported.",
    )
    return parser


def main():
    logging.basicConfig(level=logging.INFO, stream=sys.stdout)

    args = _make_parser().parse_args()
    butler = daf_butler.Butler(args.src_repo, collections=args.collection)

    logging.info("Exporting from %s", butler)
    start = time.time_ns()
    _export_for_copy(butler)
    end = time.time_ns()
    logging.info("Export finished in %.3fs.", 1e-9 * (end - start))


def _export_for_copy(butler):
    """Export selected data to make copies in another butler repo.

    Parameters
    ----------
    butler: `lsst.daf.butler.Butler`
        The source Butler from which datasets are exported
    """
    with butler.export(format="yaml") as contents:
        logging.debug("Selecting goodSeeingCoadd datasets")
        records = butler.registry.queryDatasets(
            datasetType="goodSeeingCoadd",
        )
        contents.saveDatasets(records)


if __name__ == "__main__":
    main()
