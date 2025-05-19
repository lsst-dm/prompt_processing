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


"""Curate template_coadd datasets for LSSTCam with a new TAGGED collection.

This script copies a selected set of template_coadd datasets to a new location
at a butler repository, and tag them to a new collection.

The input collections are ideally collections with only vetted templates.

The scripts leaves a ecsv file used by butler ingest-files.

This script provides a short-term workaround to store duplicated templates
with a special prefix, before DM-50699 provides a better mechanism.
"""

import argparse
import logging
import sys

from astropy.table import Table

from lsst.daf.butler import Butler, CollectionType
from lsst.daf.butler.script import ingest_files


def _make_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "repo",
        help="An existing data repository.",
    )
    parser.add_argument(
        "collections",
        help="The input collections to search for template_coadd datasets.",
    )
    parser.add_argument(
        "--where",
        default="",
        help="A string expression to select datasets in the input collections.",
    )
    parser.add_argument(
        "tag",
        help="A Jira ticket number for the new template collection name.",
    )
    parser.add_argument(
        "--records",
        required=False,
        default="records.ecsv",
        help="An output table file with records of selected template files."
        " The file can be used by butler ingest-files.",
    )
    return parser


def main():
    logging.basicConfig(level=logging.INFO, stream=sys.stdout)

    args = _make_parser().parse_args()
    butler = Butler(args.repo, writeable=True)

    tagged_collection = "LSSTCam/templates/" + args.tag
    registered = butler.collections.register(
        tagged_collection, type=CollectionType.TAGGED
    )
    if not registered:
        logging.error(f"Collection {tagged_collection} already exists. Aborting.")
        sys.exit(1)

    refs = butler.query_datasets(
        "template_coadd", collections=args.collections, where=args.where, limit=None
    )
    logging.info(f"Found {len(refs)} template_coadd datasets in {args.collections}.")

    columns = ("filename", "band", "skymap", "tract", "patch")
    data = Table(names=columns, dtype=("str", "str", "str", "int", "int"))
    for ref in refs:
        uri_path = butler.getURI(ref).geturl()
        data_id_values = tuple(ref.dataId[col] for col in columns[1:])
        data.add_row((uri_path, *data_id_values))
    data.write(args.records, overwrite=True)
    logging.info(f"Data records written to {args.records}.")

    run_collection = tagged_collection + "/run"
    ingest_files(
        args.repo,
        "template_coadd",
        run_collection,
        table_file=args.records,
        data_id=("instrument=LSSTCam",),
        transfer="copy",
    )

    refs = butler.query_datasets(
        "template_coadd", collections=run_collection, limit=None
    )
    logging.info(f"Associating {len(refs)} datasets to {tagged_collection}.")
    butler.registry.associate(tagged_collection, refs)


if __name__ == "__main__":
    main()
