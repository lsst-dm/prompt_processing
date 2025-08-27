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


"""Make a new template_coadd TAGGED collection with curated template_coadds.

This script filters and copies a curated set of template_coadd datasets to a new
location at a butler repository, and tags them to a new collection.

The input collections are ideally collections with only curated templates.

The scripts leaves an ecsv file used by butler ingest-files.
"""

import argparse
import logging
import os
import sys

from astropy.io import ascii
from astropy.table import Table

from lsst.daf.butler import Butler, CollectionType
from lsst.daf.butler.script import ingest_files


def _make_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "src_repo",
        help="An existing data repository containing the input collections.",
    )
    parser.add_argument(
        "--dest_repo",
        default="embargo",
        help="The repository to which to copy the datasets."
        " Default is embargo.",
    )
    parser.add_argument(
        "collections",
        help="The input collections containing curated template_coadd datasets.",
    )
    parser.add_argument(
        "existing_collection",
        help="The existing template collection.",
    )
    parser.add_argument(
        "--dupe_handler",
        default="drop",
        help="How to handle duplicates in the new and existing templates."
        " Drop will drop the new template and keep the existing template,"
        " and replace will replace the existing template with the new template.",
    )
    parser.add_argument(
        "--where",
        default="",
        help="A string expression to select datasets in the input collections.",
    )
    parser.add_argument(
        "tag",
        help="The Jira ticket number associated with the vetted template collection."
        " Will be used to construct the new collection version name.",
    )
    parser.add_argument(
        "version",
        help="A version tag following conventions YYYY-MM-DD.",
    )
    parser.add_argument(
        "--reject",
        required=False,
        help="An input table file listing data IDs to exclude from the final dataset."
        " The table must include 'tract', 'patch', and 'band' columns.",
    )
    parser.add_argument(
        "--records_path",
        required=False,
        default="",
        help="A filepath to save records to.",
    )
    parser.add_argument(
        "--records",
        required=False,
        help="An output table file with records of selected template files."
        " The file can be used by butler ingest-files."
        " Default is {version}_records.ecsv.",
    )
    return parser


def combine_collections(butler, old_collection, new_refs, dupe_handler):
    # Query old refs
    old_refs = list(
        butler.registry.queryDatasets(
            datasetType="template_coadd",
            collections=old_collection,
            findFirst=True
        )
    )

    # Deduplicate
    ref_map = {}
    for ref in old_refs:
        keys = sorted(ref.datasetType.dimensions.names)
        key = (ref.datasetType.name, tuple(ref.dataId[k] for k in keys))
        ref_map[key] = ref

    for ref in new_refs:
        keys = sorted(ref.datasetType.dimensions.names)
        key = (ref.datasetType.name, tuple(ref.dataId[k] for k in keys))
        if dupe_handler == "replace" or key not in ref_map:
            ref_map[key] = ref

    # Combine
    combined_refs = list(ref_map.values())

    return combined_refs


def main():
    logging.basicConfig(level=logging.INFO, stream=sys.stdout)

    args = _make_parser().parse_args()
    butler = Butler(args.src_repo)
    butler_write = Butler(args.dest_repo, writeable=True)

    # Create directory for records
    directory = args.records_path
    if directory:
        if not os.path.exists(directory):
            os.makedirs(directory)

    # Set records default based on version if not provided
    if args.records is None:
        args.records = f"{args.version}_records.csv"

    # Create tagged collection, abort if it already exists.
    tagged_collection = f"LSSTCam/templates/{args.tag}/{args.version}"
    registered = butler_write.collections.register(
        tagged_collection, type=CollectionType.TAGGED
    )
    logging.error(f"Collection {tagged_collection} registered.")
    if not registered:
        logging.error(f"Collection {tagged_collection} already exists. Aborting.")
        sys.exit(1)

    refs = butler.query_datasets(
        "template_coadd", collections=args.collections, where=args.where, limit=None
    )
    logging.info(f"Found {len(refs)} template_coadd datasets in {args.collections}.")

    # Filter out rejected templates from curated collection.
    logging.info("Filter out template_coadds that didn't pass vetting.")
    reject_table = ascii.read(args.reject)
    rejected_keys = {
        (row["tract"], row["patch"], row["band"]) for row in reject_table
    }
    filtered_refs = [
        ref for ref in refs
        if (ref.dataId["tract"], ref.dataId["patch"], ref.dataId["band"]) not in rejected_keys
    ]
    logging.info(f"Filtered {len(refs)-len(filtered_refs)} template_coadds.")

    # Generate record of vetted curated templates.
    columns = ("filename", "band", "skymap", "tract", "patch")
    data = Table(names=columns, dtype=("str", "str", "str", "int", "int"))
    for ref in filtered_refs:
        uri_path = butler.getURI(ref).geturl()
        data_id_values = tuple(ref.dataId[col] for col in columns[1:])
        data.add_row((uri_path, *data_id_values))
    data.write(args.records, overwrite=True)
    logging.info(f"Intermediate data records written to {args.records}.")

    # Copy uris to new location.
    logging.info("Begin copying template_coadds to new location.")
    run_collection = tagged_collection + "/run"
    ingest_files(
        args.dest_repo,
        "template_coadd",
        run_collection,
        table_file=args.records,
        data_id=("instrument=LSSTCam",),
        transfer="copy",
    )
    logging.info("Copying complete.")

    # Combine with existing refs
    logging.info("Begin combining new template_coadds with existing template_coadds.")
    combined_refs = combine_collections(butler, args.existing_collection, filtered_refs, args.dupe_handler)
    logging.info("Combining complete.")

    logging.info(f"Associating {len(combined_refs)} datasets ({len(filtered_refs)} new) "
                 f"to {tagged_collection}.")
    butler_write.registry.associate(tagged_collection, combined_refs)
    logging.info("Association complete.")


if __name__ == "__main__":
    main()
