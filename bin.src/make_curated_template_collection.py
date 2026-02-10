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


"""Make a new template_coadd TAGGED and RUN collections with curated template_coadds.

This script filters and copies a curated set of template_coadd datasets to a new
location in a butler repository, and tags them to a new collection.

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
        "tag",
        help="The Jira ticket number associated with the vetted template collection."
        " Will be used to construct the new collection name.",
    )
    parser.add_argument(
        "release_num",
        help="The release number (##) for the given tag.",
    )
    parser.add_argument(
        "--dest_repo",
        default="embargo",
        help="The repository to which to copy the datasets."
        " Default is embargo.",
    )
    parser.add_argument(
        "--collections",
        action="extend",
        nargs="+",
        required=True,
        help="The input collections containing curated template_coadd datasets.",
    )
    parser.add_argument(
        "--existing_collection",
        action="extend",
        nargs="+",
        help="The existing template collection. Optional.",
    )
    parser.add_argument(
        "--dupe_handler",
        choices=("drop", "replace"),
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
        "--reject",
        required=False,
        default="",
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
        " Default is {tag}_{release_num}_records.ecsv.",
    )
    return parser


def make_key(ref):
    keys = sorted(ref.datasetType.dimensions.names)
    return (ref.datasetType.name, tuple(ref.dataId[k] for k in keys))


def handle_duplicates(butler, old_collections, new_refs, dupe_handler):
    """Resolve duplicate template_coadd refs between old and new collections.

    Parameters
    ----------
    butler : Butler
        Butler instance pointing at the source repo.
    old_collections : str or list
        Collections to query for old template_coadds.
    new_refs : list of DatasetRef
        Candidate new refs to deduplicate.
    dupe_handler : {"drop", "replace"}
        Strategy for resolving duplicates:
        - "drop": keep old, drop new.
        - "replace": keep new, drop old.

    Returns
    -------
    new_ref_list : list of DatasetRef
        Deduplicated new refs.
    old_ref_list : list of DatasetRef
        Deduplicated old refs.
    """

    # Query old refs
    old_refs = butler.query_datasets("template_coadd", collections=old_collections, find_first=True)

    new_ref_map = {}
    old_ref_map = {}

    if dupe_handler.lower() == "drop":
        for ref in new_refs:
            new_ref_map[make_key(ref)] = ref
        for ref in old_refs:
            key = make_key(ref)
            old_ref_map[key] = ref
            new_ref_map.pop(key, None)  # remove duped new ref
        logging.info(f"Dropped {len(new_refs)-len(new_ref_map)} duplicates of old coadds.")

    elif dupe_handler.lower() == "replace":
        for ref in old_refs:
            old_ref_map[make_key(ref)] = ref
        for ref in new_refs:
            key = make_key(ref)
            new_ref_map[key] = ref
            old_ref_map.pop(key, None)  # remove duped old ref
        logging.info(f"Replaced {len(old_refs)-len(old_ref_map)} duplicates with new coadds.")

    else:
        raise ValueError(f"Unknown dupe_handler: {dupe_handler!r}. Options are 'drop' and 'replace'.")

    return list(new_ref_map.values()), list(old_ref_map.values())


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

    # Set records default if not provided
    if args.records is None:
        args.records = f"{args.tag}_{args.release_num}_records.ecsv"

    # Create tagged collection, abort if it already exists.
    tagged_collection = f"LSSTCam/templates/{args.tag}/release_{args.release_num}"
    registered = butler_write.collections.register(
        tagged_collection, type=CollectionType.TAGGED
    )
    if not registered:
        logging.error(f"Collection {tagged_collection} already exists. Aborting.")
        sys.exit(1)
    else:
        logging.info(f"Collection {tagged_collection} registered.")

    refs = butler.query_datasets(
        "template_coadd", collections=args.collections, where=args.where, limit=None
    )
    if not refs:
        logging.error("No template_coadd datasets found in the given collections.")
        sys.exit(1)
    logging.info(f"Found {len(refs)} template_coadd datasets in {args.collections}.")

    if args.reject != "":
        # Filter out rejected templates from curated collection.
        reject_table = ascii.read(args.reject)
        rejected_keys = {
            (row["tract"], row["patch"], row["band"]) for row in reject_table
        }
        filtered_refs = [
            ref for ref in refs
            if (ref.dataId["tract"], ref.dataId["patch"], ref.dataId["band"]) not in rejected_keys
        ]
        logging.info(f"Filtered out {len(refs)-len(filtered_refs)} template_coadds "
                     "that didn't pass vetting.")
    else:
        filtered_refs = refs

    # Handle duplicates
    if args.existing_collection:
        filtered_refs, old_refs = handle_duplicates(
            butler, args.existing_collection, filtered_refs, args.dupe_handler
        )
    else:
        logging.info("No existing_collection specified; skipping duplicate handling.")
        old_refs = []

    if len(filtered_refs) == 0:
        logging.info("No template_coadds remain after filtering.")
    else:
        # Generate record of vetted curated templates.
        logging.info("Begin generating record of vetted curated templates.")
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

        # Combine with existing refs and associate to new collection
        logging.info("Begin combining new template_coadds with existing template_coadds.")
        new_refs = butler_write.query_datasets("template_coadd", collections=run_collection, limit=None)
        combined_refs = new_refs + old_refs
        logging.info("Combining complete.")

        logging.info(f"Associating {len(combined_refs)} datasets ({len(new_refs)} new) "
                     f"to {tagged_collection}.")
        butler_write.registry.associate(tagged_collection, combined_refs)
        logging.info("Association complete.")


if __name__ == "__main__":
    main()
