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


"""Link and ingest template_coadd datasets to a destination repo.

The script queries template_coadd datasets in the main data repo at
/sdf/data/rubin/repo/main_20210215/LSSTCam/templates, creates hard links into
/sdf/data/rubin/shared/templates, and ingests them into the destination repo.
LSSTCam data are assumed.
"""

import argparse
import logging
import os
import sys

from astropy.table import Table

from lsst.daf.butler import Butler
from lsst.daf.butler.script import ingest_files


def _make_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "repo_dest",
        help="Destination data repository.",
    )
    parser.add_argument(
        "collection",
        help="The input RUN collection to search for template_coadd datasets."
        " Also used as the destination run collection for ingestion.",
    )
    parser.add_argument(
        "--where",
        default="",
        help="A string expression to select datasets in the input collections.",
    )
    parser.add_argument(
        "--records",
        required=False,
        default="records.ecsv",
        help="An output table file with records of selected template files."
        " The file can be used by butler ingest-files.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Log what would be done without creating hard links or ingesting."
        " repo_dest is not used in this mode.",
    )
    return parser


def are_hard_links(file1, file2):
    try:
        stat_file1 = os.stat(file1)
        stat_file2 = os.stat(file2)
        return stat_file1.st_ino == stat_file2.st_ino
    except FileNotFoundError:
        return False


def main():
    logging.basicConfig(level=logging.INFO, stream=sys.stdout)

    args = _make_parser().parse_args()
    if args.dry_run:
        logging.info(
            "Dry-run mode: no hard links will be created and no files will be ingested."
        )
    # Hard-coded /sdf/data/rubin/repo/main_20210215 for the path replacement below.
    butler_src = Butler("main")

    refs = butler_src.query_datasets(
        "template_coadd", collections=args.collection, where=args.where, limit=None
    )
    logging.info(
        f"Found {len(refs)} template_coadd datasets in main's {args.collection}."
    )

    columns = ("filename", "band", "skymap", "tract", "patch")
    data = Table(names=columns, dtype=("str", "str", "str", "int", "int"))
    n_skipped = 0
    for ref in refs:
        url = butler_src.getURI(ref).geturl()
        # Remove any usage of /sdf/group, just use /sdf/data
        real_path = os.path.realpath(url.removeprefix("file://"))
        # Map /sdf/data/rubin/repo/main_20210215/LSSTCam/templates/ -> /sdf/data/rubin/shared/templates/
        new_path = real_path.replace("repo/main_20210215/LSSTCam/", "shared/", 1)
        if new_path == real_path:
            logging.error(
                f"Path did not match expected pattern for hard-linking: {real_path}"
            )
            n_skipped += 1
            continue
        real_new_path = os.path.realpath(new_path)
        if not are_hard_links(real_path, real_new_path):
            logging.info(f"Making a hard link: {real_path} --> {real_new_path}")
            if not args.dry_run:
                os.makedirs(os.path.dirname(real_new_path), exist_ok=True)
                try:
                    os.link(real_path, real_new_path)
                except FileExistsError:
                    logging.error(
                        f"File exists but is not a hard link to source, skipping: {real_new_path}"
                    )
                    n_skipped += 1
                    continue
        data_id_values = tuple(ref.dataId[col] for col in columns[1:])
        # Use new_path so butler stores the stable shared/ path.
        data.add_row((new_path, *data_id_values))
    if n_skipped:
        logging.error(f"{n_skipped} files skipped; aborting before ingestion.")
        sys.exit(1)
    if args.dry_run:
        logging.info(f"{len(data)} datasets would be ingested to {args.collection}.")
        return
    data.write(args.records, overwrite=True)
    logging.info(f"Data records written to {args.records}.")

    logging.info(f"Ingesting files to {args.collection}")
    ingest_files(
        args.repo_dest,
        "template_coadd",
        args.collection,
        table_file=args.records,
        data_id=("instrument=LSSTCam",),
        transfer="direct",
    )

    butler_dest = Butler(args.repo_dest)
    refs = butler_dest.query_datasets(
        "template_coadd", collections=args.collection, limit=None
    )
    logging.info(f"{len(refs)} datasets at {args.collection}")


if __name__ == "__main__":
    main()
