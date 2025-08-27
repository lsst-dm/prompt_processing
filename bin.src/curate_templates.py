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


"""Curate template_coadd datasets for LSSTCam.

This script performs a curation cut on a set of template_coadd datasets and
prepares them for manual vetting.
"""

import argparse
import logging
import numpy as np
import os
import sys

from astropy.table import Table

from lsst.daf.butler import Butler, CollectionType


def _make_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "repo",
        help="An existing data repository containing the input collections.",
    )
    parser.add_argument(
        "collections",
        help="The input collections to search for template_coadd and coadd_depth_table datasets.",
    )
    parser.add_argument(
        "tag",
        help="A Jira ticket number for the new template collection name.",
    )
    parser.add_argument(
        "release_num",
        help="The release number (##) for the given tag.",
    )
    parser.add_argument(
        "--records_path",
        required=False,
        default="",
        help="An absolute filepath to save records to.",
    )
    parser.add_argument(
        "--stat_records",
        required=False,
        help="An output table file with accepted/rejected stats on templates that pass"
        " curation. Default is release_{release_num}_stat_records.csv.",
    )
    parser.add_argument(
        "--filter_by",
        required=False,
        default="depth_above_threshold_3",
        help="The coadd_depth_table column to use for filtering."
        " Default is depth_above_threshold_3.",
    )
    parser.add_argument(
        "--cutoff",
        required=False,
        default=95,
        help="The curation process will filter out anything below this cutoff."
        " Default is 95.",
    )
    return parser


def get_tracts(butler):
    tracts = []
    coadd_depth_tables = butler.registry.queryDatasets(datasetType='coadd_depth_table')
    for item in coadd_depth_tables:
        tract = item.dataId['tract']
        tracts.append(tract)
    tracts = set(tracts)
    return tracts


def make_threshold_cuts(butler, template_coadds, n_images, tracts, filter_by, cutoff):
    accepted_drefs = []
    accepted_n_image_refs = []
    rejected_drefs = []

    for tract in tracts:
        coadd_depth_table = butler.get('coadd_depth_table', tract=tract)
        mask = (coadd_depth_table[filter_by] > cutoff)

        accepted_coadds = coadd_depth_table[mask]
        for patch_band in accepted_coadds['patch', 'band']:
            patch = patch_band[0]
            band = patch_band[1]
            dref = [d for d in template_coadds
                    if d.dataId['tract'] == tract
                    and d.dataId['patch'] == patch
                    and d.dataId['band'] == band
                    ]
            n_image_dref = [d for d in n_images
                            if d.dataId['tract'] == tract
                            and d.dataId['patch'] == patch
                            and d.dataId['band'] == band
                            ]

            if len(dref) > 1:
                sorted_dupe_entry = sorted(dref, key=lambda ref: ref.run)
                ref = sorted_dupe_entry[-1]
            else:
                ref = dref[0]
            accepted_drefs.append(ref)

            if len(n_image_dref) > 1:
                sorted_dupe_entry = sorted(n_image_dref, key=lambda ref: ref.run)
                n_image_ref = sorted_dupe_entry[-1]
            else:
                n_image_ref = n_image_dref[0]
            accepted_n_image_refs.append(n_image_ref)

        rejected_coadds = coadd_depth_table[~mask]
        for patch_band in rejected_coadds['patch', 'band']:
            patch = patch_band[0]
            band = patch_band[1]
            dref = [d for d in template_coadds
                    if d.dataId['tract'] == tract
                    and d.dataId['patch'] == patch
                    and d.dataId['band'] == band
                    ]

            if len(dref) > 1:
                sorted_dupe_entry = sorted(dref, key=lambda ref: ref.run)
                ref = sorted_dupe_entry[-1]
            else:
                ref = dref[0]
            rejected_drefs.append(ref)
    return accepted_drefs, rejected_drefs, accepted_n_image_refs


def run_stats(accepted_drefs, rejected_drefs, tracts, stats_records_file):
    # Create table of accepted drefs
    accepted = Table()
    accepted_tracts = []
    accepted_patches = []
    accepted_bands = []
    bands = ['u', 'g', 'r', 'i', 'z', 'y']

    for ref in accepted_drefs:
        accepted_tracts.append(ref.dataId['tract'])
        accepted_patches.append(ref.dataId['patch'])
        accepted_bands.append(ref.dataId['band'])

    accepted_table_data = [accepted_tracts, accepted_patches, accepted_bands]
    accepted = Table(data=accepted_table_data, names=['tract', 'patch', 'band'])

    # Create table of rejected drefs
    rejected = Table()
    rejected_tracts = []
    rejected_patches = []
    rejected_bands = []

    for ref in rejected_drefs:
        rejected_tracts.append(ref.dataId['tract'])
        rejected_patches.append(ref.dataId['patch'])
        rejected_bands.append(ref.dataId['band'])

    rejected_table_data = [rejected_tracts, rejected_patches, rejected_bands]
    rejected = Table(data=rejected_table_data, names=['tract', 'patch', 'band'])

    # Run stats
    by_band_stats = []
    for tract in tracts:
        tract_band_stats = []
        for band in bands:
            accepted_bands = ((accepted['tract'] == tract) & (accepted['band'] == band)).sum()
            rejected_bands = ((rejected['tract'] == tract) & (rejected['band'] == band)).sum()
            total_bands = accepted_bands + rejected_bands
            if total_bands == 0:
                tract_band_stats.append(["0 / 0", np.nan])
            else:
                tract_band_stats.append([f"{accepted_bands} / {total_bands}",
                                         accepted_bands / total_bands * 100])
        by_band_stats.append(tract_band_stats)

    # Compile stats into a table and save
    accepted_col_names = [f"{band}_{suffix}" for band in bands for suffix
                          in ("num_accepted", "percent_accepted")]
    by_tract_names = ['tract'] + accepted_col_names

    stat_table_data = {col: [] for col in by_tract_names}

    for tract_index, tract in enumerate(tracts):
        band_stats = by_band_stats[tract_index]

        stat_table_data['tract'].append(tract)

        for band_idx, band in enumerate(bands):
            accepted_str, percent = band_stats[band_idx]
            stat_table_data[f"{band}_num_accepted"].append(accepted_str)
            stat_table_data[f"{band}_percent_accepted"].append(percent)
    by_tract_stats = Table(stat_table_data)
    by_tract_stats.write(stats_records_file, format='ascii.csv', overwrite=True)


def main():
    logging.basicConfig(level=logging.INFO, stream=sys.stdout)

    # Hide spurious messages from numexpr by setting the numexpr env var.
    os.environ["NUMEXPR_MAX_THREADS"] = "8"

    args = _make_parser().parse_args()
    butler = Butler(args.repo, collections=args.collections)
    butler_write = Butler(args.repo, writeable=True)

    # Create directory for records
    directory = args.records_path
    if directory:
        if not os.path.exists(directory):
            os.makedirs(directory)

    # Set (stat_)records defaults based on release_num if not provided
    if args.stat_records is None:
        args.stat_records = f"release_{args.release_num}_stat_records.csv"

    # Create tagged collection, abort if it already exists.
    tagged_collection = f"LSSTCam/templates/candidates/{args.tag}/release_{args.release_num}"
    registered = butler_write.collections.register(
        tagged_collection, type=CollectionType.TAGGED
    )
    if not registered:
        logging.error(f"Collection {tagged_collection} already exists. Aborting.")
        sys.exit(1)

    refs = butler.query_datasets("template_coadd", limit=None)
    n_image_refs = butler.query_datasets("template_coadd_n_image", limit=None)
    logging.info(f"Found {len(refs)} template_coadd datasets in {args.collections}.")

    # Get a list of the tracts inside the template collection.
    tracts = get_tracts(butler)

    # Filter out template_coads that don't meet the cutoff and save them to record.
    logging.info("Starting curation.")
    accepted_drefs, rejected_drefs, accepted_n_image_refs = make_threshold_cuts(butler, refs,
                                                                                n_image_refs, tracts,
                                                                                args.filter_by, args.cutoff
                                                                                )
    logging.info(f"Curation complete. Accepted {len(accepted_drefs)} out of {len(refs)}"
                 f" template_coadd datasets in {args.collections}.")

    # Run accepted/rejected statistics and save them to record.
    logging.info("Starting stat generation.")
    stats_records_file = os.path.join(directory, args.stat_records)
    run_stats(accepted_drefs, rejected_drefs, tracts, stats_records_file)
    logging.info("Stat generation complete. Accepted/rejected stat records written to"
                 f" {stats_records_file}.")

    # Associate accepted template_coadds to tagged collection.
    logging.info(f"Associating {len(accepted_drefs)} template_coadds to {tagged_collection}.")
    butler_write.registry.associate(tagged_collection, accepted_drefs)
    logging.info("Association complete.")

    # Associate accepted template_coadd_n_images to tagged collection.
    logging.info(f"Associating {len(accepted_n_image_refs)} template_coadd_n_images to {tagged_collection}.")
    butler_write.registry.associate(tagged_collection, accepted_n_image_refs)
    logging.info("Association complete.")


if __name__ == "__main__":
    main()
