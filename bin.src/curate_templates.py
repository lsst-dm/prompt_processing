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
import os
import sys

from astropy.table import Table, vstack

from lsst.daf.butler import Butler, CollectionType


def _make_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "repo",
        help="An existing data repository containing the input collections.",
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
        "--collections",
        action="extend",
        nargs="+",
        required=True,
        help="The input collections to search for template_coadd and coadd_depth_table datasets.",
    )
    parser.add_argument(
        "--where",
        default="",
        help="A string expression to select datasets in the input collections.",
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
        type=int,
        help="The curation process will filter out anything below this cutoff."
        " Default is 95.",
    )
    return parser


def select_ref(drefs, tract, patch, band, dtype="template_coadd"):
    if not drefs:
        logging.warning(f"No {dtype} found for tract {tract}, patch {patch}, band {band}. Skipping.")
        return None
    if len(drefs) > 1:
        return sorted(drefs, key=lambda ref: ref.run)[-1]
    return drefs[0]


def make_threshold_cuts(butler, template_coadds, n_images, tracts, filter_by, cutoff):
    """Select template_coadd and template_coadd_n_image datasets that pass a depth threshold.

    Parameters
    ----------
    butler : Butler
        Butler instance used to fetch the coadd depth table.
    template_coadds : list of DatasetRef
        Candidate template_coadd references to filter.
    n_images : list of DatasetRef
        Candidate template_coadd_n_image references to filter.
    tracts : list[int]
        List of tract IDs to evaluate.
    filter_by : str
        Column name in coadd_depth_table used for thresholding.
    cutoff : int
        Minimum depth value required for a patch/band to be accepted.

    Returns
    -------
    accepted_drefs : list of DatasetRef
        Template coadd dataset refs that passed the threshold.
    rejected_drefs : list of DatasetRef
        Template coadd dataset refs that did not pass the threshold.
    accepted_n_image_refs : list of DatasetRef
        Corresponding template_coadd_n_image dataset refs for the accepted coadds.
    """
    accepted_drefs = []
    accepted_n_image_refs = []
    rejected_drefs = []

    for tract in tracts:
        coadd_depth_table = butler.get("template_coadd_depth_table", tract=tract)
        mask = (coadd_depth_table[filter_by] > cutoff)

        # --- Handle accepted patches/bands ---
        accepted_coadds = coadd_depth_table[mask]
        for patch_band in accepted_coadds['patch', 'band']:
            patch = patch_band[0]
            band = patch_band[1]

            # Find matching template_coadd references for this tract/patch/band.
            dref = [d for d in template_coadds
                    if d.dataId['tract'] == tract
                    and d.dataId['patch'] == patch
                    and d.dataId['band'] == band
                    ]
            # Find matching template_coadd_n_image references for this tract/patch/band.
            n_image_dref = [d for d in n_images
                            if d.dataId['tract'] == tract
                            and d.dataId['patch'] == patch
                            and d.dataId['band'] == band
                            ]

            # Skip if no template_coadd is found.
            if not dref:
                logging.warning(f"No template_coadd found for tract {tract}, patch {patch}, band {band}. "
                                f"Skipping.")
                continue

            # If duplicates exist, keep the one from the most recent run.
            if len(dref) > 1:
                sorted_dupe_entry = sorted(dref, key=lambda ref: ref.run)
                ref = sorted_dupe_entry[-1]
            else:
                ref = dref[0]
            accepted_drefs.append(ref)

            # Skip if no corresponding template_coadd_n_image is found.
            if not n_image_dref:
                logging.warning(f"No template_coadd_n_image found for tract {tract}, patch {patch}, "
                                f"band {band}. Skipping.")
                continue

            # Again, if duplicates exist, keep the latest run.
            if len(n_image_dref) > 1:
                sorted_dupe_entry = sorted(n_image_dref, key=lambda ref: ref.run)
                n_image_ref = sorted_dupe_entry[-1]
            else:
                n_image_ref = n_image_dref[0]
            accepted_n_image_refs.append(n_image_ref)

        # --- Handle accepted patches/bands ---
        rejected_coadds = coadd_depth_table[~mask]
        for patch_band in rejected_coadds['patch', 'band']:
            patch = patch_band[0]
            band = patch_band[1]

            # Find matching template_coadd references for this tract/patch/band.
            dref = [d for d in template_coadds
                    if d.dataId['tract'] == tract
                    and d.dataId['patch'] == patch
                    and d.dataId['band'] == band
                    ]

            # Skip if no template_coadd is found.
            if not dref:
                logging.warning(f"No template_coadd found for tract {tract}, patch {patch}, band {band}. "
                                f"Skipping.")
                continue

            # If duplicates exist, keep the one from the most recent run.
            if len(dref) > 1:
                sorted_dupe_entry = sorted(dref, key=lambda ref: ref.run)
                ref = sorted_dupe_entry[-1]
            else:
                ref = dref[0]
            rejected_drefs.append(ref)

    return accepted_drefs, rejected_drefs, accepted_n_image_refs


def run_stats(accepted_drefs, rejected_drefs, tracts, stats_records_file):
    """
    Compute per-tract and per-band accepted/rejected statistics and save to CSV.

    Parameters
    ----------
    accepted_drefs : list of DatasetRef
        Template coadd references that passed curation.
    rejected_drefs : list of DatasetRef
        Template coadd references that failed curation.
    tracts : iterable of int
        List of tract IDs to include in the stats.
    stats_records_file : str
        Path to save the resulting CSV file.
    """

    bands = ['u', 'g', 'r', 'i', 'z', 'y']

    # Build accepted table
    if accepted_drefs:
        accepted = Table(
            {
                'tract': [int(r.dataId['tract']) for r in accepted_drefs],
                'patch': [int(r.dataId['patch']) for r in accepted_drefs],
                'band': [str(r.dataId['band']) for r in accepted_drefs],
                'status': ['accepted'] * len(accepted_drefs)
            }
        )
    else:
        accepted = Table(names=('tract', 'patch', 'band', 'status'))

    # Build rejected table
    if rejected_drefs:
        rejected = Table(
            {
                'tract': [int(r.dataId['tract']) for r in rejected_drefs],
                'patch': [int(r.dataId['patch']) for r in rejected_drefs],
                'band': [str(r.dataId['band']) for r in rejected_drefs],
                'status': ['rejected'] * len(rejected_drefs)
            }
        )
    else:
        rejected = Table(names=('tract', 'patch', 'band', 'status'))

    # Combine tables
    all_refs = vstack([accepted, rejected])

    # Group by tract and band
    grouped = all_refs.group_by(['tract', 'band'])

    # Prepare output table
    stat_table_data = {'tract': [], }
    for band in bands:
        stat_table_data[f'{band}_num_accepted'] = []
        stat_table_data[f'{band}_percent_accepted'] = []

    for tract in tracts:
        stat_table_data['tract'].append(tract)
        for band in bands:
            mask = (grouped['tract'] == tract) & (grouped['band'] == band)
            subset = grouped[mask]
            n_total = len(subset)
            n_accepted = (subset['status'] == 'accepted').sum() if n_total > 0 else 0
            percent = (n_accepted / n_total * 100) if n_total > 0 else float('nan')
            stat_table_data[f'{band}_num_accepted'].append(f"{n_accepted} / {n_total}")
            stat_table_data[f'{band}_percent_accepted'].append(percent)

    # Create final table
    stat_table = Table(stat_table_data)
    stat_table.write(stats_records_file, format='ascii.csv', overwrite=True)


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
    username = os.environ.get('USER')
    tagged_collection = f"u/{username}/LSSTCam/templates/candidates/{args.tag}/release_{args.release_num}"
    logging.info(f"Creating tagged collection {tagged_collection}.")
    registered = butler_write.collections.register(
        tagged_collection, type=CollectionType.TAGGED
    )
    if not registered:
        logging.error(f"Collection {tagged_collection} already exists. Aborting.")
        sys.exit(1)

    logging.info("Collecting coadd_depth_table, template_coadd, and template_coadd_n_image refs.")
    coadd_depth_table_refs = butler.query_datasets("template_coadd_depth_table", where=args.where, limit=None)
    if not coadd_depth_table_refs:
        logging.error("No coadd_depth_table datasets found in the given collections.")
        sys.exit(1)

    # Get a list of relavent tracts.
    tracts = {item.dataId['tract'] for item in coadd_depth_table_refs}

    # Ammend the where argument to restrict refs to relavent tracts.
    tracts_str = ",".join(str(t) for t in tracts)
    tract_restriction = f"tract IN ({tracts_str})"
    args.where = f"({args.where}) AND ({tract_restriction})" if args.where else tract_restriction

    # Get relavent template_coadd and template_coadd_n_image refs.
    coadd_refs = butler.query_datasets("template_coadd", where=args.where, limit=None)
    if not coadd_refs:
        logging.error("No template_coadd datasets found in the given collections.")
        sys.exit(1)
    n_image_refs = butler.query_datasets("template_coadd_n_image", where=args.where, limit=None)
    if not n_image_refs:
        logging.error("No template_coadd_n_image datasets found in the given collections.")
        sys.exit(1)
    logging.info(f"Found {len(coadd_refs)} template_coadd datasets with coadd_depth_tables "
                 f"in {args.collections}.")

    # Filter out template_coads that don't meet the cutoff and save them to record.
    logging.info("Starting curation.")
    accepted_drefs, rejected_drefs, accepted_n_image_refs = make_threshold_cuts(butler, coadd_refs,
                                                                                n_image_refs, tracts,
                                                                                args.filter_by, args.cutoff
                                                                                )
    logging.info(f"Curation complete. Accepted {len(accepted_drefs)} out of {len(coadd_refs)}"
                 f" template_coadd datasets in {args.collections}.")

    # Run accepted/rejected statistics and save them to record.
    logging.info("Starting stat generation.")
    stats_records_file = os.path.join(directory, args.stat_records)
    run_stats(accepted_drefs, rejected_drefs, tracts, stats_records_file)
    logging.info("Stat generation complete. Accepted/rejected stat records written to"
                 f" {stats_records_file}.")

    # Associate accepted template_coadds and template_coadd_n_images to tagged collection.
    logging.info(f"Associating {len(accepted_drefs)} template_coadds and "
                 f"{len(accepted_n_image_refs)} template_coadd_n_images to {tagged_collection}.")
    butler_write.registry.associate(tagged_collection, accepted_drefs)
    butler_write.registry.associate(tagged_collection, accepted_n_image_refs)
    logging.info("Association complete.")


if __name__ == "__main__":
    main()
