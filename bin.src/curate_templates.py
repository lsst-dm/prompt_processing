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
import time

from astropy.table import Table, vstack

from lsst.daf.butler import Butler, CollectionType


def _make_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "repo",
        help="An existing data repository containing the input collections.",
    )
    parser.add_argument(
        "skymap",
        help="The skymap used for this batch of templates.",
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
        nargs="+",
        required=True,
        help="The input collections to search for template_coadd and coadd_depth_table datasets.",
    )
    parser.add_argument(
        "--where",
        default="instrument='LSSTCam' AND skymap='lsst_cells_v2'",
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
        "--min_filter",
        nargs="+",
        metavar="COLUMN=CUTOFF",
        help="Filtering criteria in the form column=cutoff (e.g. depth_above_threshold_3=80)"
        " representing minimum cutoff values (e.g. column <= cutoff)."
        " Default is --min_filter depth_above_threshold_3=80."
        " Column must be from the coadd_depth_table.",
    )
    parser.add_argument(
        "--max_filter",
        nargs="+",
        metavar="COLUMN=CUTOFF",
        help="Filtering criteria in the form column=cutoff (e.g. chip_gap_percent=0.5)"
        " representing maximum cutoff values (e.g. column <= cutoff)."
        " Default is --max_filter chip_gap_percent=0.5."
        " Column must be from the coadd_depth_table.",
    )
    return parser


def select_ref(drefs, tract, patch, band, dtype="template_coadd"):
    if not drefs:
        logging.warning(f"No {dtype} found for tract {tract}, patch {patch}, band {band}. Skipping.")
        return None
    if len(drefs) > 1:
        return sorted(drefs, key=lambda ref: ref.run)[-1]
    return drefs[0]


def make_threshold_cuts(butler, skymap, template_coadds, n_images, tracts, min_filters, max_filters):
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
    min_filters : dict[str, int]
        Mapping of column names to minimum cutoff values. A coadd is retained
        only if it meets all thresholds, i.e., for each (column, cutoff),
        ``table[column] >= cutoff``.
    max_filters : dict[str, int]
        Mapping of column names to maximum cutoff values. A coadd is retained
        only if it meets all thresholds, i.e., for each (column, cutoff),
        ``table[column] <= cutoff``.

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
        coadd_depth_table = butler.get("template_coadd_depth_table", skymap=skymap, tract=tract)
        mask = True
        for filter_by, cutoff in min_filters.items():
            mask &= (coadd_depth_table[filter_by] >= cutoff)
        for filter_by, cutoff in max_filters.items():
            mask &= (coadd_depth_table[filter_by] <= cutoff)

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
    stat_table_data = {'tract': [], 'percent_coverage': [], }
    for band in bands:
        stat_table_data[f'{band}_num_accepted'] = []
        stat_table_data[f'{band}_num_rejected'] = []
        stat_table_data[f'{band}_percent_accepted'] = []

    for tract in tracts:
        stat_table_data['tract'].append(tract)
        total = 0
        for band in bands:
            mask = (grouped['tract'] == tract) & (grouped['band'] == band)
            subset = grouped[mask]
            n_total = len(subset)
            n_accepted = (subset['status'] == 'accepted').sum() if n_total > 0 else 0
            n_rejected = (subset['status'] == 'rejected').sum() if n_total > 0 else 0
            percent = (n_accepted / n_total * 100) if n_total > 0 else float('nan')
            total += n_accepted
            stat_table_data[f'{band}_num_accepted'].append(n_accepted)
            stat_table_data[f'{band}_num_rejected'].append(n_rejected)
            stat_table_data[f'{band}_percent_accepted'].append(percent)
        stat_table_data['percent_coverage'].append(total / 600 * 100)  # 100 patches per tract x 6 bands

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

    # Parse filters, if any, into a dictionary.
    if args.min_filter is None:
        # Set default filters.
        min_filters = {
            "depth_above_threshold_3": 80,
        }
    else:
        min_filters = {}
        for item in args.min_filter:
            try:
                filter_by, cutoff = map(str.strip, item.split("=", 1))
                min_filters[filter_by] = int(cutoff)
            except ValueError:
                logging.error(f"Invalid filter format: '{item}'. Expected COLUMN=INTEGER.")
                sys.exit(1)
    if args.max_filter is None:
        # Set default filters.
        max_filters = {
            "chip_gap_percent": 0.5,
        }
    else:
        max_filters = {}
        for item in args.max_filter:
            try:
                filter_by, cutoff = map(str.strip, item.split("=", 1))
                max_filters[filter_by] = int(cutoff)
            except ValueError:
                logging.error(f"Invalid filter format: '{item}'. Expected COLUMN=INTEGER.")
                sys.exit(1)

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
    logging.info("Starting curation with filters:")
    for key, value in min_filters.items():
        logging.info(f"{key} >= {value}")
    for key, value in max_filters.items():
        logging.info(f"{key} <= {value}")
    start_time = time.perf_counter()
    accepted_drefs, rejected_drefs, accepted_n_image_refs = make_threshold_cuts(butler, args.skymap,
                                                                                coadd_refs, n_image_refs,
                                                                                tracts, min_filters,
                                                                                max_filters)
    end_time = time.perf_counter()
    logging.info(f"Curation complete. Accepted {len(accepted_drefs)} out of {len(coadd_refs)}"
                 f" template_coadd datasets in {args.collections}."
                 f" Process took {(end_time - start_time) / 60:.2f} minutes.")

    # Run accepted/rejected statistics and save them to record.
    logging.info("Starting stat generation.")
    start_time = time.perf_counter()
    stats_records_file = os.path.join(directory, args.stat_records)
    run_stats(accepted_drefs, rejected_drefs, tracts, stats_records_file)
    end_time = time.perf_counter()
    logging.info("Stat generation complete. Accepted/rejected stat records written to"
                 f" {stats_records_file}. Process took {(end_time - start_time) / 60:.2f} minutes.")

    # Associate accepted template_coadds and template_coadd_n_images to tagged collection.
    logging.info(f"Associating {len(accepted_drefs)} template_coadds and "
                 f"{len(accepted_n_image_refs)} template_coadd_n_images to {tagged_collection}.")
    start_time = time.perf_counter()
    butler_write.registry.associate(tagged_collection, accepted_drefs)
    butler_write.registry.associate(tagged_collection, accepted_n_image_refs)
    end_time = time.perf_counter()
    logging.info(f"Association complete. Process took {(end_time - start_time) / 60:.2f} minutes.")


if __name__ == "__main__":
    main()
