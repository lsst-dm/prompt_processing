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


"""Selectively export some contents from a butler repo.

This script selects some data in a source butler repo, and makes an export
file for importing to the test central prompt processing repository.
"""


import argparse
import collections.abc
import logging
import sys
import tempfile
import yaml

import lsst.daf.butler as daf_butler
from lsst.utils.timer import time_this


def _make_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--src-repo",
        required=True,
        help="The location of the repository from which datasets are exported.",
    )
    parser.add_argument(
        "--target-repo",
        required=False,
        help="The location of the repository to which datasets are exported. "
             "Datasets already existing in the target repo will not be "
             "exported from the source repo. If no target repo is given, all "
             "selected datasets in the source repo will be exported.",
    )
    parser.add_argument(
        "--select",
        required=True,
        help="URI to a YAML file containing expressions to identify the "
             "datasets and collections to be exported. An example is at "
             "etc/export_latiss.yaml."
    )
    return parser


def main():
    logging.basicConfig(level=logging.INFO, stream=sys.stdout)

    args = _make_parser().parse_args()
    src_butler = daf_butler.Butler(args.src_repo)
    with open(args.select, "r") as file:
        wants = yaml.safe_load(file)

    with tempfile.TemporaryDirectory() as temp_repo:
        if args.target_repo:
            target_butler = daf_butler.Butler(args.target_repo, writeable=False)
        else:
            # If no target_butler is given, create an empty one.
            config = daf_butler.Butler.makeRepo(temp_repo)
            target_butler = daf_butler.Butler(config)

        with time_this(msg="Datasets and collections exported", level=logging.INFO):
            _export_for_copy(src_butler, target_butler, wants)


def _export_for_copy(butler, target_butler, wants):
    """Export selected data to make copies in another butler repo.

    Parameters
    ----------
    butler : `lsst.daf.butler.Butler`
        The source Butler from which datasets are exported.
    target_butler : `lsst.daf.butler.Butler`
        The target Butler to which datasets are exported. It is checked
        to avoid exporting existing datasets. No checks are done to
        verify if datasets are really identical.
    wants : `dict`
        A dictionary to identify selections with optional keys:

        ``"datasets"``, optional
            A list of dataset selection expressions (`list` of `dict`).
            The list is iterated over to find matching datasets in the butler,
            with the matching criteria provided via the selection expressions.
            Each selection expression has the keyworded argument dictionary to
            be passed to butler to query datasets; it has the same meanings
            as the parameters of `lsst.daf.butler.Butler.query_datasets`.
        ``"collections"``, optional
            A list of collection selection expressions (`list` of `dict`).
            The list is iterated over to find matching collections in the butler,
            with the matching criteria provided via the selection expressions.
            Each selection expression has the keyworded argument dictionary to
            be passed to butler to query collectionss; it has the same meanings
            as the parameters of `lsst.daf.butler.ButlerCollections.query`.
    """
    with butler.export(format="yaml") as contents:
        if "datasets" in wants:
            for selection in wants["datasets"]:
                logging.debug(f"Selecting datasets: {selection}")
                if "collections" not in selection:
                    raise RuntimeError("Must provide collections to select datasets.")
                if "datasetType" in selection:
                    dataset_types = [selection.pop("datasetType")]
                else:
                    # TODO: A new query API after DM-45873 may replace or improve this usage.
                    all_types = {t.name for t in butler.registry.queryDatasetTypes()}
                    collections_info = butler.collections.query_info(
                        selection["collections"], include_summary=True
                    )
                    dataset_types = butler.collections._filter_dataset_types(
                        all_types, collections_info
                    )
                all_records = set(_query_no_undefined(butler, dataset_types, **selection))
                if not all_records:
                    raise RuntimeError("Query found no matches in source repo.")
                target_records = set(_query_no_undefined(target_butler, dataset_types, **selection))
                missing = all_records - target_records
                logging.debug("Found %d matching datasets. %d present in target, %d to export.",
                              len(all_records), len(all_records & target_records), len(missing))
                contents.saveDatasets(missing)

        # Save selected collections and chains
        if "collections" in wants:
            for selection in wants["collections"]:
                for collection in butler.collections.query(**selection):
                    logging.debug(f"Selecting collection {collection}")
                    try:
                        if not target_butler.collections.query(collection):
                            contents.saveCollection(collection)
                    except daf_butler.registry.MissingCollectionError:
                        # MissingCollectionError is raised if the collection does not exist in target_butler.
                        contents.saveCollection(collection)


def _query_no_undefined(butler: daf_butler.Butler,
                        dataset_types: collections.abc.Iterable[str | daf_butler.DatasetType],
                        *args,
                        **kwargs) -> collections.abc.Iterable[daf_butler.DatasetRef]:
    """Query a Butler, treating missing data IDs, dataset types, etc. as
    empty results.

    Parameters
    ----------
    butler : `lsst.daf.butler.Butler`
        The Butler to query.
    dataset_types : iterable [`str` | `lsst.daf.butler.DatasetType`]
        Iterable of dataset type object or name to search for.
    *args, **kwargs
        Parameters for describing the dataset query. They have the same
        meanings as the parameters of `lsst.daf.butler.query_datasets`.

    Returns
    -------
    datasets : iterable [`lsst.daf.butler.DatasetRef`]
        The datasets found by the query. All dataset refs are fully expanded.
    """
    datasets = set()
    for dataset_type in dataset_types:
        try:
            datasets |= set(butler.query_datasets(
                # explain=False because empty query result is ok here.
                dataset_type, explain=False, with_dimension_records=True, *args, **kwargs
            ))
        except (daf_butler.DataIdValueError,
                daf_butler.MissingDatasetTypeError,
                daf_butler.MissingCollectionError) as e:
            logging.debug("query failed with %s.", e)
    return datasets


if __name__ == "__main__":
    main()
