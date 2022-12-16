#!/usr/bin/env python
# This file is part of prompt_prototype.
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


"""Simple script for creating a repository at a remote URI, given
a source repository and export file.

For most values of --target-repo and --seed-config, this script is only useful
if run from the prompt-proto project on Google Cloud (because of access
restrictions to **both** the repository's storage location and its registry
database).

The user is responsible for clearing any old copies of the repository from
both the target URI and the registry database.
"""


import argparse
import logging
import os
import sys

from lsst.utils import getPackageDir
from lsst.utils.timer import time_this
from lsst.daf.butler import Butler, CollectionType, Config
from lsst.obs.base import Instrument


def _make_parser():
    parser = argparse.ArgumentParser()
    # Could reasonably be positional arguments, but keep them as keywords to
    # prevent users from confusing --src-repo with --target-repo.
    parser.add_argument("--src-repo", required=True,
                        help="The location of the repository whose files are to be copied.")
    parser.add_argument("--target-repo", required=True,
                        help="The URI of the repository to create.")
    parser.add_argument("--seed-config",
                        default=os.path.join(getPackageDir("prompt_prototype"), "etc", "db_butler.yaml"),
                        help="The config file to use for the new repository. Defaults to etc/db_butler.yaml.")
    parser.add_argument("--export-file", default="export.yaml",
                        help="The export file containing the repository contents. Defaults to ./export.yaml.")
    parser.add_argument("--hsc-rc2", action="store_true", help="Extra fix up for HSC-RC2 dataset.")
    return parser


def _add_chains(butler):
    """Create collections to serve as a uniform interface.

    Parameters
    ----------
    butler : `lsst.daf.butler.Butler`
        A Butler pointing to the repository to modify. Assumed to already contain the following collections:

        - standard calibration collection
        - standard skymap collection
        - templates/*
        - refcats/*
    """
    butler.registry.registerCollection("templates", type=CollectionType.CHAINED)
    butler.registry.setCollectionChain(
        "templates",
        list(butler.registry.queryCollections("templates/*", collectionTypes=CollectionType.RUN))
    )

    butler.registry.registerCollection("refcats", type=CollectionType.CHAINED)
    butler.registry.setCollectionChain(
        "refcats",
        list(butler.registry.queryCollections("refcats/*", collectionTypes=CollectionType.RUN))
    )

    instrument = Instrument.fromName(list(butler.registry.queryDataIds("instrument"))[0]["instrument"],
                                     butler.registry)
    defaults = instrument.makeCollectionName("defaults")
    butler.registry.registerCollection(defaults, type=CollectionType.CHAINED)
    calib_collection = instrument.makeCalibrationCollectionName()
    butler.registry.registerCollection(calib_collection, type=CollectionType.CHAINED)
    butler.registry.setCollectionChain(
        defaults,
        [calib_collection, "templates", "skymaps", "refcats"]
    )


def _hsc_rc2(butler):
    """fix up some specifics of the HSC-RC2 dataset export

    Parameters
    ----------
    butler: `lsst.daf.butler.Butler`
        The source Butler from which datasets are exported
    """
    # Chain calibration collections
    instrument = Instrument.fromName("HSC", butler.registry)
    butler.registry.setCollectionChain(
        instrument.makeCalibrationCollectionName(),
        [
            "HSC/calib/DM-32378",
            "HSC/calib/gen2/20180117",
            "HSC/calib/DM-28636",
        ],
    )
    # Chain rerun collections to templates
    # The export script should have guaranteed that there are only coadds in these collections.
    current = butler.registry.getCollectionChain("templates")
    addition = butler.registry.queryCollections("HSC/runs/*",
                                                collectionTypes=CollectionType.RUN)
    butler.registry.setCollectionChain("templates",
                                       list(addition) + list(current),
                                       flatten=False)


def main():
    logging.basicConfig(level=logging.INFO, stream=sys.stdout)

    args = _make_parser().parse_args()
    seed_config = Config(args.seed_config)
    logging.info("Creating repository at %s...", args.target_repo)
    with time_this(msg="Repository creation", level=logging.INFO):
        config = Butler.makeRepo(args.target_repo, config=seed_config, overwrite=False)
    with time_this(msg="Butler creation", level=logging.INFO):
        butler = Butler(config, writeable=True)
    with time_this(msg="Import", level=logging.INFO):
        butler.import_(directory=args.src_repo, filename=args.export_file, transfer="auto")
    _add_chains(butler)
    if args.hsc_rc2:
        _hsc_rc2(butler)


if __name__ == "__main__":
    main()
