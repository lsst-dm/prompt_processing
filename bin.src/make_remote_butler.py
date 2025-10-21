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


"""Simple script for creating a repository at a remote URI, given
a source repository and export file.

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
                        default=os.path.join(getPackageDir("prompt_processing"), "etc", "db_butler.yaml"),
                        help="The config file to use for the new repository. Defaults to etc/db_butler.yaml.")
    parser.add_argument("--export-file", default="export.yaml",
                        help="The export file containing the repository contents. Defaults to ./export.yaml.")
    parser.add_argument("--add-instrument-chain", default=None,
                        help="The instrument short name (HSC, LATISS, etc) to make default collections.")
    return parser


def _add_chains(butler, instrument_name):
    """Create collections to serve as a uniform interface.

    Parameters
    ----------
    butler : `lsst.daf.butler.Butler`
        A Butler pointing to the repository to modify. Assumed to already contain the following collections:

        - standard calibration collection
        - standard skymap collection
        - templates/*
        - refcats/*
    instrument_name : `str`
        The short name of the instrument.
    """
    butler.collections.register(f"{instrument_name}/templates", type=CollectionType.CHAINED)

    butler.collections.register("refcats", type=CollectionType.CHAINED)
    butler.collections.redefine_chain(
        "refcats",
        list(butler.collections.query("refcats/*", collectionTypes=CollectionType.RUN))
    )

    instrument = Instrument.fromName(instrument_name, butler.registry)
    defaults = instrument.makeUmbrellaCollectionName()
    butler.collections.register(defaults, type=CollectionType.CHAINED)
    calib_collection = instrument.makeCalibrationCollectionName()
    butler.collections.register(calib_collection, type=CollectionType.CHAINED)
    butler.collections.redefine_chain(
        defaults,
        [calib_collection, f"{instrument_name}/templates", "skymaps", "refcats", "pretrained_models"]
    )


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
    if args.add_instrument_chain:
        _add_chains(butler, args.add_instrument_chain)


if __name__ == "__main__":
    main()
