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


"""Export an IERS cache to a local or remote location.
"""


import argparse
import logging
import sys
import tempfile

import astropy.utils.data
from astropy.utils import iers

import lsst.resources


IERS_URLS = [iers.IERS_A_URL, iers.IERS_B_URL, iers.IERS_LEAP_SECOND_URL]


def _make_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("destination", nargs="+",
                        help="The path(s) or URI(s) to put the exported IERS cache. Should be a zip file.")
    return parser


def main():
    logging.basicConfig(level=logging.INFO, stream=sys.stdout)

    args = _make_parser().parse_args()
    # Update download cache
    for url in IERS_URLS:
        astropy.utils.data.download_file(url, cache="update")
    with tempfile.NamedTemporaryFile(suffix=".zip") as local_cache:
        astropy.utils.data.export_download_cache(local_cache, IERS_URLS, overwrite=True)
        src = lsst.resources.ResourcePath(local_cache.name, isTemporary=True)
        # Any errors past this point may invalidate the remote cache
        for d in args.destination:
            logging.info("Writing %s...", d)
            dest = lsst.resources.ResourcePath(d)
            dest.transfer_from(src, "copy", overwrite=True)


if __name__ == "__main__":
    main()
