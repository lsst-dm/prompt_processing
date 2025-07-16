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

__all__ = ["import_iers_cache"]


import logging
import os
import tempfile

import astropy.utils.data

import lsst.resources
import lsst.utils.timer


_log = logging.getLogger("lsst." + __name__)
_log.setLevel(logging.DEBUG)


LOCAL_CACHE = os.path.join(tempfile.gettempdir(), "iers-cache.zip")


def import_iers_cache():
    """Download the IERS cache from a shared resource at USDF.
    """
    remote_cache = os.environ.get("CENTRAL_IERS_CACHE")
    if not os.path.exists(LOCAL_CACHE):
        if not remote_cache:
            _log.warning("No IERS download has been configured. Time conversions may be inaccurate.")
            return
        with lsst.utils.timer.time_this(_log, msg="Download IERS", level=logging.DEBUG):
            src = lsst.resources.ResourcePath(remote_cache)
            dest = lsst.resources.ResourcePath(LOCAL_CACHE)
            dest.transfer_from(src, "copy")
    else:
        _log.info("IERS cache already exists in this pod, skipping fresh download. "
                  "This normally means that more than one worker has run in this pod.")

    with lsst.utils.timer.time_this(_log, msg="Update IERS", level=logging.DEBUG):
        astropy.utils.data.import_download_cache(LOCAL_CACHE, update_cache=True)
        _log.info("IERS cache is up to date.")
