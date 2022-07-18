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

"""Common definitions of raw paths.

This module provides tools to convert raw paths into exposure metadata and
vice versa.
"""

__all__ = ["RAW_REGEXP", "get_raw_path"]

import re

# Format for filenames of raws uploaded to image bucket:
# instrument/detector/group/snap/expid/filter/*.(fits, fz, fits.gz)
RAW_REGEXP = re.compile(
    r"(?P<instrument>.*?)/(?P<detector>\d+)/(?P<group>.*?)/(?P<snap>\d+)/(?P<expid>.*?)/(?P<filter>.*?)/"
    r"[^/]+\.f"
)


def get_raw_path(instrument, detector, group, snap, exposure_id, filter):
    """The path on which to store raws in the image bucket.
    """
    return (
        f"{instrument}/{detector}/{group}/{snap}/{exposure_id}/{filter}"
        f"/{instrument}-{group}-{snap}"
        f"-{exposure_id}-{filter}-{detector}.fz"
    )
