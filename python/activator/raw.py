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

__all__ = ["Snap", "RAW_REGEXP", "get_raw_path"]

from dataclasses import dataclass
import re

from .visit import Visit


@dataclass(frozen=True)
class Snap:
    instrument: str    # short name
    detector: int
    group: str         # observatory-specific ID; not the same as visit number
    snap: int          # exposure number within a group
    exp_id: int        # Butler-compatible unique exposure ID
    filter: str        # physical filter

    def __str__(self):
        """Return a short string that disambiguates the image.
        """
        return f"(exposure {self.exp_id}, group {self.group}/{self.snap})"

    @classmethod
    def from_oid(cls, oid: str):
        """Construct a Snap from an image bucket's filename.

        Parameters
        ----------
        oid : `str`
            A pathname from which to extract snap information.
        """
        m = re.match(RAW_REGEXP, oid)
        if m:
            return Snap(instrument=m["instrument"],
                        detector=int(m["detector"]),
                        group=m["group"],
                        snap=int(m["snap"]),
                        exp_id=int(m["expid"]),
                        filter=m["filter"],
                        )
        else:
            raise ValueError(f"{oid} could not be parsed into a Snap")

    def is_consistent(self, visit: Visit):
        """Test if this snap could have come from a particular visit.

        Parameters
        ----------
        visit : `activator.visit.Visit`
            The visit from which snaps were expected.
        """
        return (self.instrument == visit.instrument
                and self.detector == visit.detector
                and self.group == visit.groupId
                and self.snap < visit.snaps
                )


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
