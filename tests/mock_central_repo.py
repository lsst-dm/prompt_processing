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


"""Common code references to the test repository.

This file must be kept in sync with tests/data/central_repo, otherwise tests
that depend on it will fail.
"""


__all__ = ["TestRepo"]


import astropy.coordinates
import astropy.time
import astropy.units as u

import astro_metadata_translator
import lsst.resources
import lsst.daf.butler as daf_butler
from lsst.obs.base.formatters.fitsExposure import FitsImageFormatter  # Can't use unqualified name
from lsst.obs.base import ingest

import shared.visit


class TestRepo:
    """Entirely passive "class" whose main purpose is to get these definitions
    out of the global namespace.

    Should never need to be instantiated.
    """
    # The short name of the instrument used in the test repo.
    instname = "LSSTCam"
    # Full name of the physical filter for the test file.
    filter = "g_6"
    # The skymap name used in the test repo.
    skymap_name = "lsst_cells_v1"
    # The day_obs used for the init-output runs in the test repo.
    # Does not need to be synchronized with simulated metadata.
    sim_date = astropy.time.Time("2025-09-13T00:00:00Z")
    # The deployment ID used in the test repo.
    sim_deployment = "pipelines-cf62e06-config-8acfde6"

    @classmethod
    def fake_file_data(cls, filename, dimensions, instrument, visit):
        """Return file data for a mock file to be ingested.

        This method supports both science and engineering data, and
        distinguishes them based on the contents of the ``visit`` argument.

        Parameters
        ----------
        filename : `str`
            Full path to the file to mock. Can be a non-existant file.
        dimensions : `lsst.daf.butler.DimensionsUniverse`
            The full set of dimensions for this butler.
        instrument : `lsst.obs.base.Instrument`
            The instrument the file is supposed to be from.
        visit : `shared.visit.FannedOutVisit`
            Group of snaps from one detector to be processed.

        Returns
        -------
        data_id, file_data, : `DataCoordinate`, `RawFileData`
            The id and descriptor for the mock file.
        """
        exposure_id = int(visit.groupId)
        data_id = daf_butler.DataCoordinate.standardize({"exposure": exposure_id,
                                                         "detector": visit.detector,
                                                         "instrument": instrument.getName()},
                                                        universe=dimensions)

        start_time = astropy.time.Time("2025-05-22T05:20:46", scale="tai")
        day_obs = 20250521

        # Simulate either science or engineering data
        if visit.coordinateSystem != shared.visit.FannedOutVisit.CoordSys.NONE:
            visit_id = exposure_id
            native_rotation_angle = astropy.coordinates.Angle(visit.cameraAngle*u.degree)
            rotation_system = visit.rotationSystem.name.lower()
            obs_type = "science"
        else:
            visit_id = None
            native_rotation_angle = None
            rotation_system = None
            obs_type = "goofing off"

        obs_info = astro_metadata_translator.makeObservationInfo(
            instrument=instrument.getName(),
            datetime_begin=start_time,
            datetime_end=start_time + 30*u.second,
            exposure_id=exposure_id,
            exposure_group=visit.groupId,
            visit_id=visit_id,
            boresight_rotation_angle=native_rotation_angle,
            boresight_rotation_coord=rotation_system,
            tracking_radec=visit.get_boresight_icrs(),  # Supports NONE coordinates
            observation_id=visit.groupId,
            physical_filter=cls.filter,
            exposure_time=30.0*u.second,
            exposure_time_requested=30.0*u.second,
            observation_type=obs_type,
            observing_day=day_obs,
            group_counter_start=exposure_id,
            group_counter_end=exposure_id,
        )
        dataset_info = ingest.RawFileDatasetInfo(data_id, obs_info)
        file_data = ingest.RawFileData([dataset_info],
                                       lsst.resources.ResourcePath(filename),
                                       FitsImageFormatter,
                                       instrument)
        return data_id, file_data
