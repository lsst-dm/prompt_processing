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


__all__ = ["PipelinesConfig"]


import os

from lsst.utils import getPackageDir

from .visit import FannedOutVisit


class PipelinesConfig:
    """A pipeline configuration for the Prompt Processing service.

    This class provides the execution framework with a simple interface for
    identifying the pipeline to execute. It attempts to abstract the details of
    which factors affect the choice of pipeline to make it easier to add new
    features in the future.

    Notes
    -----
    While it is not expected that there will ever be more than one
    PipelinesConfig instance in a program's lifetime, this class is *not* a
    singleton and objects must be passed explicitly to the code that
    needs them.
    """

    def get_pipeline_file(self, visit: FannedOutVisit) -> str:
        """Identify the pipeline to be run, based on the provided visit.

        Parameters
        ----------
        visit : `activator.visit.FannedOutVisit`
            The visit for which a pipeline must be selected.

        Returns
        -------
        pipeline : `str`
            A path to a configured pipeline file.
        """
        # TODO: We hacked the basepath in the Dockerfile so this works both in
        # development and in service container, but it would be better if there
        # were a path that's valid in both.
        return os.path.join(getPackageDir("prompt_prototype"),
                            "pipelines",
                            visit.instrument,
                            "ApPipe.yaml")
