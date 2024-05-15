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


__all__ = ["DummyTask"]


import lsst.pipe.base as pipe_base


class DummyConnections(pipe_base.PipelineTaskConnections,
                       dimensions={"instrument", "group", "detector"},
                       ):
    # Emulate actual input for PreloadApdbTask
    input = pipe_base.connectionTypes.Input(
        doc="The (possibly predicted) region and time associated with this group.",
        name="regionTimeInfo",
        storageClass="RegionTimeInfo",
        dimensions={"instrument", "group", "detector"},
    )


class DummyConfig(pipe_base.PipelineTaskConfig, pipelineConnections=DummyConnections):
    pass


# TODO: This class is a test placeholder until real tasks can be implemented.
# Remove this file after DM-31833 or DM-42576.
class DummyTask(pipe_base.PipelineTask):
    _DefaultName = "dummy"
    ConfigClass = DummyConfig
