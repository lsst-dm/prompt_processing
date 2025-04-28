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

import glob
import os.path
import unittest

from lsst.pipe.base import Pipeline
from lsst.utils import getPackageDir


class PipelineDefintionsTestSuite(unittest.TestCase):
    """Tests of our pipeline definitions."""

    def setUp(self):
        self.path = os.path.join(getPackageDir("prompt_processing"), "pipelines")

    def test_graph_build(self):
        """Test that each pipeline definition file can be
        used to build a graph.
        """
        files = glob.glob(os.path.join(self.path, "**", "*.yaml"))
        for file in files:
            with self.subTest(file):
                pipeline = Pipeline.from_uri(file)
                pipeline.addConfigOverride(
                    "parameters", "apdb_config", "some/file/path.yaml"
                )
                # If this fails, it will produce a useful error message.
                pipeline.to_graph()
