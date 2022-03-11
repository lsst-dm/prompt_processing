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

import os.path
import tempfile
import unittest

from activator.middleware_interface import MiddlewareInterface
from activator.visit import Visit


class MiddlewareInterfaceTest(unittest.TestCase):
    def setUp(self):
        input_dir = os.path.join(os.path.abspath(os.path.dirname(__file__)), "data")
        instrument = "TestCam"
        self.output_dir = tempfile.TemporaryDirectory()
        self.interface = MiddlewareInterface(input_dir, self.output_dir, instrument)
        self.next_visit = Visit(instrument,
                                detector=1,
                                group=1,
                                snaps=1,
                                filter="r",
                                ra=10,
                                dec=20,
                                kind="BIAS")
        self.logger_name = "lsst.activator.middleware_interface"

    def test_init(self):
        """Basic tests of initializing an interface object.
        """
        # did we get a useable butler instance?
        self.assertIn("/tmp/butler-", self.interface.dest.datastore.root.path)
        # Ideas for things to test:
        # * On init, does the right kind of butler get created, with the right
        #   collections, etc?
        # * On init, is the local butler repo purely in memory?

    def test_prep_butler(self):
        with self.assertLogs(self.logger_name, level="INFO") as cm:
            self.interface.prep_butler(self.next_visit)
        msg = f"INFO:{self.logger_name}:Preparing Butler for visit '{self.next_visit}'"
        self.assertEqual(cm.output, [msg])

    def test_ingest_image(self):
        with self.assertLogs(self.logger_name, level="INFO") as cm:
            self.interface.ingest_image(self.next_visit)
        msg = f"INFO:{self.logger_name}:Ingesting image id '{self.next_visit}'"
        self.assertEqual(cm.output, [msg])

    def test_run_pipeline(self):
        with self.assertLogs(self.logger_name, level="INFO") as cm:
            self.interface.run_pipeline(self.next_visit, 1)
        msg = f"INFO:{self.logger_name}:Running pipeline bias.yaml on visit '{self.next_visit}', snaps 1"
        self.assertEqual(cm.output, [msg])
