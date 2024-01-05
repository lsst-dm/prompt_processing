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


import time
import unittest

import astropy.units as u

import lsst.verify
import lsst.analysis.tools

from activator.timer import time_this_to_bundle


class TimeThisTest(unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.bundle = lsst.analysis.tools.interfaces.MetricMeasurementBundle(dataset_identifier="unittest")

    def test_basic(self):
        duration = 0.2
        with time_this_to_bundle(self.bundle, "testAction", "basicMetric"):
            time.sleep(duration)

        self.assertEqual(self.bundle.keys(), {"testAction"})
        self.assertEqual(len(self.bundle["testAction"]), 1)
        meas = self.bundle["testAction"][0]

        self.assertEqual(meas.metric_name, lsst.verify.Name(metric="basicMetric"))
        self.assertIsNotNone(meas.quantity)
        self.assertGreater(meas.quantity, duration * u.second)
        self.assertLess(meas.quantity, 2 * duration * u.second)

    def test_exception(self):
        duration = 0.2
        try:
            with time_this_to_bundle(self.bundle, "testAction", "prompt_processing.resilientMetric"):
                time.sleep(duration)
                raise RuntimeError("I take exception to that!")
        except RuntimeError:
            pass

        self.assertEqual(self.bundle.keys(), {"testAction"})
        self.assertEqual(len(self.bundle["testAction"]), 1)
        meas = self.bundle["testAction"][0]

        self.assertEqual(meas.metric_name,
                         lsst.verify.Name(package="prompt_processing", metric="resilientMetric"))
        self.assertIsNotNone(meas.quantity)
        self.assertGreater(meas.quantity, duration * u.second)
        self.assertLess(meas.quantity, 2 * duration * u.second)
