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

from activator.timer import time_this_to_bundle, enforce_schema


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


class EnforceSchemaTest(unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.bundle = lsst.analysis.tools.interfaces.MetricMeasurementBundle(dataset_identifier="unittest")
        self.bundle.setdefault("action1", []).append(
            lsst.verify.Measurement("testMetric1", 42 * u.dimensionless_unscaled))
        self.bundle.setdefault("action1", []).append(
            lsst.verify.Measurement("testMetric2", 2.7 * u.meter / u.second))
        self.bundle.setdefault("action2", []).append(
            lsst.verify.Measurement(lsst.verify.Name(package="test", metric="testMetric3"), 5 * u.second))
        self.bundle.setdefault("action3", [])

    @staticmethod
    def _get_names(meas_list):
        return sorted(str(m.metric_name) for m in meas_list)

    def test_match(self):
        enforce_schema(self.bundle, {"action1": ["testMetric1", "testMetric2"],
                                     "action2": ["test.testMetric3"],
                                     })

        # action3 ignored because no actual measurements
        self.assertEqual(self.bundle.keys(), {"action1", "action2", "action3"})
        self.assertEqual(self._get_names(self.bundle["action1"]), ["testMetric1", "testMetric2"])
        self.assertEqual(self._get_names(self.bundle["action2"]), ["test.testMetric3"])
        self.assertEqual(self._get_names(self.bundle["action3"]), [])

    def test_extra_metrics(self):
        enforce_schema(self.bundle, {"action1": ["testMetric1", "testMetric2", "testMetric2b"],
                                     "action2": ["test.testMetric3"],
                                     "action3": ["testMetric4"],
                                     "action4": ["testMetric5", "testMetric6"],
                                     })

        self.assertEqual(self.bundle.keys(), {"action1", "action2", "action3", "action4"})
        self.assertEqual(self._get_names(self.bundle["action1"]),
                         ["testMetric1", "testMetric2", "testMetric2b"])
        for meas in self.bundle["action1"]:
            if meas.metric_name == lsst.verify.Name(metric="testMetric2b"):
                self.assertIsNone(meas.quantity)
            else:
                self.assertIsNotNone(meas.quantity)
        self.assertEqual(self._get_names(self.bundle["action2"]), ["test.testMetric3"])
        for meas in self.bundle["action2"]:
            self.assertIsNotNone(meas.quantity)
        self.assertEqual(self._get_names(self.bundle["action3"]), ["testMetric4"])
        for meas in self.bundle["action3"]:
            self.assertIsNone(meas.quantity)
        self.assertEqual(self._get_names(self.bundle["action4"]), ["testMetric5", "testMetric6"])
        for meas in self.bundle["action4"]:
            self.assertIsNone(meas.quantity)

    def test_missing_metrics(self):
        with self.assertRaises(RuntimeError):
            enforce_schema(self.bundle, {"action1": ["testMetric1"],
                                         "action2": ["test.testMetric3"],
                                         })

    def test_missing_actions(self):
        with self.assertRaises(RuntimeError):
            enforce_schema(self.bundle, {"action1": ["testMetric1", "testMetric2"],
                                         })

    def test_empty(self):
        enforce_schema(self.bundle, {"action1": ["testMetric1", "testMetric2"],
                                     "action2": ["test.testMetric3"],
                                     "action4": [],
                                     })

        # action3 and action4 ignored because no actual measurements
        self.assertEqual(self.bundle.keys(), {"action1", "action2", "action3"})
        self.assertEqual(self._get_names(self.bundle["action1"]), ["testMetric1", "testMetric2"])
        self.assertEqual(self._get_names(self.bundle["action2"]), ["test.testMetric3"])
        self.assertEqual(self._get_names(self.bundle["action3"]), [])
