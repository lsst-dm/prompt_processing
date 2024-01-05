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

__all__ = ["time_this_to_bundle"]


from contextlib import contextmanager

import lsst.verify


# This is a temporary workaround for the difficulty of handling timing metrics
# in analysis_tools. It will be removed once we have a fully compliant solution
# in place.
@contextmanager
def time_this_to_bundle(bundle, action_id, metric):
    """Time the enclosed block and record it as a measurement in a
    MetricMeasurementBundle.

    Parameters
    ----------
    bundle : `lsst.analysis.tools.interfaces.MetricMeasurementBundle`
        The bundle in which to store the measurement.
    action_id : `str`
        The analysis_tools-style "action" to declare for these metrics.
    metric : `str` or `lsst.verify.Name`
        The fully-qualified name for the metric.
    """
    metric_obj = lsst.verify.Metric(metric, f"Automatic timing for {metric}", "s")
    meas = lsst.verify.Measurement(metric_obj)
    try:
        with lsst.verify.timer.time_this_to_measurement(meas):
            yield
    finally:
        bundle.setdefault(action_id, []).append(meas)
