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

import tempfile
import unittest
import unittest.mock

import astropy.time

from initializer.signal_day_end import main, _get_previous_day_obs


class SignalDayEndTest(unittest.TestCase):
    def test_get_previous_day_obs(self):
        with unittest.mock.patch(
            "astropy.time.Time.now", return_value=astropy.time.Time("2026-01-01T15:00")
        ):
            self.assertEqual(_get_previous_day_obs(), 20251231)

    def _main_env(self, **overrides):
        env = {
            "RUBIN_INSTRUMENT": "LSSTCam",
            "MAIN_PIPELINES_CONFIG": "- survey: SURVEY\n  pipelines:\n  - /etc/pipelines/Main.yaml\n",
            "CENTRAL_REPO": "buter-repo",
        }
        env.update(overrides)
        return env

    def test_main_no_kafka(self):
        with tempfile.TemporaryDirectory() as central_repo:
            env = self._main_env(CENTRAL_REPO=central_repo)
            with (
                unittest.mock.patch.dict("os.environ", env, clear=True),
                unittest.mock.patch("initializer.signal_day_end.import_iers_cache"),
                unittest.mock.patch("confluent_kafka.Producer") as mock_producer,
                unittest.mock.patch(
                    "initializer.signal_day_end.KafkaButlerWriter"
                ) as mock_writer,
            ):
                result = main([])
        self.assertEqual(result, 0)
        mock_producer.assert_not_called()
        mock_writer.assert_not_called()

    def test_main_with_kafka(self):
        with tempfile.TemporaryDirectory() as central_repo:
            env = self._main_env(
                CENTRAL_REPO=central_repo,
                USE_KAFKA_BUTLER_WRITER="1",
                BUTLER_WRITER_KAFKA_CLUSTER="kafka:9092",
                BUTLER_WRITER_KAFKA_USERNAME="user",
                BUTLER_WRITER_KAFKA_PASSWORD="password",
                BUTLER_WRITER_KAFKA_TOPIC="topic-name",
            )
            with (
                unittest.mock.patch.dict("os.environ", env, clear=True),
                unittest.mock.patch("initializer.signal_day_end.import_iers_cache"),
                unittest.mock.patch("confluent_kafka.Producer"),
                unittest.mock.patch(
                    "initializer.signal_day_end.KafkaButlerWriter"
                ) as mock_writer,
                unittest.mock.patch(
                    "astropy.time.Time.now",
                    return_value=astropy.time.Time("2024-09-24T15:00"),
                ),
                unittest.mock.patch(
                    "shared.run_utils.get_lsst_distrib_version", return_value="g0123456789+0123456789"
                ),
            ):
                result = main([])
        self.assertEqual(result, 0)
        mock_writer.return_value.send_day_end.assert_called_once_with(
            "LSSTCam", 20240923, ["SURVEY"], "g0123456789+0123456789"
        )

    def test_main_with_kafka_day_obs_override(self):
        with tempfile.TemporaryDirectory() as central_repo:
            env = self._main_env(
                CENTRAL_REPO=central_repo,
                USE_KAFKA_BUTLER_WRITER="1",
                BUTLER_WRITER_KAFKA_CLUSTER="kafka:9092",
                BUTLER_WRITER_KAFKA_USERNAME="user",
                BUTLER_WRITER_KAFKA_PASSWORD="password",
                BUTLER_WRITER_KAFKA_TOPIC="topic-name",
            )
            with (
                unittest.mock.patch.dict("os.environ", env, clear=True),
                unittest.mock.patch("initializer.signal_day_end.import_iers_cache"),
                unittest.mock.patch("confluent_kafka.Producer"),
                unittest.mock.patch(
                    "initializer.signal_day_end.KafkaButlerWriter"
                ) as mock_writer,
                unittest.mock.patch(
                    "shared.run_utils.get_lsst_distrib_version", return_value="g0123456789+0123456789"
                ),
            ):
                result = main(["--day-obs", "20240901"])
        self.assertEqual(result, 0)
        mock_writer.return_value.send_day_end.assert_called_once_with(
            "LSSTCam", 20240901, ["SURVEY"], "g0123456789+0123456789"
        )
