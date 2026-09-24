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


"""Signal to Butler Writer that the previous day_obs has ended.

This script tells butler-writer-service that no more outputs will be
produced for the previous day_obs.
"""

__all__ = ["main", "make_parser"]


import argparse
import logging
import os
import sys

import astropy.time
import confluent_kafka

from activator.kafka_butler_writer import KafkaButlerWriter
from shared.astropy import import_iers_cache
from shared.config import PipelinesConfig
from shared.logger import setup_usdf_logger
from shared import run_utils


_log = logging.getLogger("lsst." + __name__)
_log.setLevel(logging.DEBUG)


def make_parser():
    parser = argparse.ArgumentParser()
    # Instrument, repo, pipelines configs, and butler writer configs submitted
    # through envvars to parallel the activator.
    parser.add_argument(
        "--day-obs",
        required=False,
        type=int,
        help="Override the day_obs (YYYYMMDD) being signaled as ended. By "
        "default, the previous day_obs is used.",
    )
    return parser


def _get_previous_day_obs() -> int:
    """Generate the day_obs value for the previous day.

    Returns
    -------
    day_obs : `int`
        The previous day_obs value in YYYYMMDD format.
    """
    yesterday = astropy.time.Time.now() - astropy.time.TimeDelta(1, format="jd")
    return run_utils.get_day_obs(yesterday)


def main(args=None):
    """Notify butler-writer-service that a day_obs has ended.

    Parameters
    ----------
    args : `list` [`str`]
        The command-line arguments for this program. Defaults to `sys.argv`.
    """
    instrument_name = os.environ["RUBIN_INSTRUMENT"]
    setup_usdf_logger(labels={"instrument": instrument_name},)
    try:
        import_iers_cache()  # Should not be needed, but best to be consistent

        parsed = make_parser().parse_args(args)
        # The main pipelines to execute and the conditions in which to choose them.
        main_pipelines = PipelinesConfig.from_yaml(os.environ["MAIN_PIPELINES_CONFIG"])

        previous_day_obs = parsed.day_obs or _get_previous_day_obs()
        surveys = list(main_pipelines.get_all_surveys())

        if os.environ.get("USE_KAFKA_BUTLER_WRITER", "0") == "1":
            repo = os.environ["CENTRAL_REPO"]
            producer = confluent_kafka.Producer(
                {
                    "bootstrap.servers": os.environ["BUTLER_WRITER_KAFKA_CLUSTER"],
                    "security.protocol": "sasl_plaintext",
                    "sasl.mechanism": "SCRAM-SHA-512",
                    "sasl.username": os.environ["BUTLER_WRITER_KAFKA_USERNAME"],
                    "sasl.password": os.environ["BUTLER_WRITER_KAFKA_PASSWORD"],
                }
            )
            writer = KafkaButlerWriter(
                producer,
                output_topic=os.environ["BUTLER_WRITER_KAFKA_TOPIC"],
                output_repo=repo,
            )
            writer.send_day_end(
                instrument_name, previous_day_obs, surveys, run_utils.get_lsst_distrib_version()
            )
            _log.info(
                "Sent day-end signal for %s day_obs %d.",
                instrument_name,
                previous_day_obs,
            )
        else:
            _log.info(
                "USE_KAFKA_BUTLER_WRITER not set, skipping day-end signal for %s day_obs %d.",
                instrument_name,
                previous_day_obs,
            )
        return 0
    except Exception:
        _log.exception("Failed to send day-end signal for %s.", instrument_name)
        return 1


if __name__ == "__main__":
    sys.exit(main())
