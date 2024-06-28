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

__all__ = ["check_for_snap", "next_visit_handler"]

import logging
import os
import signal
import sys
import time
from typing import Optional, Tuple
import uuid

import boto3
from botocore.handlers import validate_bucket_name
import confluent_kafka as kafka
from flask import Flask, request

from .config import PipelinesConfig
from .logger import setup_usdf_logger
from .middleware_interface import get_central_butler, flush_local_repo, \
    make_local_repo, make_local_cache
from .raw import (
    get_prefix_from_snap,
)

PROJECT_ID = "prompt-processing"

# The short name for the instrument.
instrument_name = os.environ["RUBIN_INSTRUMENT"]
# The skymap to use in the central repo
skymap = os.environ["SKYMAP"]
# URI to the main repository containing calibs and templates
calib_repo = os.environ["CALIB_REPO"]
# S3 Endpoint for Buckets; needed for direct Boto access but not Butler
s3_endpoint = os.environ["S3_ENDPOINT_URL"]
# Bucket name (not URI) containing raw images
image_bucket = os.environ["IMAGE_BUCKET"]
# Time to wait after expected script completion for image arrival, in seconds
image_timeout = int(os.environ.get("IMAGE_TIMEOUT", 20))
# Absolute path on this worker's system where local repos may be created
local_repos = os.environ.get("LOCAL_REPOS", "/tmp")
# Kafka server
kafka_cluster = os.environ["KAFKA_CLUSTER"]
# Kafka group; must be worker-unique to keep workers from "stealing" messages for others.
kafka_group_id = str(uuid.uuid4())
# The topic on which to listen to updates to image_bucket
bucket_topic = os.environ.get("BUCKET_TOPIC", "rubin-prompt-processing")
# The preprocessing pipelines to execute and the conditions in which to choose them.
pre_pipelines = PipelinesConfig(os.environ["PREPROCESSING_PIPELINES_CONFIG"])
# The main pipelines to execute and the conditions in which to choose them.
main_pipelines = PipelinesConfig(os.environ["MAIN_PIPELINES_CONFIG"])

setup_usdf_logger(
    labels={"instrument": instrument_name},
)
_log = logging.getLogger("lsst." + __name__)
_log.setLevel(logging.DEBUG)


def find_local_repos(base_path):
    """Search for existing local repos.

    Parameters
    ----------
    base_path : `str`
        The directory in which to search for repos.

    Returns
    -------
    repos : collection [`str`]
        The root directories of any local repos found.
    """
    subdirs = {entry.path for entry in os.scandir(base_path) if entry.is_dir()}
    return {d for d in subdirs if os.path.exists(os.path.join(d, "butler.yaml"))}


try:
    app = Flask(__name__)

    consumer = kafka.Consumer({
        "bootstrap.servers": kafka_cluster,
        "group.id": kafka_group_id,
        "auto.offset.reset": "latest",  # default, but make explicit
    })

    storage_client = boto3.client('s3', endpoint_url=s3_endpoint)
    storage_client.meta.events.unregister("before-parameter-build.s3", validate_bucket_name)

    central_butler = get_central_butler(calib_repo, instrument_name)
    for old_repo in find_local_repos(local_repos):
        _log.warning("Orphaned repo found at %s, attempting to remove.", old_repo)
        flush_local_repo(old_repo, central_butler)
    # local_repo is a temporary directory with the same lifetime as this process.
    local_repo = make_local_repo(local_repos, central_butler, instrument_name)
    local_cache = make_local_cache()
except Exception as e:
    _log.critical("Failed to start worker; aborting.")
    _log.exception(e)
    # gunicorn assumes exit code 3 means "Worker failed to boot", though this is not documented
    sys.exit(3)


def check_for_snap(
    instrument: str, group: int, snap: int, detector: int
) -> Optional[str]:
    """Search for new raw files matching a particular data ID.

    The search is performed in the active image bucket.

    Parameters
    ----------
    instrument, group, snap, detector
        The data ID to search for.

    Returns
    -------
    name : `str` or `None`
        The raw's location in the active bucket, or `None` if no file
        was found. If multiple files match, this function logs an error
        but returns one of the files anyway.
    """
    prefix = get_prefix_from_snap(instrument, group, detector, snap)
    if not prefix:
        return None
    _log.debug(f"Checking for '{prefix}'")
    response = storage_client.list_objects_v2(Bucket=image_bucket, Prefix=prefix)
    if response["KeyCount"] == 0:
        return None
    elif response["KeyCount"] > 1:
        _log.error(
            f"Multiple files detected for a single detector/group/snap: '{prefix}'"
        )
    # Contents only exists if >0 objects found.
    return response["Contents"][0]['Key']


@app.route("/next-visit", methods=["POST"])
def next_visit_handler() -> Tuple[str, int]:
    """A Flask view function for handling next-visit events.

    Like all Flask handlers, this function accepts input through the
    ``request`` global rather than parameters.

    Returns
    -------
    message : `str`
        The HTTP response reason to return to the client.
    status : `int`
        The HTTP response status code to return to the client.
    """
    _log.info(f"Starting next_visit_handler for {request}.")
    consumer.subscribe([bucket_topic])
    _log.debug(f"Created subscription to '{bucket_topic}'")
    # Try to get a message right away to minimize race conditions
    consumer.consume(num_messages=1, timeout=0.001)

    try:
        # Do nothing until timeout sequence starts
        sig = signal.sigwait({signal.SIGTERM, signal.SIGABRT})
        _log.info(f"Signal {signal.Signals(sig).name} detected. Shutting down in 83 seconds...")

        # Simulate a yellow-light shutdown
        time.sleep(83.0)

        return "Processing completed at the last minute", 200
    finally:
        consumer.unsubscribe()
        # Want to know when the handler exited for any reason.
        _log.info("next_visit handling completed.")


@app.errorhandler(500)
def server_error(e) -> Tuple[str, int]:
    _log.exception("An error occurred during a request.")
    return (
        f"""
    An internal error occurred: <pre>{e}</pre>
    See logs for full stacktrace.
    """,
        500,
    )


def main():
    # This function is only called in test environments. Container
    # deployments call `app()` through Gunicorn.
    app.run(host="127.0.0.1", port=8080, debug=True)


if __name__ == "__main__":
    main()
else:
    _log.info("Worker ready to handle requests.")
