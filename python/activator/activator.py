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

__all__ = ["check_for_snap", "next_visit_handler"]

import base64
import json
import logging
import os
import time
from typing import Optional, Tuple

import boto3
from flask import Flask, request
from google.cloud import pubsub_v1

from .logger import setup_usdf_logger
from .make_pgpass import make_pgpass
from .middleware_interface import get_central_butler, MiddlewareInterface
from .raw import Snap
from .visit import Visit

PROJECT_ID = "prompt-proto"

# The short name for the instrument.
instrument_name = os.environ["RUBIN_INSTRUMENT"]
# URI to the main repository containing calibs and templates
calib_repo = os.environ["CALIB_REPO"]
# Bucket name (not URI) containing raw images
image_bucket = os.environ["IMAGE_BUCKET"]
# Time to wait for raw image upload, in seconds
timeout = os.environ.get("IMAGE_TIMEOUT", 50)
# Absolute path on this worker's system where local repos may be created
local_repos = os.environ.get("LOCAL_REPOS", "/tmp")

setup_usdf_logger(
    labels={"instrument": instrument_name},
)
_log = logging.getLogger("lsst." + __name__)
_log.setLevel(logging.DEBUG)


# Write PostgreSQL credentials.
# This MUST be done before creating a Butler or accessing the APDB.
make_pgpass()


app = Flask(__name__)

subscriber = pubsub_v1.SubscriberClient()
topic_path = subscriber.topic_path(
    PROJECT_ID,
    f"{instrument_name}-image",
)
subscription = None

storage_client = boto3.client('s3')

# Initialize middleware interface.
mwi = MiddlewareInterface(get_central_butler(calib_repo, instrument_name),
                          image_bucket,
                          instrument_name,
                          local_repos)


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
    prefix = f"{instrument}/{detector}/{group}/{snap}/"
    _log.debug(f"Checking for '{prefix}'")
    blobs = storage_client.list_objects_v2(Bucket=image_bucket, Prefix=prefix)['Contents']
    if not blobs:
        return None
    elif len(blobs) > 1:
        _log.error(
            f"Multiple files detected for a single detector/group/snap: '{prefix}'"
        )
    return blobs[0]['Key']


def parse_next_visit(http_request):
    """Parse a next_visit event and extract its data.

    Parameters
    ----------
    http_request : `flask.Request`
        The request to be parsed.

    Returns
    -------
    next_visit : `activator.visit.Visit`
        The next_visit message contained in the request.

    Raises
    ------
    ValueError
        Raised if ``http_request`` is not a valid message.
    """
    envelope = http_request.get_json()
    if not envelope:
        raise ValueError("no Pub/Sub message received")
    if not isinstance(envelope, dict) or "message" not in envelope:
        raise ValueError("invalid Pub/Sub message format")

    payload = base64.b64decode(envelope["message"]["data"])
    data = json.loads(payload)
    return Visit(**data)


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
    subscription = subscriber.create_subscription(
        topic=topic_path,
        ack_deadline_seconds=60,
    )
    _log.debug(f"Created subscription '{subscription.name}'")
    try:
        try:
            expected_visit = parse_next_visit(request)
        except ValueError as msg:
            _log.warn(f"error: '{msg}'")
            return f"Bad Request: {msg}", 400
        assert expected_visit.instrument == instrument_name, \
            f"Expected {instrument_name}, received {expected_visit.instrument}."
        expid_set = set()

        # Copy calibrations for this detector/visit
        mwi.prep_butler(expected_visit)

        # Check to see if any snaps have already arrived
        for snap in range(expected_visit.snaps):
            oid = check_for_snap(
                expected_visit.instrument,
                expected_visit.group,
                snap,
                expected_visit.detector,
            )
            if oid:
                raw_info = Snap.from_oid(oid)
                _log.debug("Found %r already present", raw_info)
                mwi.ingest_image(expected_visit, oid)
                expid_set.add(raw_info.exp_id)

        _log.debug(f"Waiting for snaps from {expected_visit}.")
        start = time.time()
        while len(expid_set) < expected_visit.snaps:
            response = subscriber.pull(
                subscription=subscription.name,
                max_messages=189 + 8 + 8,
                timeout=timeout,
            )
            end = time.time()
            if len(response.received_messages) == 0:
                if end - start < timeout:
                    _log.debug(f"Empty pull after {end - start}s for {expected_visit}.")
                    continue
                _log.warning(
                    f"Timed out waiting for image in {expected_visit} "
                    f"after receiving exposures {expid_set}"
                )
                break

            ack_list = []
            for received in response.received_messages:
                ack_list.append(received.ack_id)
                oid = received.message.attributes["objectId"]
                try:
                    raw_info = Snap.from_oid(oid)
                    _log.debug("Received %r", raw_info)
                    if raw_info.is_consistent(expected_visit):
                        # Ingest the snap
                        mwi.ingest_image(oid)
                        expid_set.add(raw_info.exp_id)
                except ValueError:
                    _log.error(f"Failed to match object id '{oid}'")
            subscriber.acknowledge(subscription=subscription.name, ack_ids=ack_list)

        if expid_set:
            # Got at least some snaps; run the pipeline.
            # If this is only a partial set, the processed results may still be
            # useful for quality purposes.
            if len(expid_set) < expected_visit.snaps:
                _log.warning(f"Processing {len(expid_set)} snaps, expected {expected_visit.snaps}.")
            _log.info(f"Running pipeline on {expected_visit}.")
            mwi.run_pipeline(expected_visit, expid_set)
            # TODO: broadcast alerts here
            # TODO: call export_outputs on success or permanent failure in DM-34141
            mwi.export_outputs(expected_visit, expid_set)
            return "Pipeline executed", 200
        else:
            _log.error(f"Timed out waiting for images for {expected_visit}.")
            return "Timed out waiting for images", 500
    finally:
        subscriber.delete_subscription(subscription=subscription.name)


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
    with subscriber:
        app.run(host="127.0.0.1", port=8080, debug=True)


if __name__ == "__main__":
    main()
