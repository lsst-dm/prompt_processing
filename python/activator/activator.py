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

import json
import logging
import os
import time
from typing import Optional, Tuple
import uuid

import boto3
import cloudevents.http
import confluent_kafka as kafka
from flask import Flask, request

from .logger import setup_usdf_logger
from .make_pgpass import make_pgpass
from .middleware_interface import get_central_butler, MiddlewareInterface
from .raw import Snap
from .visit import Visit

PROJECT_ID = "prompt-proto"

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
# Time to wait for raw image upload, in seconds
timeout = os.environ.get("IMAGE_TIMEOUT", 50)
# Absolute path on this worker's system where local repos may be created
local_repos = os.environ.get("LOCAL_REPOS", "/tmp")
# Kafka server
kafka_cluster = os.environ["KAFKA_CLUSTER"]
# Kafka group; must be worker-unique to keep workers from "stealing" messages for others.
kafka_group_id = str(uuid.uuid4())
# The topic on which to listen to updates to image_bucket
# bucket_topic = f"{instrument_name}-image"
bucket_topic = "rubin-prompt-processing"

setup_usdf_logger(
    labels={"instrument": instrument_name},
)
_log = logging.getLogger("lsst." + __name__)
_log.setLevel(logging.DEBUG)


# Write PostgreSQL credentials.
# This MUST be done before creating a Butler or accessing the APDB.
make_pgpass()


app = Flask(__name__)

consumer = kafka.Consumer({
    "bootstrap.servers": kafka_cluster,
    "group.id": kafka_group_id,
})

storage_client = boto3.client('s3', endpoint_url=s3_endpoint)

# Initialize middleware interface.
mwi = MiddlewareInterface(get_central_butler(calib_repo, instrument_name),
                          image_bucket,
                          instrument_name,
                          skymap,
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
    response = storage_client.list_objects_v2(Bucket=image_bucket, Prefix=prefix)
    if response["KeyCount"] == 0:
        return None
    elif response["KeyCount"] > 1:
        _log.error(
            f"Multiple files detected for a single detector/group/snap: '{prefix}'"
        )
    # Contents only exists if >0 objects found.
    return response["Contents"][0]['Key']


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
    event = cloudevents.http.from_http(http_request.headers, http_request.get_data())
    if not event:
        raise ValueError("no CloudEvent received")
    if not event.data:
        raise ValueError("empty CloudEvent received")

    # Message format is determined by the nextvisit-start deployment.
    data = json.loads(event.data)
    return Visit(**data)


def _filter_messages(messages):
    """Split Kafka output into proper messages and errors.

    This function returns the proper messages, and logs the errors.

    Parameters
    ----------
    messages : `list` [`confluent_kafka.Message`]
        The messages to filter.

    Returns
    -------
    cleaned_messages : `list` [`confluent_kafka.Message`]
        The received messages from within ``messages``.
    """
    cleaned = []
    for msg in messages:
        if msg.error():
            # TODO: not all error() are actually *errors*
            _log.warning("Consumer event: %s", msg.error())
        else:
            cleaned.append(msg)
    return cleaned


def _parse_bucket_notifications(payload):
    """Extract object IDs from an S3 notification.

    If one record is invalid, an error is logged but the function tries to
    process the remaining records.

    Parameters
    ----------
    payload : `str` or `bytes`
        A message payload containing S3 notifications.

    Yields
    ------
    oid : `str`
        The filename referred to by each message.
    """
    msg = json.loads(payload)
    for record in msg["Records"]:
        if not record["eventName"].startswith("ObjectCreated"):
            _log.warning("Unexpected non-creation notification in topic: %s", record)
            continue
        try:
            yield record["s3"]["object"]["key"]
        except KeyError as e:
            _log.error("Invalid S3 bucket notification: %s", e)


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

        # expected_visit.nimages == 0 means "not known in advance"; keep listening until timeout
        expected_snaps = expected_visit.nimages if expected_visit.nimages else 100
        # Check to see if any snaps have already arrived
        for snap in range(expected_snaps):
            oid = check_for_snap(
                expected_visit.instrument,
                expected_visit.groupId,
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
        while len(expid_set) < expected_snaps:
            response = consumer.consume(
                num_messages=189 + 8 + 8,
                timeout=timeout,
            )
            end = time.time()
            messages = _filter_messages(response)
            if len(messages) == 0:
                if end - start < timeout:
                    _log.debug(f"Empty consume after {end - start}s for {expected_visit}.")
                    continue
                _log.warning(
                    f"Timed out waiting for image in {expected_visit} "
                    f"after receiving exposures {expid_set}"
                )
                break

            for received in messages:
                for oid in _parse_bucket_notifications(received.value()):
                    try:
                        raw_info = Snap.from_oid(oid)
                        _log.debug("Received %r", raw_info)
                        if raw_info.is_consistent(expected_visit):
                            # Ingest the snap
                            mwi.ingest_image(expected_visit, oid)
                            expid_set.add(raw_info.exp_id)
                    except ValueError:
                        _log.error(f"Failed to match object id '{oid}'")
                # Commits are per-group, so this can't interfere with other
                # workers. This may wipe messages associated with a next_visit
                # that will later be assigned to this worker, but those cases
                # should be caught by the "already arrived" check.
                consumer.commit(message=received)

        if expid_set:
            # Got at least some snaps; run the pipeline.
            # If this is only a partial set, the processed results may still be
            # useful for quality purposes.
            # If nimages == 0, any positive number of snaps is OK.
            if len(expid_set) < expected_visit.nimages:
                _log.warning(f"Processing {len(expid_set)} snaps, expected {expected_visit.nimages}.")
            _log.info(f"Running pipeline on {expected_visit}.")
            mwi.run_pipeline(expected_visit, expid_set)
            # TODO: broadcast alerts here
            # TODO: call export_outputs on success or permanent failure in DM-34141
            mwi.export_outputs(expected_visit, expid_set)
            # Clean only if export successful.
            mwi.clean_local_repo(expected_visit, expid_set)
            return "Pipeline executed", 200
        else:
            _log.error(f"Timed out waiting for images for {expected_visit}.")
            return "Timed out waiting for images", 500
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
    app.run(host="127.0.0.1", port=8080, debug=True)


if __name__ == "__main__":
    main()
