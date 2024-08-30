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

__all__ = ["next_visit_handler"]

import collections.abc
import contextlib
import json
import logging
import os
import sys
import time
import signal
import uuid

import boto3
from botocore.handlers import validate_bucket_name
import cloudevents.http
import confluent_kafka as kafka
from flask import Flask, request
from werkzeug.exceptions import ServiceUnavailable

from .config import PipelinesConfig
from .exception import GracefulShutdownInterrupt, NonRetriableError, RetriableError
from .logger import setup_usdf_logger
from .middleware_interface import get_central_butler, flush_local_repo, \
    make_local_repo, make_local_cache, MiddlewareInterface
from .raw import (
    check_for_snap,
    is_path_consistent,
    get_group_id_from_oid,
)
from .visit import FannedOutVisit

PROJECT_ID = "prompt-processing"

# The short name for the instrument.
instrument_name = os.environ["RUBIN_INSTRUMENT"]
# The skymap to use in the central repo
skymap = os.environ["SKYMAP"]
# URI to the main repository containing calibs and templates
calib_repo = os.environ["CALIB_REPO"]
# S3 Endpoint for Buckets; needed for direct Boto access but not Butler
s3_endpoint = os.environ["S3_ENDPOINT_URL"]
# URI of raw image microservice
raw_microservice = os.environ.get("RAW_MICROSERVICE", "")
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


def _graceful_shutdown(signum: int, stack_frame):
    """Signal handler for cases where the service should gracefully shut down.

    Parameters
    ----------
    signum : `int`
        The signal received.
    stack_frame : `frame` or `None`
        The "current" stack frame.

    Raises
    ------
    GracefulShutdownInterrupt
        Raised unconditionally.
    """
    signame = signal.Signals(signum).name
    _log.info("Signal %s detected, cleaning up and shutting down.", signame)
    # TODO DM-45339: raising in signal handlers is dangerous; can we get a way
    # for pipeline processing to check for interrupts?
    raise GracefulShutdownInterrupt(f"Received signal {signame}.")


def with_signal(signum: int,
                handler: collections.abc.Callable | signal.Handlers,
                ) -> collections.abc.Callable:
    """A decorator that registers a signal handler for the duration of a
    function call.

    Parameters
    ----------
    signum : `int`
        The signal for which to register a handler; see `signal.signal`.
    handler : callable or `signal.Handlers`
        The handler to register.
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            old_handler = signal.signal(signum, handler)
            try:
                return func(*args, **kwargs)
            finally:
                if old_handler is not None:
                    signal.signal(signum, old_handler)
                else:
                    signal.signal(signum, signal.SIG_DFL)
        return wrapper
    return decorator


def parse_next_visit(http_request):
    """Parse a next_visit event and extract its data.

    Parameters
    ----------
    http_request : `flask.Request`
        The request to be parsed.

    Returns
    -------
    next_visit : `activator.visit.FannedOutVisit`
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
    visit = FannedOutVisit(**data)
    _log.debug("Unpacked message as %r.", visit)
    return visit


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

    Do not return notifications from "sidecar" JSON files.

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
            key = record["s3"]["object"]["key"]
            if not key.endswith(".json"):
                yield key
        except KeyError as e:
            _log.error("Invalid S3 bucket notification: %s", e)


def _try_export(mwi: MiddlewareInterface, exposures: set[int], log: logging.Logger) -> bool:
    """Attempt to export pipeline products, logging any failure.

    This method is designed to be safely run from within exception handlers.

    Returns
    -------
    exported : `bool`
        `True` if the export was successful, `False` for a (possibly partial)
        failure.
    """
    try:
        mwi.export_outputs(exposures)
        return True
    except Exception:
        log.exception("Central repo export failed. Some output products may be lost.")
        return False


@app.route("/next-visit", methods=["POST"])
@with_signal(signal.SIGHUP, _graceful_shutdown)
@with_signal(signal.SIGTERM, _graceful_shutdown)
def next_visit_handler() -> tuple[str, int]:
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
    with contextlib.ExitStack() as cleanups:
        # Want to know when the handler exited for any reason.
        cleanups.callback(_log.info, "next_visit handling completed.")
        consumer.subscribe([bucket_topic])
        cleanups.callback(consumer.unsubscribe)
        _log.debug(f"Created subscription to '{bucket_topic}'")
        # Try to get a message right away to minimize race conditions
        startup_response = consumer.consume(num_messages=1, timeout=0.001)

        try:
            try:
                expected_visit = parse_next_visit(request)
            except ValueError as msg:
                _log.warn(f"error: '{msg}'")
                return f"Bad Request: {msg}", 400
            assert expected_visit.instrument == instrument_name, \
                f"Expected {instrument_name}, received {expected_visit.instrument}."
            if not main_pipelines.get_pipeline_files(expected_visit):
                _log.info(f"No pipeline configured for {expected_visit}, skipping.")
                return "No pipeline configured for the received visit.", 422

            log_factory = logging.getLogRecordFactory()
            with log_factory.add_context(group=expected_visit.groupId,
                                         survey=expected_visit.survey,
                                         detector=expected_visit.detector,
                                         ):
                try:
                    expid_set = set()

                    # Create a fresh MiddlewareInterface object to avoid accidental
                    # "cross-talk" between different visits.
                    mwi = MiddlewareInterface(central_butler,
                                              image_bucket,
                                              expected_visit,
                                              pre_pipelines,
                                              main_pipelines,
                                              skymap,
                                              local_repo.name,
                                              local_cache)
                    # TODO: pipeline execution requires a clean run until DM-38041.
                    cleanups.callback(mwi.clean_local_repo, expid_set)
                    # Copy calibrations for this detector/visit
                    mwi.prep_butler()

                    # expected_visit.nimages == 0 means "not known in advance"; keep listening until timeout
                    expected_snaps = expected_visit.nimages if expected_visit.nimages else 100
                    # Heuristic: take the upcoming script's duration and multiply by 2 to
                    # include the currently executing script, then add time to transfer
                    # the last image.
                    timeout = expected_visit.duration * 2 + image_timeout
                    # Check to see if any snaps have already arrived
                    for snap in range(expected_snaps):
                        oid = check_for_snap(
                            storage_client,
                            image_bucket,
                            raw_microservice,
                            expected_visit.instrument,
                            expected_visit.groupId,
                            snap,
                            expected_visit.detector,
                        )
                    if oid:
                        _log.debug("Found object %s already present", oid)
                        exp_id = mwi.ingest_image(oid)
                        expid_set.add(exp_id)

                    _log.debug("Waiting for snaps...")
                    start = time.time()
                    while len(expid_set) < expected_snaps and time.time() - start < timeout:
                        if startup_response:
                            response = startup_response
                        else:
                            time_remaining = max(0.0, timeout - (time.time() - start))
                            response = consumer.consume(num_messages=1, timeout=time_remaining + 1.0)
                        end = time.time()
                        messages = _filter_messages(response)
                        response = []
                        if len(messages) == 0 and end - start < timeout and not startup_response:
                            _log.debug(f"Empty consume after {end - start}s.")
                            continue
                        startup_response = []

                        # Not all notifications are for this group/detector
                        for received in messages:
                            for oid in _parse_bucket_notifications(received.value()):
                                try:
                                    if is_path_consistent(oid, expected_visit):
                                        _log.debug("Received %r", oid)
                                        group_id = get_group_id_from_oid(oid)
                                        if group_id == expected_visit.groupId:
                                            # Ingest the snap
                                            exp_id = mwi.ingest_image(oid)
                                            expid_set.add(exp_id)
                                except ValueError:
                                    _log.error(f"Failed to match object id '{oid}'")
                            # Commits are per-group, so this can't interfere with other
                            # workers. This may wipe messages associated with a next_visit
                            # that will later be assigned to this worker, but those cases
                            # should be caught by the "already arrived" check.
                            consumer.commit(message=received)
                    if len(expid_set) < expected_snaps:
                        _log.warning(f"Timed out waiting for image after receiving exposures {expid_set}.")
                except GracefulShutdownInterrupt as e:
                    _log.exception("Processing interrupted before pipeline execution")
                    # Do not export, to leave room for the next attempt
                    # Service unavailable is not quite right, but no better standard response
                    raise ServiceUnavailable(f"The server aborted processing, but it can be retried: {e}",
                                             retry_after=10) from None

                if expid_set:
                    with log_factory.add_context(exposures=expid_set):
                        # Got at least some snaps; run the pipeline.
                        # If this is only a partial set, the processed results may still be
                        # useful for quality purposes.
                        # If nimages == 0, any positive number of snaps is OK.
                        if len(expid_set) < expected_visit.nimages:
                            _log.warning(f"Processing {len(expid_set)} snaps, "
                                         f"expected {expected_visit.nimages}.")
                        _log.info("Running pipeline...")
                        try:
                            mwi.run_pipeline(expid_set)
                            try:
                                # TODO: broadcast alerts here
                                mwi.export_outputs(expid_set)
                            except Exception as e:
                                raise NonRetriableError("APDB and possibly alerts or central repo modified") \
                                    from e
                        except RetriableError as e:
                            error = e.nested if e.nested else e
                            _log.error("Processing failed but can be retried: ", exc_info=error)
                            # Do not export, to leave room for the next attempt
                            # Service unavailable is not quite right, but no better standard response
                            raise ServiceUnavailable(f"A temporary error occurred during processing: {error}",
                                                     retry_after=10) from None
                        except NonRetriableError as e:
                            error = e.nested if e.nested else e
                            _log.error("Processing failed: ", exc_info=error)
                            _try_export(mwi, expid_set, _log)
                            return f"An error occurred during processing: {error}.\nThe system's state has " \
                                   "permanently changed, so this request should **NOT** be retried.", 500
                        except Exception as e:
                            _log.error("Processing failed: ", exc_info=e)
                            _try_export(mwi, expid_set, _log)
                            return f"An error occurred during processing: {e}.", 500
                        return "Pipeline executed", 200
                else:
                    _log.error("Timed out waiting for images.")
                    return "Timed out waiting for images", 500
        except GracefulShutdownInterrupt:
            # Safety net to minimize chance of interrupt propagating out of the worker.
            # Ideally, this would be a Flask.errorhandler, but Flask ignores BaseExceptions.
            _log.error("Service interrupted. Shutting down *without* syncing to the central repo.")
            return "The worker was interrupted before it could complete the request. " \
                   "Retrying the request may not be safe.", 500


@app.errorhandler(500)
def server_error(e) -> tuple[str, int]:
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
