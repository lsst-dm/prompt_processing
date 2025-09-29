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

__all__ = []

import collections.abc
import contextlib
import functools
import json
import logging
import math
import os
import signal
import time
import uuid
import yaml

import astropy.time
import boto3
from botocore.handlers import validate_bucket_name
import confluent_kafka as kafka

from shared.config import PipelinesConfig
from shared.logger import logging_context
from shared.raw import (
    check_for_snap,
    is_path_consistent,
    get_exp_id_from_oid,
    get_group_id_from_oid,
)
from shared.visit import FannedOutVisit
from .exception import GracefulShutdownInterrupt, IgnorableVisit, \
    NonRetriableError, RetriableError
from .middleware_interface import get_central_butler, \
    make_local_repo, make_local_cache, MiddlewareInterface, ButlerWriter, DirectButlerWriter
from .kafka_butler_writer import KafkaButlerWriter
from .repo_tracker import LocalRepoTracker
from .setup import ServiceSetup

# The short name for the instrument.
instrument_name = os.environ["RUBIN_INSTRUMENT"]
# The skymap to use in the central repo
skymap = os.environ["SKYMAP"]
# URI to the main repository to contain processing results
write_repo = os.environ["CENTRAL_REPO"]
# URI to the main repository containing calibs and templates
read_repo = os.environ.get("READ_CENTRAL_REPO", write_repo)
# S3 Endpoint for Buckets; needed for direct Boto access but not Butler
s3_endpoint = os.environ["S3_ENDPOINT_URL"]
# URI of raw image microservice
raw_microservice = os.environ.get("RAW_MICROSERVICE", "")
# Bucket name (not URI) containing raw images
raw_image_bucket = os.environ["IMAGE_BUCKET"]
# Time to wait after expected script completion for image arrival, in seconds
image_timeout = int(os.environ.get("IMAGE_TIMEOUT", 20))
# Absolute path on this worker's system where local repos may be created
local_repo_space = os.environ.get("LOCAL_REPOS", "/tmp")
# Kafka server for raw notifications
notification_kafka_cluster = os.environ["KAFKA_CLUSTER"]
# Kafka group; must be worker-unique to keep workers from "stealing" messages for others.
notification_kafka_group_id = str(uuid.uuid4())
# The topic on which to listen to updates to raw_image_bucket
raw_bucket_topic = os.environ.get("BUCKET_TOPIC", "rubin-prompt-processing")
# Offset for Kafka bucket notification.
bucket_notification_kafka_offset_reset = os.environ.get("BUCKET_NOTIFICATION_KAFKA_OFFSET_RESET", "latest")

# If '1', sends outputs to a service for transfer into the central Butler
# repository instead of writing to the database directly.
use_kafka_butler_writer = os.environ.get("USE_KAFKA_BUTLER_WRITER", "0") == "1"
if use_kafka_butler_writer:
    # Hostname of the Kafka cluster used by the Butler writer.
    butler_writer_kafka_cluster = os.environ["BUTLER_WRITER_KAFKA_CLUSTER"]
    # Username for authentication to BUTLER_WRITER_KAFKA_CLUSTER.
    butler_writer_kafka_username = os.environ["BUTLER_WRITER_KAFKA_USERNAME"]
    # Password for authentication to BUTLER_WRITER_KAFKA_CLUSTER.
    butler_writer_kafka_password = os.environ["BUTLER_WRITER_KAFKA_PASSWORD"]
    # Topic used to transfer output datasets to the central repository.
    butler_writer_kafka_topic = os.environ["BUTLER_WRITER_KAFKA_TOPIC"]

_log = logging.getLogger("lsst." + __name__)
_log.setLevel(logging.DEBUG)


def _config_from_yaml(yaml_string):
    """Initialize a PipelinesConfig from a YAML-formatted string.

    Parameters
    ----------
    yaml_string : `str`
        A YAML representation of the structured config. See
        `~activator.config.PipelineConfig` for details.

    Returns
    -------
    config : `shared.config.PipelineConfig`
        The corresponding config object.
    """
    return PipelinesConfig(yaml.safe_load(yaml_string))


# The preprocessing pipelines to execute and the conditions in which to choose them.
pre_pipelines = _config_from_yaml(os.environ["PREPROCESSING_PIPELINES_CONFIG"])
# The main pipelines to execute and the conditions in which to choose them.
main_pipelines = _config_from_yaml(os.environ["MAIN_PIPELINES_CONFIG"])


@ServiceSetup.check_on_init
@functools.cache
def _get_notification_consumer():
    """Lazy initialization of Kafka Consumer for raw bucket notifications."""
    return kafka.Consumer({
        "bootstrap.servers": notification_kafka_cluster,
        "group.id": notification_kafka_group_id,
        "auto.offset.reset": bucket_notification_kafka_offset_reset,
    })


@functools.cache
def _get_butler_writer_producer():
    """Lazy initialization of Kafka Producer for Butler writer."""
    return kafka.Producer({
        "bootstrap.servers": butler_writer_kafka_cluster,
        "security.protocol": "sasl_plaintext",
        "sasl.mechanism": "SCRAM-SHA-512",
        "sasl.username": butler_writer_kafka_username,
        "sasl.password": butler_writer_kafka_password
    })


@ServiceSetup.check_on_init
@functools.cache
def _get_storage_client():
    """Lazy initialization of cloud storage reader."""
    storage_client = boto3.client('s3', endpoint_url=s3_endpoint)
    storage_client.meta.events.unregister("before-parameter-build.s3", validate_bucket_name)
    return storage_client


@functools.cache
def _get_write_butler():
    """Lazy initialization of central Butler for writes.
    """
    return get_central_butler(write_repo, instrument_name, writeable=True)


@ServiceSetup.check_on_init
@functools.cache
def _get_read_butler():
    """Lazy initialization of central Butler for reads.
    """
    if read_repo != write_repo:
        return get_central_butler(read_repo, instrument_name, writeable=False)
    else:
        # Don't initialize an extra Butler from scratch
        return _get_write_butler()


@ServiceSetup.check_on_init
@functools.cache
def _get_butler_writer() -> ButlerWriter:
    """Lazy initialization of Butler writer."""
    if use_kafka_butler_writer:
        return KafkaButlerWriter(
            _get_butler_writer_producer(),
            output_topic=butler_writer_kafka_topic,
            output_repo=write_repo
        )
    else:
        return DirectButlerWriter(_get_write_butler())


@ServiceSetup.check_on_init
@functools.cache
def _get_local_repo():
    """Lazy initialization of a new local repo.

    Returns
    -------
    repo : `tempfile.TemporaryDirectory`
        The directory containing the repo, to be removed when the
        process exits.
    """
    repo = make_local_repo(local_repo_space, _get_read_butler(), instrument_name)
    tracker = LocalRepoTracker.get()
    tracker.register(os.getpid(), repo.name)
    return repo


@functools.cache
def _get_local_cache():
    """Lazy initialization of local repo dataset cache."""
    return make_local_cache()


def time_since(start_time):
    """Calculates time since a reference timestamp.

    Parameters
    ----------
    start_time : `float`
        Time since a reference point, in seconds since Unix epoch.

    Returns
    -------
    duration : `float`
        Time in seconds since ``start_time``.
    """
    return time.time() - start_time


def is_processable(visit, expire) -> bool:
    """Test whether a nextVisit message should be processed, or rejected out
    of hand.

    This function emits explanatory logs as a side effect.

    Parameters
    ----------
    visit : `shared.visit.FannedOutVisit`
        The nextVisit message to consider processing.
    expire : `float`
        The maximum age, in seconds, that a message can still be handled.

    Returns
    -------
    handleable : `bool`
        `True` is the message can be processed, `False` otherwise.
    """
    # sndStamp is visit publication, in seconds since 1970-01-01 TAI
    # For expirations of a few minutes the TAI-UTC difference is significant!
    published = astropy.time.Time(visit.private_sndStamp, format="unix_tai").utc.unix
    age = round(time_since(published))  # Microsecond precision is distracting
    if age > expire:
        _log.warning("Message published on %s UTC is %s old, ignoring.",
                     time.ctime(published),
                     astropy.time.TimeDelta(age, format='sec').quantity_str
                     )
        return False
    return True


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
    activator.exception.GracefulShutdownInterrupt
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
        @functools.wraps(func)
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


def _ingest_existing_raws(expected_visit, expected_snaps, ingester, expid_set):
    """Check if any snaps have already arrived, and ingest raws if found.

    Parameters
    ----------
    expected_visit : `shared.visit.FannedOutVisit`
        The nextVisit being processed.
    expected_snaps : `int`
        The number of snaps to check for this visit.
    ingester : callable [(`str`), `int`]
        A callable that takes an S3 object key, ingests the image, and returns
        its exposure ID.
    expid_set : set [`int`]
        The IDs of already ingested exposures. This set is modified in place.
    """
    for snap in range(expected_snaps):
        oid = check_for_snap(
            _get_storage_client(),
            raw_image_bucket,
            raw_microservice,
            expected_visit.instrument,
            expected_visit.groupId,
            snap,
            expected_visit.detector,
        )
        if oid:
            _log.debug("Found object %s already present", oid)
            exp_id = get_exp_id_from_oid(oid)
            if exp_id not in expid_set:
                exp_id = ingester(oid)
                expid_set.add(exp_id)


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


def _consume_messages(messages, consumer, expected_visit, ingester, expid_set):
    """Parse the file arrival messages, ingest the matching images, and commit.

    Parameters
    ----------
    messages : `list` [`confluent_kafka.Message`]
        The messages to process and consume.
    consumer: `confluent_kafka.Consumer`
        The Kafka Consumer instance to commit the messages when done processing.
    expected_visit : `shared.visit.FannedOutVisit`
        The nextVisit being processed.
    ingester : callable [(`str`), `int`]
        A callable that takes an S3 object key, ingests the image, and returns
        its exposure ID.
    expid_set : set [`int`]
        The IDs of already ingested exposures. This set is modified in place.
    """
    # Not all notifications are for this group/detector.
    for received in messages:
        for oid in _parse_bucket_notifications(received.value()):
            try:
                if is_path_consistent(oid, expected_visit):
                    _log.debug("Received %r", oid)
                    group_id = get_group_id_from_oid(oid)
                    if group_id == expected_visit.groupId:
                        exp_id = get_exp_id_from_oid(oid)
                        if exp_id not in expid_set:
                            exp_id = ingester(oid)
                            expid_set.add(exp_id)
            except ValueError:
                _log.error(f"Failed to match object id '{oid}'")
        # Commits are per-group, so this can't interfere with other
        # workers. This may wipe messages associated with a next_visit
        # that will later be assigned to this worker, but those cases
        # should be caught by the "already arrived" check.
        consumer.commit(message=received)


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


def _filter_exposures(exposures, visit, get_skyangle):
    """Check exposures against the nextVisit and remove any that aren't safe
    to process.

    Parameters
    ----------
    exposures : collection [`int`]
        The exposures to filter.
    visit : `shared.visit.FannedOutVisit`
        The nextVisit for the exposures.
    get_skyangle : callable [(`int`), `astropy.coordinates.Angle`]
        A callable that takes an exposure ID and returns its rotation angle.
        Called on each element of ``exposures``; this function's caller is
        responsible for ensuring they meet any prerequisites.

    Returns
    -------
    filtered : set [`int`]
        The exposures to process.
    """
    to_drop = set()
    if visit.rotationSystem != FannedOutVisit.RotSys.NONE:
        expected_angle = visit.get_rotation_sky()
        for expid in exposures:
            angle = get_skyangle(expid)
            if angle is not None and not math.isnan(angle.degree):
                diff = (angle - expected_angle).wrap_at("180d")
                if abs(diff).degree > 1.0:
                    _log.warning("Exposure %d had sky rotation %.1f, expected %.1f. Discarding.",
                                 expid, angle.degree, expected_angle.degree)
                    to_drop.add(expid)
            else:
                _log.warning("Exposure %d is missing metadata. Discarding.", expid)
                to_drop.add(expid)
    return set(exposures) - to_drop


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


@with_signal(signal.SIGHUP, _graceful_shutdown)
@with_signal(signal.SIGTERM, _graceful_shutdown)
def process_visit(expected_visit: FannedOutVisit):
    """Prepare and run a pipeline on a nextVisit message.

    This function should not make any assumptions about the execution framework
    for the Prompt Processing system; in particular, it should not assume it is
    running on a web server.

    Parameters
    ----------
    expected_visit : `shared.visit.FannedOutVisit`
        The visit to process.

    Raises
    ------
    activator.exception.GracefulShutdownInterrupt
        Raised if the process was terminated at an unexpected point.
        Terminations during preprocessing or processing are chained by
        `~activator.exception.NonRetriableError` or
        `~activator.exception.RetriableError`, depending on the program state
        at the time.
    activator.exception.IgnorableVisit
        Raised if the service is configured to not process ``expected_visit``.
    activator.exception.InvalidVisitError
        Raised if ``expected_visit`` is not processable.
    activator.exception.NonRetriableError
        Raised if external resources (such as the APDB or alert stream) may
        have been left in a state that makes it unsafe to retry failures. This
        exception is always chained to another exception representing the
        original error.
    activator.exception.RetriableError
        Raised if the conditions for NonRetriableError are not met, *and*
        processing failed in a way that is expected to be transient. This
        exception is always chained to another exception representing the
        original error.
    """
    with contextlib.ExitStack() as cleanups:
        consumer = _get_notification_consumer()
        consumer.subscribe([raw_bucket_topic])
        cleanups.callback(consumer.unsubscribe)
        _log.debug(f"Created subscription to '{raw_bucket_topic}'")
        # Try to get a message right away to minimize race conditions
        startup_response = consumer.consume(num_messages=1, timeout=0.001)

        assert expected_visit.instrument == instrument_name, \
            f"Expected {instrument_name}, received {expected_visit.instrument}."
        if not main_pipelines.get_pipeline_files(expected_visit):
            raise IgnorableVisit(f"No pipeline configured for {expected_visit}.")

        with logging_context(group=expected_visit.groupId,
                             survey=expected_visit.survey,
                             detector=expected_visit.detector,
                             ):
            try:
                expid_set = set()

                # Create a fresh MiddlewareInterface object to avoid accidental
                # "cross-talk" between different visits.
                mwi = MiddlewareInterface(_get_read_butler(),
                                          _get_butler_writer(),
                                          raw_image_bucket,
                                          expected_visit,
                                          pre_pipelines,
                                          main_pipelines,
                                          skymap,
                                          _get_local_repo().name,
                                          _get_local_cache())
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
                _ingest_existing_raws(expected_visit, expected_snaps, mwi.ingest_image, expid_set)

                _log.debug("Waiting for snaps...")
                start = time.time()
                while len(expid_set) < expected_snaps and time_since(start) < timeout:
                    if startup_response:
                        response = startup_response
                    else:
                        time_remaining = max(0.0, timeout - time_since(start))
                        response = consumer.consume(num_messages=1, timeout=time_remaining + 1.0)
                    end = time.time()
                    messages = _filter_messages(response)
                    response = []
                    if len(messages) == 0 and end - start < timeout and not startup_response:
                        _log.debug(f"Empty consume after {end - start}s.")
                        continue
                    startup_response = []

                    _consume_messages(messages, consumer, expected_visit, mwi.ingest_image, expid_set)
                if len(expid_set) < expected_snaps:
                    _log.debug("Received %d out of %d expected snaps. Check again.",
                               len(expid_set), expected_snaps)
                    # Retry in case of race condition with microservice.
                    _ingest_existing_raws(expected_visit, expected_snaps, mwi.ingest_image, expid_set)
                if len(expid_set) < expected_snaps:
                    _log.warning(f"Timed out waiting for image after receiving exposures {expid_set}.")
            except GracefulShutdownInterrupt as e:
                raise RetriableError("Processing interrupted before pipeline execution") from e

            if not expid_set:
                raise RuntimeError("Timed out waiting for images.")
            # If nimages == 0, any positive number of snaps is OK.
            if len(expid_set) < expected_visit.nimages:
                _log.warning(f"Found {len(expid_set)} snaps, expected {expected_visit.nimages}.")

            expid_set = _filter_exposures(expid_set, expected_visit, mwi.get_observed_skyangle)
            if not expid_set:
                raise RuntimeError("All images rejected as unprocessable.")

            with logging_context(exposures=expid_set):
                # Got at least some snaps; run the pipeline.
                # If this is only a partial set, the processed results may still be
                # useful for quality purposes.
                _log.info("Running main pipeline...")
                try:
                    mwi.run_pipeline(expid_set)
                except RetriableError:
                    # Do not export, to leave room for the next attempt
                    raise
                except Exception:
                    _try_export(mwi, expid_set, _log)
                    raise
                else:
                    try:
                        mwi.export_outputs(expid_set)
                    except Exception as e:
                        raise NonRetriableError("APDB and possibly alerts or central repo modified") \
                            from e
