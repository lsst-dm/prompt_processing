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
import functools
import json
import logging
import os
import signal
import socket
import sys
import time
import uuid
import yaml

import astropy.time
import boto3
from botocore.handlers import validate_bucket_name
import cloudevents.http
import confluent_kafka as kafka
import flask
import prometheus_client as prometheus
import redis

from shared.config import PipelinesConfig
from shared.logger import setup_usdf_logger
from shared.raw import (
    check_for_snap,
    is_path_consistent,
    get_group_id_from_oid,
)
from shared.visit import FannedOutVisit
from .exception import GracefulShutdownInterrupt, IgnorableVisit, InvalidVisitError, \
    NonRetriableError, RetriableError
from .middleware_interface import get_central_butler, \
    make_local_repo, make_local_cache, MiddlewareInterface
from .repo_tracker import LocalRepoTracker

# Platform that prompt processing will run on
platform = os.environ["PLATFORM"].lower()
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
# Kafka server for raw notifications
kafka_cluster = os.environ["KAFKA_CLUSTER"]
# Kafka group; must be worker-unique to keep workers from "stealing" messages for others.
kafka_group_id = str(uuid.uuid4())
# The time (in seconds) after which to ignore old nextVisit messages.
visit_expire = float(os.environ.get("MESSAGE_EXPIRATION", 3600))
# The topic on which to listen to updates to image_bucket
bucket_topic = os.environ.get("BUCKET_TOPIC", "rubin-prompt-processing")
# Offset for Kafka bucket notification.
bucket_notification_kafka_offset_reset = os.environ.get("BUCKET_NOTIFICATION_KAFKA_OFFSET_RESET", "latest")
# Max requests to handle before restarting. 0 means no limit.
max_requests = int(os.environ.get("WORKER_RESTART_FREQ", 0))

# Conditionally load keda environment variables
if platform == "keda":
    # Time to wait for fanned out messages before spawning new pod.
    fanned_out_msg_listen_timeout = int(os.environ.get("FANNED_OUT_MSG_LISTEN_TIMEOUT", 300))
    # Redis Stream Cluster
    redis_stream_host = os.environ["REDIS_STREAM_HOST"]
    # Redis stream name to receive messages.
    redis_stream_name = os.environ["REDIS_STREAM_NAME"]
    # Redis streams group; must be worker-unique to keep workers from stealing messages for others.
    redis_group_id = str(uuid.uuid4())
    # Redis stream consumer group
    redis_stream_consumer_group = os.environ["REDIS_STREAM_CONSUMER_GROUP"]

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


@functools.cache
def _get_consumer():
    """Lazy initialization of shared Kafka Consumer."""
    return kafka.Consumer({
        "bootstrap.servers": kafka_cluster,
        "group.id": kafka_group_id,
        "auto.offset.reset": bucket_notification_kafka_offset_reset,
    })


@functools.cache
def _get_storage_client():
    """Lazy initialization of cloud storage reader."""
    storage_client = boto3.client('s3', endpoint_url=s3_endpoint)
    storage_client.meta.events.unregister("before-parameter-build.s3", validate_bucket_name)
    return storage_client


@functools.cache
def _get_central_butler():
    """Lazy initialization of central Butler."""
    return get_central_butler(calib_repo, instrument_name)


@functools.cache
def _get_local_repo():
    """Lazy initialization of local repo.

    Returns
    -------
    repo : `tempfile.TemporaryDirectory`
        The directory containing the repo, to be removed when the
        process exits.
    """
    repo = make_local_repo(local_repos, _get_central_butler(), instrument_name)
    tracker = LocalRepoTracker.get()
    tracker.register(os.getpid(), repo.name)
    return repo


@functools.cache
def _get_local_cache():
    """Lazy initialization of local repo dataset cache."""
    return make_local_cache()


class RedisStreamSession:
    """The use of a single Redis Stream by a single consumer.

    A "session" may include multiple connections to Redis Streams. This object
    automatically opens connections as needed, and closes them when they are
    unsafe. Connections may also be closed manually by calling `close`.

    Parameters
    ----------
    host : `str`
        The address of the Redis Streams cluster.
    stream : `str`
        The name of the stream to listen to.
    consumer_id : `str`
        The unique Redis Streams consumer for this session.
    consumer_group : `str`
        The Redis Stream consumer group.
    connect : `bool`, optional
        Whether to connect to ``stream`` on construction. If `False`, the
        connection is deferred until a stream operation is needed.
    """

    # invariant: self.client is either an open Redis object, or `None`

    def __init__(self, host, stream, consumer_id, consumer_group, *, connect=True):
        self.host = host
        self.stream = stream
        self.groupname = consumer_group
        self.consumername = consumer_id
        self.client = None
        if connect:
            self._ensure_connection()

    def _make_redis_streams_client(self):
        """Create a new Redis client.

        Returns
        -------
        redis_client : `redis.Redis`
            Initialized Redis client.
        """
        return redis.Redis(host=self.host)

    @staticmethod
    def _close_on_error(func):
        """A decorator that closes the Redis client on a connection error.

        This is a safety measure: if the caller aborts, the client needs to be
        cleaned up; if they can recover, it's best to do so with a fresh
        connection.
        """
        @functools.wraps(func)
        def wrapper(self, *args, **kwargs):
            try:
                return func(self, *args, **kwargs)
            # TODO Review Redis Errors and determine what should be retriable.
            except redis.exceptions.RedisError:
                self.close()
                raise
        return wrapper

    @_close_on_error
    def _ensure_connection(self):
        """Check for a valid connection to the stream, opening a new one
        if necessary.

        After this method returns, ``self.client`` is guaranteed non-`None`.

        Exceptions
        ----------
        redis.exceptions.RedisError
            Raised if the client could not connect or an existing connection
            has gone bad.
        """
        if not self.client:
            self.client = self._make_redis_streams_client()
            _log.debug("Redis Streams client setup")
        self.client.ping()

    @_close_on_error
    def acknowledge(self, message_id):
        """Acknowledge receipt of a message.

        Parameters
        ----------
        message_id : `str`
            The message to acknowledge.
        """
        self._ensure_connection()
        self.client.xack(self.stream, self.groupname, message_id)

    def close(self):
        """Close the session's active connection, if it has one.

        This method is idempotent.
        """
        if self.client:
            self.client.close()
            self.client = None

    @_close_on_error
    def read_message(self):
        """Attempt to read one message from the stream.

        Returns
        -------
        message_id : `str` or `None`
            The Redis Streams message ID. `None` if no message was read.
        message : `dict` [`str`, `str`]
            The message contents. Empty if no message was read.

        Exceptions
        ----------
        redis.exceptions.RedisError
            Raised if the stream could not be read.
        ValueError
            Raised if a message was received but the message was invalid.
            Invalid messages may not be acknowledged (a message ID might not
            exist) and do not close the stream, even if ``close_on_receipt``
            is set.
        """
        self._ensure_connection()
        raw_message = self.client.xreadgroup(
            streams={self.stream: ">"},  # Read new messages (">" for pending messages)
            consumername=self.consumername,
            groupname=self.groupname,
            count=1  # Read one message at a time
        )

        if not raw_message:
            return None, {}
        else:
            return self._decode_redis_streams_message(raw_message)

    @staticmethod
    def _decode_redis_streams_message(fan_out_message):
        """Decode redis streams message from binary.

        Parameters
        ----------
        fan_out_message
            Fan out message, as a list of dicts.

        Returns
        -------
        redis_streams_message_id : `str`
            Redis streams message id decoded from bytes.
        fan_out_visit_decoded : `dict` [`str`, `str`]
            Fan out visit message decoded from bytes.

        Raises
        ------
        ValueError
            Raised if the message could not be decoded.
        """
        try:
            # Decode redis streams message id
            redis_streams_message_id = (fan_out_message[0][1][0][0]).decode("utf-8")
            # Decode and unpack fan out message from redis stream
            fan_out_visit_bytes = fan_out_message[0][1][0][1]
            fan_out_visit_decoded = {key.decode("utf-8"): value.decode("utf-8")
                                     for key, value in fan_out_visit_bytes.items()}
            return redis_streams_message_id, fan_out_visit_decoded
        except (LookupError, UnicodeError) as e:
            raise ValueError("Invalid redis stream message") from e


def _time_since(start_time):
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


def _calculate_time_since_fan_out_message_delivered(redis_streams_message_id):
    """Calculates time from fan out message to when message is unpacked
    in prompt processing.

    Parameters
    ----------
    redis_streams_message_id : `str`
        Fan out message ID. It includes the timestamp in milliseconds since
        Unix epoch suffixed with the message number.

    Returns
    -------
    fan_out_to_prompt_time : `float`
        Time in seconds from fan out message to when message is unpacked
        in prompt processing.
    """
    message_timestamp = float(redis_streams_message_id.split('-', 1)[0].strip())
    return _time_since(message_timestamp/1000.0)


def is_processable(visit, expire) -> bool:
    """Test whether a nextVisit message should be processed, or rejected out
    of hand.

    This function emits explanatory logs as a side effect.

    Parameters
    ----------
    visit : `FannedOutVisit`
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
    age = round(_time_since(published))  # Microsecond precision is distracting
    if age > expire:
        _log.warning("Message published on %s UTC is %s old, ignoring.",
                     time.ctime(published),
                     astropy.time.TimeDelta(age, format='sec').quantity_str
                     )
        return False
    return True


def create_app():
    try:
        setup_usdf_logger(
            labels={"instrument": instrument_name},
        )

        # Check initialization and abort early
        _get_consumer()
        _get_storage_client()
        _get_central_butler()
        _get_local_repo()

        app = flask.Flask(__name__)
        app.add_url_rule("/next-visit", view_func=next_visit_handler, methods=["POST"])
        app.register_error_handler(IgnorableVisit, skip_visit)
        app.register_error_handler(InvalidVisitError, invalid_visit)
        app.register_error_handler(RetriableError, request_retry)
        app.register_error_handler(NonRetriableError, forbid_retry)
        app.register_error_handler(500, server_error)
        _log.info("Worker ready to handle requests.")
        return app
    except Exception as e:
        _log.critical("Failed to start worker; aborting.")
        _log.exception(e)
        # gunicorn assumes exit code 3 means "Worker failed to boot", though this is not documented
        sys.exit(3)


def keda_start():

    try:
        setup_usdf_logger(
            labels={"instrument": instrument_name},
        )

        # Prometheus gauge setup
        instrument_name_gauge = instrument_name.lower().replace(" ", "_").replace("-", "_")
        instances_started_gauge = prometheus.Gauge(
            instrument_name_gauge + "_prompt_processing_instances_running",
            "prompt processing instances running with " + instrument_name_gauge + " as instrument"
        )
        instances_processing_gauge = prometheus.Gauge(
            instrument_name_gauge + "_prompt_processing_instances_processing",
            "instances performing prompt processing with " + instrument_name_gauge + " as instrument"
        )

        # Start Prometheus endpoint
        prometheus.start_http_server(8000)

        # Initialize local registry
        registry = LocalRepoTracker.get()
        registry.init_tracker()

        # Check initialization and abort early
        _get_consumer()
        _get_storage_client()
        _get_central_butler()
        _get_local_repo()

        redis_session = RedisStreamSession(
            redis_stream_host,
            redis_stream_name,
            redis_group_id,
            redis_stream_consumer_group,
            connect=True,
        )

        _log.info("Worker ready to handle requests.")

    except Exception as e:
        _log.critical("Failed to start worker; aborting.")
        _log.exception(e)
        sys.exit(1)

    fan_out_listen_start_time = time.time()
    consumer_polls_with_message = 0

    with instances_started_gauge.track_inprogress():
        try:
            while (max_requests <= 0 or consumer_polls_with_message < max_requests) \
                    and (_time_since(fan_out_listen_start_time) < fanned_out_msg_listen_timeout):

                try:
                    redis_streams_message_id, fan_out_visit_decoded = redis_session.read_message()
                    processing_start = time.time()
                    processing_result = "Unknown"

                    if not redis_streams_message_id:
                        continue

                    # TODO: Revisit acknowledgement policy for old messages once fan-out service exists
                    redis_session.acknowledge(redis_streams_message_id)

                    expected_visit = FannedOutVisit.from_dict(fan_out_visit_decoded)
                    _log.debug("Unpacked message as %r.", expected_visit)
                    if is_processable(expected_visit, visit_expire):
                        # Processing can take a long time, and long-lived connections are ill-behaved
                        redis_session.close()
                    else:
                        continue

                    # Calculate time to receive message based on timestamp in Redis Stream message
                    fan_out_to_prompt_time = _calculate_time_since_fan_out_message_delivered(
                        redis_streams_message_id)
                    _log.debug("Seconds since fan out message delivered %r", fan_out_to_prompt_time)

                # TODO Review Redis Errors and determine what should be retriable.
                except redis.exceptions.RedisError as e:
                    _log.critical("Redis Streams error; aborting.")
                    _log.exception(e)
                    sys.exit(1)
                except ValueError as e:
                    _log.error("Invalid redis stream message %s", e)
                    fan_out_listen_start_time = time.time()
                    continue

                with instances_processing_gauge.track_inprogress():
                    try:

                        consumer_polls_with_message += 1
                        if consumer_polls_with_message >= 1:
                            fan_out_listen_time = _time_since(fan_out_listen_start_time)
                            _log.debug(
                                "Seconds since last redis streams message received %r for consumer poll %r",
                                fan_out_listen_time, consumer_polls_with_message)

                        # Process fan out visit
                        process_visit(expected_visit)
                        processing_result = "Success"
                    except IgnorableVisit as e:
                        _log.info("Skipping visit: %s", e)
                        processing_result = "Ignore"
                    except GracefulShutdownInterrupt:
                        # Safety net to minimize chance of interrupt propagating out of the worker.
                        _log.error("Service interrupted.")
                        processing_result = "Interrupted"
                        sys.exit(1)
                    except RetriableError as e:
                        # TODO: need to implement retries for both cases
                        if isinstance(e.nested, GracefulShutdownInterrupt):
                            _log.error("Service interrupted.")
                            processing_result = "Interrupted"
                            sys.exit(1)
                        else:
                            _log.exception("Processing failed:")
                            processing_result = "Error"
                    except NonRetriableError as e:
                        if isinstance(e.nested, GracefulShutdownInterrupt):
                            _log.error("Service interrupted.")
                            processing_result = "Interrupted"
                            sys.exit(1)
                        else:
                            _log.exception("Processing failed:")
                            processing_result = "Error"
                    except Exception:
                        _log.exception("Processing failed:")
                        processing_result = "Error"
                    finally:
                        _log.debug("Request took %.3f s. Result: %s",
                                   _time_since(processing_start), processing_result)
                        _log.info("Processing completed for %s.", socket.gethostname())

                # Reset timer for fan out message polling
                _log.info("Starting next visit fan out event consumer poll")
                fan_out_listen_start_time = time.time()

            if _time_since(fan_out_listen_start_time) >= fanned_out_msg_listen_timeout:
                _log.info("No messages received in %f seconds, shutting down.", fanned_out_msg_listen_timeout)
            if max_requests > 0 and consumer_polls_with_message >= max_requests:
                _log.info("Handled %d messages, shutting down.", consumer_polls_with_message)

        finally:
            # Assume only one worker per pod, don't need to remove local repo.
            _log.info("Finished listening for fanned out messages")


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


def parse_next_visit(http_request):
    """Parse a next_visit event and extract its data.

    Parameters
    ----------
    http_request : `flask.Request`
        The request to be parsed.

    Returns
    -------
    next_visit : `shared.visit.FannedOutVisit`
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


def next_visit_handler() -> tuple[str, int]:
    """A Flask view function for handling next-visit events.

    Like all Flask handlers, this function accepts input through the
    ``flask.request`` global rather than parameters.

    Returns
    -------
    message : `str`
        The HTTP response reason to return to the client.
    status : `int`
        The HTTP response status code to return to the client.
    """
    _log.info(f"Starting next_visit_handler for {flask.request}.")

    try:
        try:
            expected_visit = parse_next_visit(flask.request)
        except ValueError as e:
            _log.exception("Bad Request")
            return f"Bad Request: {e}", 400
        if is_processable(expected_visit, visit_expire):
            process_visit(expected_visit)
            return "Pipeline executed", 200
        else:
            return "Stale request, ignoring", 403
    except GracefulShutdownInterrupt:
        # Safety net to minimize chance of interrupt propagating out of the worker.
        # Ideally, this would be a Flask.errorhandler, but Flask ignores BaseExceptions.
        _log.error("Service interrupted. Shutting down *without* syncing to the central repo.")
        return "The worker was interrupted before it could complete the request. " \
               "Retrying the request may not be safe.", 500
    finally:
        # Want to know when the handler exited for any reason.
        _log.info("next_visit handling completed.")


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
        consumer = _get_consumer()
        consumer.subscribe([bucket_topic])
        cleanups.callback(consumer.unsubscribe)
        _log.debug(f"Created subscription to '{bucket_topic}'")
        # Try to get a message right away to minimize race conditions
        startup_response = consumer.consume(num_messages=1, timeout=0.001)

        assert expected_visit.instrument == instrument_name, \
            f"Expected {instrument_name}, received {expected_visit.instrument}."
        if not main_pipelines.get_pipeline_files(expected_visit):
            raise IgnorableVisit(f"No pipeline configured for {expected_visit}.")

        log_factory = logging.getLogRecordFactory()
        with log_factory.add_context(group=expected_visit.groupId,
                                     survey=expected_visit.survey,
                                     detector=expected_visit.detector,
                                     ):
            try:
                expid_set = set()

                # Create a fresh MiddlewareInterface object to avoid accidental
                # "cross-talk" between different visits.
                mwi = MiddlewareInterface(_get_central_butler(),
                                          image_bucket,
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
                # Check to see if any snaps have already arrived
                for snap in range(expected_snaps):
                    oid = check_for_snap(
                        _get_storage_client(),
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
                raise RetriableError("Processing interrupted before pipeline execution") from e

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
                            mwi.export_outputs(expid_set)
                        except Exception as e:
                            raise NonRetriableError("APDB and possibly alerts or central repo modified") \
                                from e
                    except RetriableError:
                        # Do not export, to leave room for the next attempt
                        raise
                    except Exception:
                        _try_export(mwi, expid_set, _log)
                        raise
            else:
                raise RuntimeError("Timed out waiting for images.")


def invalid_visit(e: InvalidVisitError) -> tuple[str, int]:
    _log.exception("Invalid visit")
    return f"Cannot process visit: {e}.", 422


def skip_visit(e: IgnorableVisit) -> tuple[str, int]:
    _log.info("Skipping visit: %s", e)
    return f"Skipping visit without processing: {e}.", 422


def request_retry(e: RetriableError):
    error = e.nested if e.nested else e
    _log.error("Processing failed but can be retried: ", exc_info=error)
    # Service unavailable is not quite right, but no better standard response
    response = flask.make_response(
        f"The server could not process the request, but it can be retried: {error}",
        503,
        {"Retry-After": 30})
    return response


def forbid_retry(e: NonRetriableError) -> tuple[str, int]:
    error = e.nested if e.nested else e
    _log.error("Processing failed: ", exc_info=error)
    return f"An error occurred during processing: {error}.\nThe system's state has " \
           "permanently changed, so this request should **NOT** be retried.", 500


def server_error(e: Exception) -> tuple[str, int]:
    _log.exception("An error occurred during a request.")
    return (
        f"""
    An internal error occurred: <pre>{e}</pre>
    See logs for full stacktrace.
    """,
        500,
    )


def main():
    # Knative deployments call `create_app()()` through Gunicorn.
    # Keda deployments invoke main.
    if platform == "knative":
        _log.info("starting standalone Flask app")
        app = create_app()
        app.run(host="127.0.0.1", port=8080, debug=True)
    # starts keda instance of the application
    elif platform == "keda":
        _log.info("starting keda instance")
        keda_start()
    else:
        _log.info("no platform defined")


if __name__ == "__main__":
    main()
