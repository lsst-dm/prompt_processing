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


import functools
import logging
import os
import socket
import sys
import time
import uuid

import astropy.time
import prometheus_client as prometheus
import redis

from shared.astropy import import_iers_cache
from shared.logger import setup_usdf_logger, logging_context
from shared.run_utils import get_day_obs
from shared.visit import FannedOutVisit
from .activator import time_since, is_processable, process_visit
from .exception import GracefulShutdownInterrupt, TimeoutInterrupt, IgnorableVisit, \
    NonRetriableError, RetriableError
from .repo_tracker import LocalRepoTracker
from .setup import ServiceSetup

# The short name for the instrument.
instrument_name = os.environ["RUBIN_INSTRUMENT"]
# The time (in seconds) after which to ignore old nextVisit messages.
visit_expire = float(os.environ.get("MESSAGE_EXPIRATION", 3600))
# Max requests to handle before restarting. 0 means no limit.
max_worker_requests = int(os.environ.get("WORKER_RESTART_FREQ", 0))
# The number of seconds to delay retrying connections to the Redis stream.
redis_retry = float(os.environ.get("REDIS_RETRY_DELAY", 30))
# Time to wait for fanned out messages before spawning new pod.
fanned_out_msg_listen_timeout = int(os.environ.get("FANNED_OUT_MSG_LISTEN_TIMEOUT", 300))
# Redis Stream Cluster
fanout_redis_stream_host = os.environ["REDIS_STREAM_HOST"]
# Redis stream name to receive messages.
fanout_redis_stream_name = os.environ["REDIS_STREAM_NAME"]
# Redis streams group; must be worker-unique to keep workers from stealing messages for others.
fanout_redis_group_id = str(uuid.uuid4())
# Redis stream consumer group
fanout_redis_stream_consumer_group = os.environ["REDIS_STREAM_CONSUMER_GROUP"]

_log = logging.getLogger("lsst." + __name__)
_log.setLevel(logging.DEBUG)


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
        policy = redis.retry.Retry(redis.backoff.ConstantBackoff(redis_retry),
                                   1,
                                   # Bare ConnectionError covers things like DNS problems
                                   (redis.exceptions.ConnectionError, ),
                                   )
        return redis.Redis(host=self.host, retry=policy)

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
    return time_since(message_timestamp/1000.0)


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
        import_iers_cache()
        ServiceSetup.run_init_checks()

        redis_session = RedisStreamSession(
            fanout_redis_stream_host,
            fanout_redis_stream_name,
            fanout_redis_group_id,
            fanout_redis_stream_consumer_group,
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
            while (max_worker_requests <= 0 or consumer_polls_with_message < max_worker_requests) \
                    and (time_since(fan_out_listen_start_time) < fanned_out_msg_listen_timeout):

                # Ensure consistent day_obs for whole run, even if it crosses the boundary
                # This is needed for Grafana dashboards that compile statistics by day_obs
                with logging_context(day_obs=get_day_obs(astropy.time.Time.now())):
                    try:
                        redis_streams_message_id, fan_out_visit_decoded = redis_session.read_message()

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
                        consumer_polls_with_message += 1
                        if consumer_polls_with_message >= 1:
                            fan_out_listen_time = time_since(fan_out_listen_start_time)
                            _log.debug(
                                "Seconds since last redis streams message received %r for consumer poll %r",
                                fan_out_listen_time, consumer_polls_with_message)

                        handle_keda_visit(expected_visit)

                    # Reset timer for fan out message polling
                    _log.info("Starting next visit fan out event consumer poll")
                    fan_out_listen_start_time = time.time()

                if time_since(fan_out_listen_start_time) >= fanned_out_msg_listen_timeout:
                    _log.info("No messages received in %f seconds, shutting down.",
                              fanned_out_msg_listen_timeout)
                if max_worker_requests > 0 and consumer_polls_with_message >= max_worker_requests:
                    _log.info("Handled %d messages, shutting down.", consumer_polls_with_message)

        finally:
            # Assume only one worker per pod, don't need to remove local repo.
            _log.info("Finished listening for fanned out messages")


def handle_keda_visit(visit):
    """Process a next_visit message with error handling appropriate for Keda.

    Parameters
    ----------
    visit : `shared.visit.FannedOutVisit`
        The next_visit to handle.
    """
    processing_start = time.time()
    processing_result = "Unknown"

    try:
        process_visit(visit)
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
    except (Exception, TimeoutInterrupt):
        _log.exception("Processing failed:")
        processing_result = "Error"
    finally:
        _log.debug("Request took %.3f s. Result: %s",
                   time_since(processing_start), processing_result)
        _log.info("Processing completed for %s.", socket.gethostname())


def main():
    _log.info("starting keda instance")
    keda_start()


if __name__ == "__main__":
    main()
