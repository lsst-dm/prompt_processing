import base64
import json
import logging
import os
import re
import time
from typing import Optional, Tuple

from flask import current_app, Flask, request
from google.cloud import pubsub_v1, storage

from middleware_interface import MiddlewareInterface
from visit import Visit

PROJECT_ID = "prompt-proto"

verification_token = os.environ["PUBSUB_VERIFICATION_TOKEN"]
config_instrument = os.environ["RUBIN_INSTRUMENT"]
calib_repo = os.environ["CALIB_REPO"]
image_bucket = os.environ["IMAGE_BUCKET"]
oid_regexp = re.compile(r"(.*?)/(\d+)/(.*?)/(\d+)/\1-\3-\4-.*?-.*?-\2\.f")
timeout = os.environ.get("IMAGE_TIMEOUT", 50)

logging.basicConfig(
    # Use JSON format compatible with Google Cloud Logging
    format=(
        '{{"severity":"{levelname}", "labels":{{"instrument":"'
        + config_instrument
        + '"}}, "message":{message!r}}}'
    ),
    style="{",
)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
app = Flask(__name__)

subscriber = pubsub_v1.SubscriberClient()
topic_path = subscriber.topic_path(
    PROJECT_ID,
    f"{config_instrument}-image",
)
subscription = None

storage_client = storage.Client()

# Initialize middleware interface, including copying standard calibrations
mwi = MiddlewareInterface(calib_repo, image_bucket, config_instrument)


def check_for_snap(
    instrument: str, group: int, snap: int, detector: int
) -> Optional[str]:
    prefix = f"{instrument}/{detector}/{group}/{snap}/{instrument}-{group}-{snap}-"
    logger.debug(f"Checking for '{prefix}'")
    blobs = list(storage_client.list_blobs(image_bucket, prefix=prefix))
    if not blobs:
        return None
    elif len(blobs) > 1:
        logger.error(
            f"Multiple files detected for a single group/snap/detector: '{prefix}'"
        )
    return blobs[0]


@app.route("/next-visit", methods=["POST"])
def next_visit_handler() -> Tuple[str, int]:
    if request.args.get("token", "") != verification_token:
        return "Invalid request", 400
    subscription = subscriber.create_subscription(
        topic=topic_path,
        ack_deadline_seconds=60,
    )
    logger.debug(f"Created subscription '{subscription.name}'")
    try:
        envelope = request.get_json()
        if not envelope:
            msg = "no Pub/Sub message received"
            logging.warn(f"error: '{msg}'")
            return f"Bad Request: {msg}", 400

        if not isinstance(envelope, dict) or "message" not in envelope:
            msg = "invalid Pub/Sub message format"
            logging.warn(f"error: '{msg}'")
            return f"Bad Request: {msg}", 400

        payload = base64.b64decode(envelope["message"]["data"])
        data = json.loads(payload)
        expected_visit = Visit(**data)
        assert expected_visit.instrument == config_instrument
        snap_set = set()

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
                mwi.ingest_image(oid)
                snap_set.add(snap)

        logger.debug(
            "Waiting for snaps from group"
            f" '{expected_visit.group}' detector {expected_visit.detector}"
        )
        start = time.time()
        while len(snap_set) < expected_visit.snaps:
            response = subscriber.pull(
                subscription=subscription.name,
                max_messages=189 + 8 + 8,
                timeout=timeout,
            )
            end = time.time()
            if len(response.received_messages) == 0:
                if end - start < TIMEOUT:
                    logger.debug(
                        f"Empty pull after {end - start}"
                        f" for '{expected_visit.group}'"
                    )
                    continue
                logger.warning(
                    "Timed out waiting for image in"
                    f" '{expected_visit.group}' after receiving snaps {snap_set}"
                )
                break

            ack_list = []
            for received in response.received_messages:
                ack_list.append(received.ack_id)
                oid = received.message.attributes["objectId"]
                m = re.match(oid_regexp, oid)
                if m:
                    instrument, detector, group, snap = m.groups()
                    logger.debug(m.groups())
                    if (
                        instrument == expected_visit.instrument
                        and int(detector) == int(expected_visit.detector)
                        and group == str(expected_visit.group)
                        and int(snap) < int(expected_visit.snaps)
                    ):
                        # Ingest the snap
                        mwi.ingest_image(oid)
                        snap_set.add(snap)
                else:
                    logger.error(f"Failed to match object id '{oid}'")
            subscriber.acknowledge(subscription=subscription.name, ack_ids=ack_list)

        # Got all the snaps; run the pipeline
        mwi.run_pipeline(expected_visit, snap_set)
        return "Pipeline executed", 200
    finally:
        subscriber.delete_subscription(subscription=subscription.name)


@app.errorhandler(500)
def server_error(e) -> Tuple[str, int]:
    logger.exception("An error occurred during a request.")
    return (
        f"""
    An internal error occurred: <pre>{e}</pre>
    See logs for full stacktrace.
    """,
        500,
    )


if __name__ == "__main__":
    with subscriber:
        app.run(host="127.0.0.1", port=8080, debug=True)
