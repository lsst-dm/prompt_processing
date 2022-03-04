import base64
from dataclasses import dataclass
from google.cloud import pubsub_v1, storage
from google.oauth2 import service_account
import json
import logging
import random
import subprocess
import sys
import time
from visit import Visit


@dataclass
class Instrument:
    n_snaps: int
    n_detectors: int


INSTRUMENTS = {
    "LSSTCam": Instrument(2, 189 + 8 + 8),
    "LSSTComCam": Instrument(2, 9),
    "LATISS": Instrument(1, 1),
    "DECam": Instrument(1, 62),
}
EXPOSURE_INTERVAL = 18
SLEW_INTERVAL = 2
FILTER_LIST = "ugrizy"
PUBSUB_TOKEN = "abc123"
KINDS = ("BIAS", "DARK", "FLAT")

PROJECT_ID = "prompt-proto"


logging.basicConfig(
    format="{levelname} {asctime} {name} - {message}",
    style="{",
    level=logging.DEBUG,
)

def process_group(publisher, bucket, instrument, group, filter, kind):
    n_snaps = INSTRUMENTS[instrument].n_snaps
    send_next_visit(publisher, instrument, group, n_snaps, filter, kind)
    for snap in range(n_snaps):
        logging.info(f"Taking group {group} snap {snap}")
        time.sleep(EXPOSURE_INTERVAL)
        for detector in range(INSTRUMENTS[instrument].n_detectors):
            logging.info(f"Uploading {group} {snap} {filter} {detector}")
            exposure_id = (group // 100000) * 100000
            exposure_id += (group % 100000) * INSTRUMENTS[instrument].n_snaps
            exposure_id += snap
            fname = (
                f"{instrument}/{detector}/{group}/{snap}"
                f"/{instrument}-{group}-{snap}"
                f"-{exposure_id}-{filter}-{detector}.fz"
            )
            bucket.blob(fname).upload_from_string("Test")
            logging.info(f"Uploaded {group} {snap} {filter} {detector}")


def send_next_visit(publisher, instrument, group, snaps, filter, kind):
    topic_path = publisher.topic_path(PROJECT_ID, "nextVisit")
    ra = random.uniform(0.0, 360.0)
    dec = random.uniform(-90.0, 90.0)
    process_list = []
    for detector in range(INSTRUMENTS[instrument].n_detectors):
        visit = Visit(instrument, detector, group, snaps, filter, ra, dec, kind)
        data = json.dumps(visit.__dict__).encode("utf-8")
        publisher.publish(topic_path, data=data)


def main():
    if len(sys.argv) < 3:
        print(f"Usage: {sys.argv[0]} INSTRUMENT N_GROUPS")
        sys.exit(1)
    instrument = sys.argv[1]
    n_groups = int(sys.argv[2])

    date = time.strftime("%Y%m%d")

    credentials = service_account.Credentials.from_service_account_file(
        "./prompt-proto-upload.json"
    )
    storage_client = storage.Client(PROJECT_ID, credentials=credentials)
    bucket = storage_client.bucket("rubin-prompt-proto-main")
    batch_settings = pubsub_v1.types.BatchSettings(
        max_messages=INSTRUMENTS[instrument].n_detectors,
    )
    publisher = pubsub_v1.PublisherClient(credentials=credentials)

    blobs = storage_client.list_blobs(
        "rubin-prompt-proto-main",
        prefix=f"{instrument}/0/{date}",
        delimiter="/",
    )
    for blob in blobs:
        pass
    prefixes = [int(prefix.split("/")[2]) for prefix in blobs.prefixes]
    if len(prefixes) == 0:
        last_group = int(date) * 100000
    else:
        last_group = max(prefixes) + random.randrange(10, 19)
    logging.info(f"Last group {last_group}")

    for i in range(n_groups):
        kind = KINDS[i % len(KINDS)]
        group = last_group + i + 1
        filter = FILTER_LIST[random.randrange(0, len(FILTER_LIST))]
        process_group(publisher, bucket, instrument, group, filter, kind)
        logging.info(f"Slewing to next group")
        time.sleep(SLEW_INTERVAL)

if __name__ == "__main__":
    main()
