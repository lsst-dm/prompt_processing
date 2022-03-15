from dataclasses import dataclass
from google.cloud import pubsub_v1, storage
from google.oauth2 import service_account
import json
import logging
import random
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
    "HSC": Instrument(1, 112),
}
EXPOSURE_INTERVAL = 18
SLEW_INTERVAL = 2
FILTER_LIST = "ugrizy"
PUBSUB_TOKEN = "abc123"
KINDS = ("BIAS", "DARK", "FLAT")

PROJECT_ID = "prompt-proto"


def raw_path(instrument, detector, group, snap, exposure_id, filter):
    """The path on which to store raws in the raw bucket.

    This format is also assumed by ``activator/activator.py.``
    """
    return (
        f"{instrument}/{detector}/{group}/{snap}"
        f"/{instrument}-{group}-{snap}"
        f"-{exposure_id}-{filter}-{detector}.fz"
    )


logging.basicConfig(
    format="{levelname} {asctime} {name} - {message}",
    style="{",
)
_log = logging.getLogger("lsst." + __name__)
_log.setLevel(logging.DEBUG)


def process_group(publisher, bucket, visit_infos):
    """Simulate the observation of a single on-sky pointing.

    Parameters
    ----------
    publisher : `google.cloud.pubsub_v1.PublisherClient`
        The client that posts ``next_visit`` messages.
    bucket : `google.cloud.storage.Bucket`
        The bucket to which to transfer the raws, once observed.
    visit_infos : `set` [`activator.Visit`]
        The visit-detector combinations to be observed; each object may
        represent multiple snaps. Assumed to represent a single group, and to
        share instrument, snaps, filter, and kind.
    """
    # Assume group/snaps is shared among all visit_infos
    for info in visit_infos:
        group = info.group
        n_snaps = info.snaps
        break
    else:
        _log.info("No observations to make; aborting.")
        return

    send_next_visit(publisher, group, visit_infos)
    for snap in range(n_snaps):
        _log.info(f"Taking group: {group} snap: {snap}")
        time.sleep(EXPOSURE_INTERVAL)
        for info in visit_infos:
            _log.info(f"Uploading group: {info.group} snap: {snap} filter: {info.filter} "
                      f"detector: {info.detector}")
            exposure_id = make_exposure_id(info.instrument, info.group, snap)
            fname = raw_path(info.instrument, info.detector, info.group, snap, exposure_id, info.filter)
            bucket.blob(fname).upload_from_string("Test")
            _log.info(f"Uploaded group: {info.group} snap: {snap} filter: {info.filter} "
                      f"detector: {info.detector}")


def send_next_visit(publisher, group, visit_infos):
    """Simulate the transmission of a ``next_visit`` message.

    Parameters
    ----------
    group : `str`
        The group ID for the message to send.
    visit_infos : `set` [`activator.Visit`]
        The visit-detector combinations to be sent; each object may
        represent multiple snaps.
    """
    _log.info(f"Sending next_visit for group: {group}")
    topic_path = publisher.topic_path(PROJECT_ID, "nextVisit")
    for info in visit_infos:
        _log.debug(f"Sending next_visit for group: {info.group} detector: {info.detector} "
                   f"filter: {info.filter} ra: {info.ra} dec: {info.dec} kind: {info.kind}")
        data = json.dumps(info.__dict__).encode("utf-8")
        publisher.publish(topic_path, data=data)


def make_exposure_id(instrument, group, snap):
    """Generate an exposure ID from an exposure's other metadata.

    The exposure ID is purely a placeholder, and does not conform to any
    instrument's rules for how exposure IDs should be generated.

    Parameters
    ----------
    instrument : `str`
        The short name of the instrument.
    group : `int`
        A group ID.
    snap : `int`
        A snap ID.

    Returns
    -------
    exposure : `int`
        An exposure ID that is likely to be unique for each combination of
        ``group`` and ``snap``, for a given ``instrument``.
    """
    exposure_id = (group // 100_000) * 100_000
    exposure_id += (group % 100_000) * INSTRUMENTS[instrument].n_snaps
    exposure_id += snap
    return exposure_id


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
    publisher = pubsub_v1.PublisherClient(credentials=credentials,
                                          batch_settings=batch_settings)

    last_group = get_last_group(storage_client, instrument, date)
    _log.info(f"Last group {last_group}")

    for i in range(n_groups):
        kind = KINDS[i % len(KINDS)]
        group = last_group + i + 1
        filter = FILTER_LIST[random.randrange(0, len(FILTER_LIST))]
        ra = random.uniform(0.0, 360.0)
        dec = random.uniform(-90.0, 90.0)
        visit_infos = {
            Visit(instrument, detector, group, INSTRUMENTS[instrument].n_snaps, filter, ra, dec, kind)
            for detector in range(INSTRUMENTS[instrument].n_detectors)
        }
        process_group(publisher, bucket, visit_infos)
        _log.info("Slewing to next group")
        time.sleep(SLEW_INTERVAL)


def get_last_group(storage_client, instrument, date):
    """Identify a group number that will not collide with any previous groups.

    Parameters
    ----------
    storage_client : `google.cloud.storage.Client`
        A Google Cloud Storage object pointing to the active project.
    instrument : `str`
        The short name of the active instrument.
    date : `str`
        The current date in YYYYMMDD format.

    Returns
    -------
    group : `int`
        The largest existing group for ``instrument``, or a newly generated
        group if none exist.
    """
    blobs = storage_client.list_blobs(
        "rubin-prompt-proto-main",
        prefix=f"{instrument}/0/{date}",
        delimiter="/",
    )
    # Contrary to the docs, blobs is not an iterator, but an iterable with a .prefixes member.
    for blob in blobs:
        # Iterate over blobs to get past `list_blobs`'s pagination and
        # fill .prefixes.
        pass
    prefixes = [int(prefix.split("/")[2]) for prefix in blobs.prefixes]
    if len(prefixes) == 0:
        return int(date) * 100_000
    else:
        return max(prefixes) + random.randrange(10, 19)


if __name__ == "__main__":
    main()
