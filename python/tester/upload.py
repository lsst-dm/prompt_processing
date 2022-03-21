from dataclasses import dataclass
from google.cloud import pubsub_v1, storage
from google.oauth2 import service_account
import itertools
import json
import logging
import random
import re
import sys
import time
from visit import Visit

from lsst.geom import SpherePoint, degrees


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


# TODO: unify the format code across prompt_prototype
RAW_REGEXP = re.compile(
    r"(?P<instrument>.*?)/(?P<detector>\d+)/(?P<group>.*?)/(?P<snap>\d+)/"
    r"(?P=instrument)-(?P=group)-(?P=snap)-(?P<expid>.*?)-(?P<filter>.*?)-(?P=detector)\.f"
)


logging.basicConfig(
    format="{levelname} {asctime} {name} - {message}",
    style="{",
)
_log = logging.getLogger("lsst." + __name__)
_log.setLevel(logging.DEBUG)


def process_group(publisher, visit_infos, uploader):
    """Simulate the observation of a single on-sky pointing.

    Parameters
    ----------
    publisher : `google.cloud.pubsub_v1.PublisherClient`
        The client that posts ``next_visit`` messages.
    visit_infos : `set` [`activator.Visit`]
        The visit-detector combinations to be observed; each object may
        represent multiple snaps. Assumed to represent a single group, and to
        share instrument, snaps, filter, and kind.
    uploader : callable [`activator.Visit`, int]
        A callable that takes an exposure spec and a snap ID, and uploads the
        visit's data.
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
    # TODO: need asynchronous code to handle next_visit delay correctly
    for snap in range(n_snaps):
        _log.info(f"Taking group: {group} snap: {snap}")
        time.sleep(EXPOSURE_INTERVAL)
        for info in visit_infos:
            _log.info(f"Uploading group: {info.group} snap: {snap} filter: {info.filter} "
                      f"detector: {info.detector}")
            uploader(info, snap)
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
    dest_bucket = storage_client.bucket("rubin-prompt-proto-main")
    batch_settings = pubsub_v1.types.BatchSettings(
        max_messages=INSTRUMENTS[instrument].n_detectors,
    )
    publisher = pubsub_v1.PublisherClient(credentials=credentials,
                                          batch_settings=batch_settings)

    last_group = get_last_group(storage_client, instrument, date)
    _log.info(f"Last group {last_group}")

    src_bucket = storage_client.bucket("rubin-prompt-proto-unobserved")
    raw_pool = get_samples(src_bucket, instrument)

    if raw_pool:
        _log.info(f"Observing real raw files from {instrument}.")
        upload_from_raws(publisher, instrument, raw_pool, src_bucket, dest_bucket, n_groups, last_group + 1)
    else:
        _log.info(f"No raw files found for {instrument}, generating dummy files instead.")
        upload_from_random(publisher, instrument, dest_bucket, n_groups, last_group + 1)


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
    preblobs = storage_client.list_blobs(
        "rubin-prompt-proto-main",
        prefix=f"{instrument}/",
        delimiter="/",
    )
    # See https://cloud.google.com/storage/docs/samples/storage-list-files-with-prefix
    for blob in preblobs:
        # Iterate over blobs to get past `list_blobs`'s pagination and
        # fill .prefixes.
        pass
    detector = min(int(prefix.split("/")[1]) for prefix in preblobs.prefixes)

    blobs = storage_client.list_blobs(
        "rubin-prompt-proto-main",
        prefix=f"{instrument}/{detector}/{date}",
        delimiter="/",
    )
    for blob in blobs:
        # Iterate over blobs to get past `list_blobs`'s pagination and
        # fill .prefixes.
        pass
    prefixes = [int(prefix.split("/")[2]) for prefix in blobs.prefixes]
    if len(prefixes) == 0:
        return int(date) * 100_000
    else:
        return max(prefixes) + random.randrange(10, 19)


def make_random_visits(instrument, group):
    """Create placeholder visits without reference to any data.

    Parameters
    ----------
    instrument : `str`
        The short name of the instrument carrying out the observation.
    group : `int`
        The group number being observed.

    Returns
    -------
    visits : `set` [`activator.Visit`]
        Visits generated for ``group`` for all ``instrument``'s detectors.
    """
    kind = KINDS[group % len(KINDS)]
    filter = FILTER_LIST[random.randrange(0, len(FILTER_LIST))]
    ra = random.uniform(0.0, 360.0) * degrees
    dec = random.uniform(-90.0, 90.0) * degrees
    rot = random.uniform(0.0, 360.0) * degrees
    return {
        Visit(instrument, detector, group, INSTRUMENTS[instrument].n_snaps, filter,
              SpherePoint(ra, dec), rot, kind)
        for detector in range(INSTRUMENTS[instrument].n_detectors)
    }


def get_samples(bucket, instrument):
    """Return any predefined raw exposures for a given instrument.

    Parameters
    ----------
    bucket : `google.cloud.storage.Bucket`
        The bucket in which to search for predefined raws.
    instrument : `str`
        The short name of the instrument to sample.

    Returns
    -------
    raws : mapping [`str`, mapping [`int`, mapping [`activator.Visit`, `google.cloud.storage.Blob`]]]
        A mapping from group IDs to a mapping of snap ID. The value of the
        innermost mapping is the observation metadata for each detector,
        and a Blob representing the image taken in that detector-snap.
    """
    # TODO: set up a lookup-friendly class to represent the return value

    # TODO: replace this dict with something more scalable.
    #     One option is to attach metadata to the Google Storage objects at
    #     upload time, another is to download the blob and actually read
    #     its header.
    hsc_metadata = {
        59150: {"ra": 149.96643749999996, "dec": 2.2202916666666668, "rot": 270.0},
        59160: {"ra": 150.18157499999998, "dec": 2.2800083333333334, "rot": 270.0},
    }

    blobs = bucket.client.list_blobs(bucket.name, prefix=instrument)
    result = {}
    for blob in blobs:
        # Assume that the unobserved bucket uses the same filename scheme as
        # the observed bucket.
        parsed = re.match(RAW_REGEXP, blob.name)
        if not parsed:
            _log.warning(f"Could not parse {blob.name}; ignoring file.")
            continue

        group = parsed.group('group')
        snap_id = int(parsed.group('snap'))
        exposure_id = int(parsed.group('expid'))
        visit = Visit(instrument=instrument,
                      detector=int(parsed.group('detector')),
                      group=group,
                      snaps=INSTRUMENTS[instrument].n_snaps,
                      filter=parsed.group('filter'),
                      boresight_center=SpherePoint(hsc_metadata[exposure_id]["ra"],
                                                   hsc_metadata[exposure_id]["dec"],
                                                   degrees),
                      orientation=hsc_metadata[exposure_id]["rot"] * degrees,
                      kind="SURVEY",
                      )
        _log.debug(f"File {blob.name} parsed as snap {snap_id} of visit {visit}.")
        if group in result:
            snap_dict = result[group]
            if snap_id in snap_dict:
                _log.debug(f"New detector {visit.detector} added to snap {snap_id} of group {group}.")
                detector_dict = snap_dict[snap_id]
                detector_dict[visit] = blob
            else:
                _log.debug(f"New snap {snap_id} added to group {group}.")
                snap_dict[snap_id] = {visit: blob}
        else:
            _log.debug(f"New group {group} registered.")
            result[group] = {snap_id: {visit: blob}}

    return result


def upload_from_raws(publisher, instrument, raw_pool, src_bucket, dest_bucket, n_groups, group_base):
    """Upload visits and files using real raws.

    Parameters
    ----------
    publisher : `google.cloud.pubsub_v1.PublisherClient`
        The client that posts ``next_visit`` messages.
    instrument : `str`
        The short name of the instrument carrying out the observation.
    raw_pool : mapping [`str`, mapping [`int`, mapping [`activator.Visit`, `google.cloud.storage.Blob`]]]
        Available raws as a mapping from group IDs to a mapping of snap ID.
        The value of the innermost mapping is the observation metadata for
        each detector, and a Blob representing the image taken in that
        detector-snap.
    src_bucket : `google.cloud.storage.Bucket`
        The bucket containing the blobs in ``raw_pool``.
    dest_bucket : `google.cloud.storage.Bucket`
        The bucket to which to upload the new images.
    n_groups : `int`
        The number of observation groups to simulate. If more than the number
        of groups in ``raw_pool``, files will be re-uploaded under new
        group IDs.
    group_base : `int`
        The base number from which to offset new group numbers.
    """
    for i, true_group in enumerate(itertools.islice(itertools.cycle(raw_pool), n_groups)):
        group = group_base + i
        _log.debug(f"Processing group {group} from unobserved {true_group}...")
        # snap_dict maps snap_id to {visit: blob}
        snap_dict = {}
        # Copy all the visit-blob dictionaries under each snap_id,
        # replacing the (immutable) Visit objects to point to group
        # instead of true_group.
        for snap_id, old_visits in raw_pool[true_group].items():
            snap_dict[snap_id] = {splice_group(true_visit, group): blob
                                  for true_visit, blob in old_visits.items()}
        # Gather all the Visit objects found in snap_dict, merging
        # duplicates for different snaps of the same detector.
        visit_infos = {info for det_dict in snap_dict.values() for info in det_dict}

        # TODO: may be cleaner to use a functor object than to depend on
        # closures for the bucket and data.
        def upload_from_pool(visit, snap_id):
            src_blob = snap_dict[snap_id][visit]
            exposure_id = make_exposure_id(visit.instrument, visit.group, snap_id)
            filename = raw_path(visit.instrument, visit.detector, visit.group, snap_id,
                                exposure_id, visit.filter)
            src_bucket.copy_blob(src_blob, dest_bucket, new_name=filename)
        process_group(publisher, visit_infos, upload_from_pool)
        _log.info("Slewing to next group")
        time.sleep(SLEW_INTERVAL)


def upload_from_random(publisher, instrument, dest_bucket, n_groups, group_base):
    """Upload visits and files using randomly generated visits.

    Parameters
    ----------
    publisher : `google.cloud.pubsub_v1.PublisherClient`
        The client that posts ``next_visit`` messages.
    instrument : `str`
        The short name of the instrument carrying out the observation.
    dest_bucket : `google.cloud.storage.Bucket`
        The bucket to which to upload the new images.
    n_groups : `int`
        The number of observation groups to simulate.
    group_base : `int`
        The base number from which to offset new group numbers.
    """
    for i in range(n_groups):
        group = group_base + i
        visit_infos = make_random_visits(instrument, group)

        # TODO: may be cleaner to use a functor object than to depend on
        # closures for the bucket and data.
        def upload_dummy(visit, snap_id):
            exposure_id = make_exposure_id(visit.instrument, visit.group, snap_id)
            filename = raw_path(visit.instrument, visit.detector, visit.group, snap_id,
                                exposure_id, visit.filter)
            dest_bucket.blob(filename).upload_from_string("Test")
        process_group(publisher, visit_infos, upload_dummy)
        _log.info("Slewing to next group")
        time.sleep(SLEW_INTERVAL)


def splice_group(visit, group):
    """Replace the group ID in a Visit object.

    Parameters
    ----------
    visit : `activator.Visit`
        The object to update.
    group : `str`
        The new group ID to use.

    Returns
    -------
    new_visit : `activator.Visit`
        A visit with group ``group``, but otherwise identical to ``visit``.
    """
    return Visit(instrument=visit.instrument,
                 detector=visit.detector,
                 group=group,
                 snaps=visit.snaps,
                 filter=visit.filter,
                 boresight_center=visit.boresight_center,
                 orientation=visit.orientation,
                 kind=visit.kind,
                 )


if __name__ == "__main__":
    main()
