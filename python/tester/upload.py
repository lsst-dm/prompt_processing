import dataclasses
import itertools
import logging
import os
import random
import sys
import tempfile
import time

import boto3

from activator.raw import Snap, get_raw_path
from activator.visit import FannedOutVisit, SummitVisit
from tester.utils import get_last_group, make_exposure_id, replace_header_key, send_next_visit


@dataclasses.dataclass
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

# Kafka server
kafka_cluster = os.environ["KAFKA_CLUSTER"]


logging.basicConfig(
    format="{levelname} {asctime} {name} - {message}",
    style="{",
)
_log = logging.getLogger("lsst." + __name__)
_log.setLevel(logging.INFO)


def process_group(kafka_url, visit_infos, uploader):
    """Simulate the observation of a single on-sky pointing.

    Parameters
    ----------
    kafka_url : `str`
        The URL of the Kafka REST Proxy to send ``next_visit`` messages to.
    visit_infos : `set` [`activator.FannedOutVisit`]
        The visit-detector combinations to be observed; each object may
        represent multiple snaps. Assumed to represent a single group, and to
        share instrument, nimages, filters, and survey.
    uploader : callable [`activator.FannedOutVisit`, int]
        A callable that takes an exposure spec and a snap ID, and uploads the
        visit's data.
    """
    # Assume group/snaps is shared among all visit_infos
    for info in visit_infos:
        group = info.groupId
        n_snaps = info.nimages
        visit = SummitVisit(**info.get_bare_visit())
        send_next_visit(kafka_url, group, {visit})
        break
    else:
        _log.info("No observations to make; aborting.")
        return

    # TODO: need asynchronous code to handle next_visit delay correctly
    for snap in range(n_snaps):
        _log.info(f"Taking group: {group} snap: {snap}")
        time.sleep(EXPOSURE_INTERVAL)
        for info in visit_infos:
            _log.info(f"Uploading group: {info.groupId} snap: {snap} filters: {info.filters} "
                      f"detector: {info.detector}")
            uploader(info, snap)
            _log.info(f"Uploaded group: {info.groupId} snap: {snap} filters: {info.filters} "
                      f"detector: {info.detector}")


def main():
    if len(sys.argv) < 3:
        print(f"Usage: {sys.argv[0]} INSTRUMENT N_GROUPS")
        sys.exit(1)
    instrument = sys.argv[1]
    n_groups = int(sys.argv[2])

    date = time.strftime("%Y%m%d")

    kafka_url = "https://usdf-rsp-dev.slac.stanford.edu/sasquatch-rest-proxy/topics/test.next-visit"
    endpoint_url = "https://s3dfrgw.slac.stanford.edu"
    s3 = boto3.resource("s3", endpoint_url=endpoint_url)
    dest_bucket = s3.Bucket("rubin-pp")

    last_group = get_last_group(dest_bucket, instrument, date)
    _log.info(f"Last group {last_group}")

    src_bucket = s3.Bucket("rubin-pp-users")
    raw_pool = get_samples(src_bucket, instrument)

    new_group_base = last_group + random.randrange(10, 19)
    if raw_pool:
        _log.info(f"Observing real raw files from {instrument}.")
        upload_from_raws(kafka_url, instrument, raw_pool, src_bucket, dest_bucket,
                         n_groups, new_group_base)
    else:
        _log.error(f"No raw files found for {instrument}, aborting.")


def get_samples(bucket, instrument):
    """Return any predefined raw exposures for a given instrument.

    Parameters
    ----------
    bucket : `S3.Bucket`
        The bucket in which to search for predefined raws.
    instrument : `str`
        The short name of the instrument to sample.

    Returns
    -------
    raws : mapping [`str`, mapping [`int`, mapping [`activator.FannedOutVisit`, `s3.ObjectSummary`]]]
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
        59126: {"ra": 149.28531249999997, "dec": 2.935002777777778, "rot": 270.0},
        59134: {"ra": 149.45749166666664, "dec": 2.926961111111111, "rot": 270.0},
        59138: {"ra": 149.45739166666664, "dec": 1.4269472222222224, "rot": 270.0},
        59142: {"ra": 149.4992083333333, "dec": 2.8853, "rot": 270.0},
        59150: {"ra": 149.96643749999996, "dec": 2.2202916666666668, "rot": 270.0},
        59152: {"ra": 149.9247333333333, "dec": 2.1577777777777776, "rot": 270.0},
        59154: {"ra": 150.22329166666663, "dec": 2.238341666666667, "rot": 270.0},
        59156: {"ra": 150.26497083333334, "dec": 2.1966694444444443, "rot": 270.0},
        59158: {"ra": 150.30668333333332, "dec": 2.2591888888888887, "rot": 270.0},
        59160: {"ra": 150.18157499999998, "dec": 2.2800083333333334, "rot": 270.0},
    }

    # The pre-made raw files are stored with the "unobserved" prefix
    blobs = bucket.objects.filter(Prefix=f"unobserved/{instrument}/")
    result = {}
    for blob in blobs:
        # Assume that the unobserved bucket uses the same filename scheme as
        # the observed bucket.
        snap = Snap.from_oid(blob.key)
        visit = FannedOutVisit(
            instrument=instrument,
            detector=snap.detector,
            groupId=snap.group,
            nimages=INSTRUMENTS[instrument].n_snaps,
            filters=snap.filter,
            coordinateSystem=FannedOutVisit.CoordSys.ICRS,
            position=[hsc_metadata[snap.exp_id]["ra"], hsc_metadata[snap.exp_id]["dec"]],
            rotationSystem=FannedOutVisit.RotSys.SKY,
            cameraAngle=hsc_metadata[snap.exp_id]["rot"],
            survey="SURVEY",
            # Fan-out uses salIndex to know which instrument and detector config to use.
            # The exp_id of this test dataset is coded into fan-out's pattern matching.
            salIndex=snap.exp_id,
            scriptSalIndex=42,
            dome=FannedOutVisit.Dome.OPEN,
            duration=float(EXPOSURE_INTERVAL+SLEW_INTERVAL),
            totalCheckpoints=1,
        )
        _log.debug(f"File {blob.key} parsed as snap {snap.snap} of visit {visit}.")
        if snap.group in result:
            snap_dict = result[snap.group]
            if snap.snap in snap_dict:
                _log.debug(f"New detector {visit.detector} added to snap {snap.snap} of group {snap.group}.")
                detector_dict = snap_dict[snap.snap]
                detector_dict[visit] = blob
            else:
                _log.debug(f"New snap {snap.snap} added to group {snap.group}.")
                snap_dict[snap.snap] = {visit: blob}
        else:
            _log.debug(f"New group {snap.group} registered.")
            result[snap.group] = {snap.snap: {visit: blob}}

    return result


def upload_from_raws(kafka_url, instrument, raw_pool, src_bucket, dest_bucket, n_groups, group_base):
    """Upload visits and files using real raws.

    Parameters
    ----------
    kafka_url : `str`
        The URL of the Kafka REST Proxy to send ``next_visit`` messages to.
    instrument : `str`
        The short name of the instrument carrying out the observation.
    raw_pool : mapping [`str`, mapping [`int`, mapping [`activator.FannedOutVisit`, `s3.ObjectSummary`]]]
        Available raws as a mapping from group IDs to a mapping of snap ID.
        The value of the innermost mapping is the observation metadata for
        each detector, and a Blob representing the image taken in that
        detector-snap.
    src_bucket : `S3.Bucket`
        The bucket containing the blobs in ``raw_pool``.
    dest_bucket : `S3.Bucket`
        The bucket to which to upload the new images.
    n_groups : `int`
        The number of observation groups to simulate. If more than the number
        of groups in ``raw_pool``, files will be re-uploaded under new
        group IDs.
    group_base : `int`
        The base number from which to offset new group numbers.

    Exceptions
    ----------
    ValueError
        Raised if ``n_groups`` exceeds the number of groups in ``raw_pool``.
    """
    if n_groups > len(raw_pool):
        raise ValueError(f"Requested {n_groups} groups, but only {len(raw_pool)} "
                         "unobserved raw groups are available.")

    for i, true_group in enumerate(itertools.islice(raw_pool, n_groups)):
        group = str(group_base + i)
        _log.info(f"Processing group {group} from unobserved {true_group}...")
        # snap_dict maps snap_id to {visit: blob}
        snap_dict = {}
        # Copy all the visit-blob dictionaries under each snap_id,
        # replacing the (immutable) FannedOutVisit objects to point to group
        # instead of true_group.
        for snap_id, old_visits in raw_pool[true_group].items():
            snap_dict[snap_id] = {dataclasses.replace(true_visit, groupId=group): blob
                                  for true_visit, blob in old_visits.items()}
        # Gather all the FannedOutVisit objects found in snap_dict, merging
        # duplicates for different snaps of the same detector.
        visit_infos = {info for det_dict in snap_dict.values() for info in det_dict}

        # TODO: may be cleaner to use a functor object than to depend on
        # closures for the buckets and data.
        def upload_from_pool(visit, snap_id):
            src_blob = snap_dict[snap_id][visit]
            exposure_key, exposure_header, exposure_num = \
                make_exposure_id(visit.instrument, int(visit.groupId), snap_id)
            filename = get_raw_path(visit.instrument, visit.detector, visit.groupId, snap_id,
                                    exposure_num, visit.filters)
            # r+b required by replace_header_key.
            with tempfile.TemporaryFile(mode="r+b") as buffer:
                src_bucket.download_fileobj(src_blob.key, buffer)
                replace_header_key(buffer, exposure_key, exposure_header)
                buffer.seek(0)  # Assumed by upload_fileobj.
                dest_bucket.upload_fileobj(buffer, filename)

        process_group(kafka_url, visit_infos, upload_from_pool)
        time.sleep(SLEW_INTERVAL)


if __name__ == "__main__":
    main()
