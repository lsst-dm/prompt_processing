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

import logging
import math
import multiprocessing
import random
import sys
import tempfile
import time

import boto3
from botocore.handlers import validate_bucket_name

from lsst.utils.timer import time_this
from lsst.daf.butler import Butler

from activator.raw import get_raw_path
from activator.visit import SummitVisit
from tester.utils import (
    get_last_group,
    increment_group,
    make_exposure_id,
    replace_header_key,
    send_next_visit,
)


EXPOSURE_INTERVAL = 18
SLEW_INTERVAL = 2


logging.basicConfig(
    format="{levelname} {asctime} {name} - {message}",
    style="{",
)
_log = logging.getLogger("lsst." + __name__)
_log.setLevel(logging.INFO)


# HACK: S3 object initialized once per process; see https://stackoverflow.com/questions/48091874/
dest_bucket = None


def _set_s3_bucket():
    """Initialize and configure a bucket object to handle file upload.

    This function sets the ``dest_bucket`` global rather than returning the
    new bucket.
    """
    with time_this(log=_log, msg="Bucket initialization", prefix=None):
        global dest_bucket
        endpoint_url = "https://s3dfrgw.slac.stanford.edu"
        s3 = boto3.resource("s3", endpoint_url=endpoint_url)
        dest_bucket = s3.Bucket("rubin-pp-dev")
        dest_bucket.meta.client.meta.events.unregister("before-parameter-build.s3", validate_bucket_name)


def main():
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} N_GROUPS")
        sys.exit(1)
    n_groups = int(sys.argv[1])

    date = time.strftime("%Y%m%d")

    kafka_url = "https://usdf-rsp-dev.slac.stanford.edu/sasquatch-rest-proxy/topics/test.next-visit"
    _set_s3_bucket()

    last_group = get_last_group(dest_bucket, "HSC", date)
    group = increment_group("HSC", last_group, random.randrange(10, 19))
    _log.debug(f"Last group {last_group}; new group base {group}")

    butler = Butler("/repo/main")
    visit_list = get_hsc_visit_list(butler, n_groups)

    # fork pools don't work well with connection pools, such as those used
    # for Butler registry or S3.
    context = multiprocessing.get_context("spawn")
    max_processes = _get_max_processes()
    # Use a shared pool to minimize initialization overhead. This has the
    # benefit of letting the pool be initialized in parallel with the first
    # exposure.
    _log.debug("Uploading with %d processes...", max_processes)
    with context.Pool(processes=max_processes, initializer=_set_s3_bucket) as pool, \
            tempfile.TemporaryDirectory() as temp_dir:
        for visit in visit_list:
            group = increment_group("HSC", group, 1)
            refs = prepare_one_visit(kafka_url, group, butler, visit)
            _log.info(f"Slewing to group {group}, with HSC visit {visit}")
            time.sleep(SLEW_INTERVAL)
            _log.info(f"Taking exposure for group {group}")
            time.sleep(EXPOSURE_INTERVAL)
            _log.info(f"Uploading detector images for group {group}")
            upload_hsc_images(pool, temp_dir, group, butler, refs)
        pool.close()
        _log.info("Waiting for uploads to finish...")
        pool.join()


def get_hsc_visit_list(butler, n_sample):
    """Return a list of randomly selected raw visits from HSC-RC2 in the butler repo.

    Parameters
    ----------
    butler : `lsst.daf.butler.Butler`
        The Butler in which to search for records of raw data.
    n_sample: `int`
        The number of visits to select.

    Returns
    -------
    visits : `list` [`int`]
        A list of randomly selected non-narrow-band visit IDs from the HSC-RC2 dataset
    """
    results = butler.registry.queryDimensionRecords(
        "visit",
        datasets="raw",
        collections="HSC/RC2/defaults",
        where="instrument='HSC' and exposure.observation_type='science' "
              "and band in ('g', 'r', 'i', 'z', 'y')",
    )
    rc2 = [record.id for record in set(results)]
    if n_sample > len(rc2):
        raise ValueError(f"Requested {n_sample} groups, but only {len(rc2)} are available.")
    visits = random.sample(rc2, k=n_sample)
    return visits


def prepare_one_visit(kafka_url, group_id, butler, visit_id):
    """Extract metadata and send next_visit events for one HSC-RC2 visit

    One ``next_visit`` message is sent to the development fan-out service,
    which translates it into multiple messages.

    Parameters
    ----------
    kafka_url : `str`
        The URL of the Kafka REST Proxy to send ``next_visit`` messages to.
    group_id : `str`
        The group ID for the message to send.
    butler : `lsst.daf.butler.Butler`
        The Butler with the raw data.
    visit_id : `int`
        The ID of a visit in the HSC-RC2 dataset.

    Returns
    -------
    refs : iterable of `lsst.daf.butler.DatasetRef`
        The datasets for which the events are sent.
    """
    refs = butler.registry.queryDatasets(
        datasetType="raw",
        collections="HSC/RC2/defaults",
        dataId={"exposure": visit_id, "instrument": "HSC"},
    )

    duration = float(EXPOSURE_INTERVAL + SLEW_INTERVAL)
    # all items in refs share the same visit info and one event is to be sent
    for data_id in refs.dataIds.limit(1).expanded():
        visit = SummitVisit(
            instrument="HSC",
            groupId=group_id,
            nimages=1,
            filters=data_id.records["physical_filter"].name,
            coordinateSystem=SummitVisit.CoordSys.ICRS,
            position=[data_id.records["exposure"].tracking_ra, data_id.records["exposure"].tracking_dec],
            startTime=data_id.records["exposure"].timespan.begin.unix_tai,
            rotationSystem=SummitVisit.RotSys.SKY,
            cameraAngle=data_id.records["exposure"].sky_angle,
            survey="SURVEY",
            salIndex=999,
            scriptSalIndex=999,
            dome=SummitVisit.Dome.OPEN,
            duration=duration,
            totalCheckpoints=1,
            private_sndStamp=data_id.records["exposure"].timespan.begin.unix_tai-2*duration,
        )
        send_next_visit(kafka_url, group_id, {visit})

    return refs


def upload_hsc_images(pool, temp_dir, group_id, butler, refs):
    """Upload one group of raw HSC images to the central repo

    Parameters
    ----------
    pool : `multiprocessing.pool.Pool`
        The process pool with which to schedule file uploads.
    temp_dir : `str`
        A directory in which to temporarily hold the images so that their
        metadata can be modified.
    group_id : `str`
        The group ID under which to store the images.
    butler : `lsst.daf.butler.Butler`
        The source Butler with the raw data.
    refs : iterable of `lsst.daf.butler.DatasetRef`
        The datasets to upload
    """
    # Non-blocking assignment lets us upload during the next exposure.
    # Can't time these tasks directly, but the blocking equivalent took
    # 12-20 s depending on tuning, or less than a single exposure.
    pool.starmap_async(_upload_one_image,
                       [(temp_dir, group_id, butler, ref) for ref in refs],
                       error_callback=_log.exception,
                       chunksize=5  # Works well across a broad range of # processes
                       )


def _get_max_processes():
    """Return the optimal process limit.

    Returns
    -------
    processes : `int`
        The maximum number of processes that balances system usage, pool
        overhead, and processing speed.
    """
    try:
        return math.ceil(0.25*multiprocessing.cpu_count())
    except NotImplementedError:
        return 4


def _upload_one_image(temp_dir, group_id, butler, ref):
    """Upload a raw HSC image to the central repo.

    Parameters
    ----------
    temp_dir : `str`
        A directory in which to temporarily hold the images so that their
        metadata can be modified.
    group_id : `str`
        The group ID under which to store the images.
    butler : `lsst.daf.butler.Butler`
        The source Butler with the raw data.
    ref : `lsst.daf.butler.DatasetRef`
        The dataset to upload.
    """
    with time_this(log=_log, msg="Single-image processing", prefix=None):
        exposure_num, headers = make_exposure_id("HSC", group_id, 0)
        dest_key = get_raw_path(
            "HSC",
            ref.dataId["detector"],
            group_id,
            0,
            exposure_num,
            ref.dataId["physical_filter"],
        )
        # Each ref is done separately because butler.retrieveArtifacts does not preserve the order.
        transferred = butler.retrieveArtifacts(
            [ref],
            transfer="copy",
            preserve_path=False,
            destination=temp_dir,
        )
        if len(transferred) != 1:
            _log.error(
                f"{ref} has multiple artifacts and cannot be handled by current implementation"
            )
            return

        path = transferred[0].path
        _log.debug(
            f"Raw file for {ref.dataId} was copied from Butler to {path}"
        )
        with open(path, "r+b") as temp_file:
            for header_key in headers:
                replace_header_key(temp_file, header_key, headers[header_key])
        dest_bucket.upload_file(path, dest_key)
        _log.debug(f"{dest_key} was written at {dest_bucket}")


if __name__ == "__main__":
    main()
