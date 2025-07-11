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

import argparse
import json
import logging
import math
import multiprocessing
import os
import random
import tempfile
import time
import yaml

import astropy
from astropy.io import fits
import boto3
from botocore.handlers import validate_bucket_name

from lsst.utils.timer import time_this
from lsst.daf.butler import Butler
from lsst.resources import ResourcePath

from shared.raw import get_raw_path, _LSST_CAMERA_LIST
from shared.visit import SummitVisit
# Need explicit tester for command-line execution
from tester.utils import (
    INSTRUMENTS,
    get_last_group,
    increment_group,
    make_exposure_id,
    make_imsim_time_headers,
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


def _make_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "config",
        help="URI to a YAML file containing the configurations "
             "of the upload test, including the instrument name "
             "the data repo, and the dataset selection.",
    )
    parser.add_argument(
        "n_groups",
        type=int,
        help="The number of groups to upload.",
    )
    parser.add_argument(
        "--ordered",
        action="store_true",
        help="Upload the exposures following the order of the "
             "original exposure IDs."
    )
    return parser


def main():
    args = _make_parser().parse_args()
    with open(args.config) as file:
        configs = yaml.safe_load(file)
    instrument = configs["instrument"]

    date = time.strftime("%Y%m%d")

    kafka_url = "https://usdf-rsp-dev.slac.stanford.edu/sasquatch-rest-proxy/topics/test.next-visit-job"

    _set_s3_bucket()

    group = get_last_group(dest_bucket, instrument, date)
    _log.debug(f"Last group {group}")

    butler = Butler(configs["repo"])
    visit_list = get_visit_list(
        butler,
        args.n_groups,
        ordered=args.ordered,
        instrument=instrument,
        **configs["query"],
    )

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
            group = increment_group(instrument, group, 1)
            # For imSim, use current time as the event publish time. This does
            # not guarantee that the actual exposure will occur at a specific time.
            exposure_start = astropy.time.Time.now().unix_tai + 2*float(EXPOSURE_INTERVAL + SLEW_INTERVAL)
            refs = prepare_one_visit(kafka_url, group, butler, instrument, visit, exposure_start)
            ref_dict = butler.get_many_uris(refs)
            _log.info(f"Slewing to group {group}, with {instrument} visit {visit}")
            time.sleep(SLEW_INTERVAL)
            _log.info(f"Taking exposure for group {group}")
            time.sleep(EXPOSURE_INTERVAL)
            _log.info(f"Uploading detector images for group {group}")
            upload_images(pool, temp_dir, group, ref_dict, exposure_start)
        pool.close()
        _log.info("Waiting for uploads to finish...")
        pool.join()


def get_visit_list(butler, n_sample, ordered=False, **kwargs):
    """Return a list of selected raw visits in the butler repo.

    Parameters
    ----------
    butler : `lsst.daf.butler.Butler`
        The Butler in which to search for records of raw data.
    n_sample : `int`
        The number of visits to select.
    ordered : `bool`
        If `True`, return the first ``n_sample`` visits in the order of the visit
        IDs. Otherwise, return ``n_sample`` randomly selected visit IDs.
    **kwargs
        Additional parameters for the butler query. They have the same meanings
        as the parameters of `lsst.daf.butler.Registry.queryDimensionRecords`.
        The query must be valid for ``butler``.

    Returns
    -------
    visits : `list` [`int`]
        A list of ``n_sample`` selected visit IDs from the dataset.
    """
    results = butler.registry.queryDimensionRecords("visit", datasets="raw", **kwargs)
    records = [record.id for record in set(results)]
    if n_sample > len(records):
        raise ValueError(f"Requested {n_sample} groups, but only {len(records)} are available.")
    if ordered:
        return sorted(records)[:n_sample]
    else:
        visits = random.sample(records, k=n_sample)
        return visits


def prepare_one_visit(kafka_url, group_id, butler, instrument, visit_id, start_time_unix_tai):
    """Extract metadata and send next_visit events for one visit

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
    instrument : `str`
        The short instrument name of this visit.
    visit_id : `int`
        The ID of a visit in the dataset.
    start_time_unix_tai : `float`
        The Unix time (TAI) of the exposure start.
        This is only used for LSSTCam-imSim.

    Returns
    -------
    refs : iterable of `lsst.daf.butler.DatasetRef`
        The datasets for which the events are sent.
    """
    refs = butler.registry.queryDatasets(
        datasetType="raw",
        collections=f"{instrument}/raw/all",
        dataId={"visit": visit_id, "instrument": instrument},
    )

    duration = float(EXPOSURE_INTERVAL + SLEW_INTERVAL)
    # all items in refs share the same visit info and one event is to be sent
    for data_id in refs.dataIds.limit(1).expanded():
        # All instruments except LSSTCam-imSim uses original exposure timespan.
        start_time = data_id.records["exposure"].timespan.begin
        if instrument == "LSSTCam-imSim":
            start_time = astropy.time.Time(start_time_unix_tai, format="unix_tai")
        visit = SummitVisit(
            instrument=instrument,
            groupId=group_id,
            nimages=1,
            filters=data_id.records["physical_filter"].name,
            coordinateSystem=SummitVisit.CoordSys.ICRS,
            position=[data_id.records["exposure"].tracking_ra, data_id.records["exposure"].tracking_dec],
            startTime=start_time.unix_tai,
            rotationSystem=SummitVisit.RotSys.SKY,
            cameraAngle=data_id.records["exposure"].sky_angle,
            survey="SURVEY",
            salIndex=INSTRUMENTS[instrument].sal_index,
            scriptSalIndex=999,
            dome=SummitVisit.Dome.OPEN,
            duration=duration,
            totalCheckpoints=1,
            private_sndStamp=start_time.unix_tai-2*duration,
            private_efdStamp=start_time.unix-2*duration,
        )
        send_next_visit(kafka_url, group_id, {visit})

    return refs


def upload_images(pool, temp_dir, group_id, ref_dict, exposure_start):
    """Upload one group of raw images to the central repo

    Parameters
    ----------
    pool : `multiprocessing.pool.Pool`
        The process pool with which to schedule file uploads.
    temp_dir : `str`
        A directory in which to temporarily hold the images so that their
        metadata can be modified.
    group_id : `str`
        The group ID under which to store the images.
    ref_dict : `dict` [ `lsst.daf.butler.DatasetRef`, `lsst.daf.butler.datastore.DatasetRefURIs` ]
        A dict of the datasetRefs to upload and their corresponding URIs.
    exposure_start : `float`
        The Unix time (TAI) of the exposure start.
    """
    # Non-blocking assignment lets us upload during the next exposure.
    # Can't time these tasks directly, but the blocking equivalent took
    # 12-20 s depending on tuning, or less than a single exposure.
    pool.starmap_async(
        _upload_one_image,
        [(temp_dir, group_id, r, ref_dict[r].primaryURI, exposure_start) for r in ref_dict],
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


def _upload_one_image(temp_dir, group_id, ref, uri, exposure_start):
    """Upload a raw image to the central repo.

    Parameters
    ----------
    temp_dir : `str`
        A directory in which to temporarily hold the images so that their
        metadata can be modified.
    group_id : `str`
        The group ID under which to store the images.
    ref : `lsst.daf.butler.DatasetRef`
        The dataset to upload.
    uri : `lsst.resources.ResourcePath`
        URI to the image to upload.
    exposure_start : `float`
        The Unix time (TAI) of the exposure start.
        Currently only used in imSim tests.
    """
    instrument = ref.dataId["instrument"]
    with time_this(log=_log, msg="Single-image processing", prefix=None):
        exposure_num, headers = make_exposure_id(instrument, group_id, 0)
        if instrument == "LSSTCam-imSim":
            headers.update(make_imsim_time_headers(EXPOSURE_INTERVAL, exposure_start))
        dest_key = get_raw_path(
            instrument,
            ref.dataId["detector"],
            group_id,
            0,
            exposure_num,
            ref.dataId["physical_filter"],
        )

        sidecar_uploaded = False
        if instrument in _LSST_CAMERA_LIST:
            # Upload a corresponding sidecar json file
            sidecar = uri.updatedExtension("json")
            if sidecar.exists():
                with sidecar.open("r") as f:
                    md = json.load(f)
                    md.update(headers)
                    dest_bucket.put_object(
                        Body=json.dumps(md), Key=dest_key.removesuffix("fits") + "json"
                    )
                sidecar_uploaded = True

        path = os.path.join(temp_dir, uri.basename())
        ResourcePath(path).transfer_from(uri, transfer="copy")
        _log.debug(
            f"Raw file for {ref.dataId} was copied from Butler to {path}"
        )
        try:
            with open(path, "r+b") as temp_file:
                for header_key in headers:
                    replace_header_key(temp_file, header_key, headers[header_key])
                if not sidecar_uploaded and instrument in _LSST_CAMERA_LIST:
                    with fits.open(temp_file, mode="update") as hdul:
                        dest_bucket.put_object(
                            Body=json.dumps(dict(hdul[0].header)),
                            Key=dest_key.removesuffix("fits") + "json",
                        )
            with open(path, "rb") as temp_file:
                dest_bucket.put_object(Body=temp_file, Key=dest_key)
            _log.debug(f"{dest_key} was written at {dest_bucket}")
        finally:
            os.remove(path)


if __name__ == "__main__":
    main()
