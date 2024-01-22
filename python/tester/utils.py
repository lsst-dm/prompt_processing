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

__all__ = [
    "get_last_group",
    "make_exposure_id",
    "replace_header_key",
    "send_next_visit",
    "make_group",
    "decode_group",
    "increment_group",
]

from dataclasses import asdict
import datetime
import json
import logging
import requests

from astropy.io import fits

from lsst.obs.lsst.translators.lsst import LsstBaseTranslator

from activator.raw import _LSST_CAMERA_LIST

_log = logging.getLogger("lsst." + __name__)
_log.setLevel(logging.INFO)

max_exposure = {
    "HSC": 21474800,
}
"""A mapping of instrument to exposure_max (`dict` [`str`, `int`]).

The values are copied here so we can access them without a Butler. All
exposure IDs are in Middleware (integer) format, not native format.
"""


def get_last_group(bucket, instrument, date):
    """Identify the largest group ID or a new group ID.

    This checks the last file existing in the bucket,
    and helps decide the next group ID so it will not
    collide with any previous groups.

    Parameters
    ----------
    bucket : `s3.Bucket`
        A S3 storage bucket
    instrument : `str`
        The short name of the active instrument.
    date : `str`
        The current date in YYYYMMDD format.

    Returns
    -------
    group : `str`
        The largest existing group for ``instrument``, or a newly generated
        group if none exist.
    """
    if instrument in _LSST_CAMERA_LIST:
        blobs = bucket.objects.filter(
            Prefix=f"{instrument}/{date}/",
        )
        numbers = [int(blob.key.split("/")[2].split("_")[-1]) for blob in blobs]
    else:
        preblobs = bucket.objects.filter(
            Prefix=f"{instrument}/",
        )
        detector = min(
            (int(preblob.key.split("/")[1]) for preblob in preblobs), default=0
        )

        group_prefix = "-".join([date[:4], date[4:6:], date[-2:]])
        blobs = preblobs.filter(Prefix=f"{instrument}/{detector}/{group_prefix}")
        numbers = [int(blob.key.split("/")[2][-6:]) for blob in blobs]

    if numbers:
        last_number = max(numbers)
    else:
        last_number = 0
    return make_group(date, last_number)


def make_exposure_id(instrument, group_id, snap):
    """Generate an exposure ID from an exposure's other metadata.

    The exposure ID is designed for insertion into an image header, and is
    therefore a string in the instrument's native format.

    Parameters
    ----------
    instrument : `str`
        The short name of the instrument.
    group_id : `str`
        A group ID.
    snap : `int`
        A snap ID.

    Returns
    -------
    exposure_num : `int`
        An exposure ID that is likely to be unique for each combination of
        ``group_num`` and ``snap``, for a given ``instrument``, in the
        format expected by Gen 3 Middleware.
    headers : `dict`
        The header key-value pairs to accompany with the exposure ID in the
        format for ``instrument``'s header.
    """
    match instrument:
        case "HSC":
            return make_hsc_id(group_id, snap)
        case "LATISS":
            return make_latiss_id(group_id, snap)
        case _:
            raise NotImplementedError(f"Exposure ID generation not supported for {instrument}.")


def make_hsc_id(group_id, snap):
    """Generate an exposure ID that the Butler can parse as a valid HSC ID.

    This function returns a value in the "HSCE########" format (introduced June
    2016) for all exposures, even if the source image is older.

    Parameters
    ----------
    group_id : `str`
        A group ID.
    snap : `int`
        A snap ID.

    Returns
    -------
    exposure_num : `int`
        The exposure ID genereated by Middleware from ``headers``. It is
        likely to be unique for each combination of ``group_num`` and ``snap``.
    headers : `dict`
        The key-value pairs to represent ``exposure_num` in HSC headers.

    Notes
    -----
    The current implementation gives illegal exposure IDs after September 2024.
    If this generator is still needed after that, it will need to be tweaked.
    """
    # This is a bit too dependent on how group_id is generated, but I want the
    # group number to be discernible even after compressing to 8 digits.
    date, run_id = decode_group(group_id)  # run_id has up to 5 digits, but usually 2-3
    year = int(date[:4]) - 2023            # Always 1 digit, 0-1
    night_id = int(date[-4:])              # Always 4 digits up to 1231
    exposure_id = (year*1200 + night_id) * 10000 + (run_id % 10000)  # Always 8 digits
    if exposure_id > max_exposure["HSC"]:
        raise RuntimeError(f"{group_id} translated to expId {exposure_id}, "
                           f"max allowed is { max_exposure['HSC']}.")
    return exposure_id, {"EXP-ID": f"HSCE{exposure_id:08d}"}


def make_latiss_id(group_id, snap):
    """Generate an exposure ID that the Butler can parse as a valid LATISS ID.

    Parameters
    ----------
    group_id : `str`
        The mocked group ID.
    snap : `int`
        A snap ID.

    Returns
    -------
    exposure_number :
        An exposure ID in the format expected by Gen 3 Middleware.
    headers : `dict`
        The key-value pairs are in the form to appear in LATISS headers.
    """
    day_obs, seq_num = decode_group(group_id)
    exposure_num = LsstBaseTranslator.compute_exposure_id(day_obs, seq_num)
    obs_id = f"AT_O_{day_obs}_{seq_num:06d}"
    return exposure_num, {
        "DAYOBS": day_obs,
        "SEQNUM": seq_num,
        "OBSID": obs_id,
        "GROUPID": group_id,
    }


def send_next_visit(url, group, visit_infos):
    """Simulate the transmission of a ``next_visit`` message to Sasquatch.

    Parameters
    ----------
    url : `str`
        The URL of the Kafka REST Proxy to send ``next_visit`` messages to.
    group : `str`
        The group ID for the message to send.
    visit_infos : `set` [`activator.SummitVisit`]
        The ``next_visit`` message to be sent; each object may
        represent multiple snaps.
    """
    _log.info(f"Sending next_visit for group to kafka http proxy: {group}")
    header = {"Content-Type": "application/vnd.kafka.avro.v2+json"}
    for info in visit_infos:
        _log.debug(f"Sending next_visit for group: {info.groupId} "
                   f"filters: {info.filters} ra: {info.position[0]} dec: {info.position[1]} "
                   f"instrument: {info.instrument} survey: {info.survey}")
        records_level = dict(value=asdict(info))
        value_schema_level = dict(value_schema_id=99, records=[records_level])

        r = requests.post(url, data=json.dumps(value_schema_level), headers=header)
        _log.debug(r.content)


def replace_header_key(file, key, value):
    """Replace a header key in a FITS file with a new key-value pair.

    The file is updated in place, and left open when the function returns,
    making this function safe to use with temporary files.

    Parameters
    ----------
    file : file-like object
        The file to update. Must already be open in "rb+" mode.
    key : `str`
        The header key to update.
    value : `str`
        The value to assign to ``key``.
    """
    # Can't use astropy.io.fits.update, because that closes the underlying file.
    hdus = fits.open(file, mode="update")
    try:
        # Don't know which header is supposed to contain the key.
        for header in (hdu.header for hdu in hdus):
            if key in header:
                _log.debug("Setting %s to %s.", key, value)
                header[key] = value
    finally:
        # Clean up HDUList object *without* closing ``file``.
        hdus.close(closed=False)


def make_group(day_obs, seq_num):
    """Make up a LSST-like group ID to be used in testers.

    Parameters
    ----------
    day_obs : `str`
        Day of observation in YYYYMMDD format.
    seq_num : `int`
        Sequence number.

    Returns
    -------
    group_id : `str`
        A group ID in the LSST style.
    """
    return datetime.datetime.strptime(f"{day_obs}{seq_num:06d}", "%Y%m%d%f").isoformat(
        timespec="microseconds"
    )


def decode_group(group):
    """Interpret a group ID made by `make_group`

    Parameters
    ----------
    group_id : `str`
        A group ID made by `make_group`.

    Returns
    -------
    day_obs : `str`
        Day of observation in YYYYMMDD format.
    seq_num : `int`
        Sequence number.
    """
    timestamp = datetime.datetime.fromisoformat(group)
    day_obs = timestamp.strftime("%Y%m%d")
    seq_num = int(timestamp.strftime("%f"))
    return day_obs, seq_num


def increment_group(instrument, group_base, amount):
    """Make a larger group ID.

    Parameters
    ----------
    instrument : `str`
        The short name of the active instrument.
    group_base : `str`
        The group ID from which to increase to a new group ID.
    amount : `int`
        The amount by which to increase from ``group_base``.

    Returns
    -------
    new_group : `str`
        A new group ID that is ``amount`` larger than ``group_base``.
        The numerical amount depends on the implementation for ths
        ``intrument``.
    """
    day_obs, seq_num = decode_group(group_base)
    seq_num += amount
    return make_group(day_obs, seq_num)
