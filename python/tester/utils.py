# This file is part of prompt_prototype.
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

__all__ = ["get_last_group", "make_exposure_id", "replace_header_key", "send_next_visit"]

import json
import logging

from astropy.io import fits


_log = logging.getLogger("lsst." + __name__)
_log.setLevel(logging.DEBUG)


def get_last_group(bucket, instrument, date):
    """Identify the largest group number or a new group number.

    This number helps decide the next group number so it will not
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
    group : `int`
        The largest existing group for ``instrument``, or a newly generated
        group if none exist.
    """
    preblobs = bucket.objects.filter(
        Prefix=f"{instrument}/",
    )
    detector = min((int(preblob.key.split("/")[1]) for preblob in preblobs), default=0)

    blobs = preblobs.filter(
        Prefix=f"{instrument}/{detector}/{date}"
    )
    prefixes = [int(blob.key.split("/")[2]) for blob in blobs]
    if len(prefixes) == 0:
        return int(date) * 100_000
    else:
        return max(prefixes)


def make_exposure_id(instrument, group_num, snap):
    """Generate an exposure ID from an exposure's other metadata.

    The exposure ID is designed for insertion into an image header, and is
    therefore a string in the instrument's native format.

    Parameters
    ----------
    instrument : `str`
        The short name of the instrument.
    group_num : `int`
        The integer used to generate a group ID.
    snap : `int`
        A snap ID.

    Returns
    -------
    exposure_key : `str`
        The header key under which ``instrument`` stores the exposure ID.
    exposure_header : `str`
        An exposure ID that is likely to be unique for each combination of
        ``group_num`` and ``snap``, for a given ``instrument``, in the format
        for ``instrument``'s header.
    exposure_num : `int`
        An exposure ID equivalent to ``exposure_header`` in the format expected
        by Gen 3 Middleware.
    """
    match instrument:
        case "HSC":
            return "EXP-ID", *make_hsc_id(group_num, snap)
        case _:
            raise NotImplementedError(f"Exposure ID generation not supported for {instrument}.")


def make_hsc_id(group_num, snap):
    """Generate an exposure ID that the Butler can parse as a valid HSC ID.

    This function returns a value in the "HSCE########" format (introduced June
    2016) for all exposures, even if the source image is older.

    Parameters
    ----------
    group_num : `int`
        The integer used to generate a group ID.
    snap : `int`
        A snap ID.

    Returns
    -------
    exposure_header : `str`
        An exposure ID that is likely to be unique for each combination of
        ``group`` and ``snap``, in the form it appears in HSC headers.
    exposure_num : `int`
        The exposure ID genereated by Middleware from ``exposure_header``.

    Notes
    -----
    The current implementation allows up to 1000 group numbers per day.
    It can overflow with ~60 calls to upload.py on the same day or
    upload_hsc_rc2.py with a large N_GROUPS.
    """
    # This is a bit too dependent on how group_num is generated, but I want the
    # group number to be discernible even after compressing to 8 digits.
    night_id = (group_num // 100_000) % 2020_00_00     # Always 5 digits
    run_id = group_num % 100_000                       # Up to 5 digits, but usually 2-3
    exposure_id = (night_id * 1000) + (run_id % 1000)  # Always 8 digits
    return f"HSCE{exposure_id:08d}", exposure_id


def send_next_visit(producer, group, visit_infos):
    """Simulate the transmission of a ``next_visit`` message.

    Parameters
    ----------
    producer : `confluent_kafka.Producer`
        The client that posts ``next_visit`` messages.
    group : `str`
        The group ID for the message to send.
    visit_infos : `set` [`activator.Visit`]
        The visit-detector combinations to be sent; each object may
        represent multiple snaps.
    """
    _log.info(f"Sending next_visit for group: {group}")
    topic = "next-visit-topic"
    for info in visit_infos:
        _log.debug(f"Sending next_visit for group: {info.group} detector: {info.detector} "
                   f"filter: {info.filter} ra: {info.ra} dec: {info.dec} kind: {info.kind}")
        data = json.dumps(info.__dict__).encode("utf-8")
        producer.produce(topic, data)


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
