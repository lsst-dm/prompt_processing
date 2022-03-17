#!/bin/bash
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

# This script uploads the raw files from the ap_verify_ci_cosmos_pdr2 dataset
# to Google Storage. It renames the files to match prompt_prototype conventions.
# The user must have gsutil already configured, and must have
# ap_verify_ci_cosmos_pdr2 set up.

set -e  # Abort on any error

RAW_DIR="${AP_VERIFY_CI_COSMOS_PDR2_DIR:?'dataset is not set up'}/raw"
UPLOAD_BUCKET=rubin-prompt-proto-unobserved

# Filename format is defined in tester/upload.py and activator/activator.py:
# instrument/detector/group/snap/instrument-group-snap-exposureId-filter-detector
gsutil cp "${RAW_DIR}/HSC-0059150-050.fits.gz" \
    gs://${UPLOAD_BUCKET}/HSC/50/2016030700001/0/HSC-2016030700001-0-0059150-HSC-G-50.fits.gz
gsutil cp "${RAW_DIR}/HSC-0059160-051.fits.gz" \
    gs://${UPLOAD_BUCKET}/HSC/51/2016030700002/0/HSC-2016030700002-0-0059160-HSC-G-51.fits.gz
