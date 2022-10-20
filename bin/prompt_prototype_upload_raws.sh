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

# This script uploads the raw files from the HSC PDR2 run to a bucket at USDF.
# It renames the files to match prompt_prototype conventions. The user must
# have bucket access already configured.

set -e  # Abort on any error

shopt -s expand_aliases
alias aws="singularity exec /sdf/sw/s3/aws-cli_latest.sif aws \
	   --endpoint-url https://s3dfrgw.slac.stanford.edu"

RAW_DIR="/sdf/group/rubin/datasets/hsc/raw/ssp_pdr2/2016-03-07"
UPLOAD_BUCKET=rubin-pp-users/unobserved

# Filename format is defined in activator/raw.py:
# instrument/detector/group/snap/exposureId/filter/instrument-group-snap-exposureId-filter-detector
aws s3 cp "${RAW_DIR}/HSCA05913553.fits" \
    s3://${UPLOAD_BUCKET}/HSC/0/2016030700001/0/0059134/HSC-G/HSC-2016030700001-0-0059134-HSC-G-0.fits
aws s3 cp "${RAW_DIR}/HSCA05913542.fits" \
    s3://${UPLOAD_BUCKET}/HSC/4/2016030700001/0/0059134/HSC-G/HSC-2016030700001-0-0059134-HSC-G-4.fits
aws s3 cp "${RAW_DIR}/HSCA05913543.fits" \
    s3://${UPLOAD_BUCKET}/HSC/5/2016030700001/0/0059134/HSC-G/HSC-2016030700001-0-0059134-HSC-G-5.fits

aws s3 cp "${RAW_DIR}/HSCA05914353.fits" \
    s3://${UPLOAD_BUCKET}/HSC/0/2016030700002/0/0059142/HSC-G/HSC-2016030700002-0-0059142-HSC-G-0.fits
aws s3 cp "${RAW_DIR}/HSCA05914343.fits" \
    s3://${UPLOAD_BUCKET}/HSC/5/2016030700002/0/0059142/HSC-G/HSC-2016030700002-0-0059142-HSC-G-5.fits
aws s3 cp "${RAW_DIR}/HSCA05914337.fits" \
    s3://${UPLOAD_BUCKET}/HSC/11/2016030700002/0/0059142/HSC-G/HSC-2016030700002-0-0059142-HSC-G-11.fits

aws s3 cp "${RAW_DIR}/HSCA05915112.fits" \
    s3://${UPLOAD_BUCKET}/HSC/50/2016030700003/0/0059150/HSC-G/HSC-2016030700003-0-0059150-HSC-G-50.fits
aws s3 cp "${RAW_DIR}/HSCA05915116.fits" \
    s3://${UPLOAD_BUCKET}/HSC/58/2016030700003/0/0059150/HSC-G/HSC-2016030700003-0-0059150-HSC-G-58.fits

aws s3 cp "${RAW_DIR}/HSCA05916109.fits" \
    s3://${UPLOAD_BUCKET}/HSC/43/2016030700004/0/0059160/HSC-G/HSC-2016030700004-0-0059160-HSC-G-43.fits
aws s3 cp "${RAW_DIR}/HSCA05916113.fits" \
    s3://${UPLOAD_BUCKET}/HSC/51/2016030700004/0/0059160/HSC-G/HSC-2016030700004-0-0059160-HSC-G-51.fits
