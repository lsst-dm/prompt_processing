#!/bin/bash -e
# Create a central repo that contains calibs, refcats, templates for use in
# tests, making all the files be empty and adjusting the sqlite registry to
# match.

# This export file was created with this command, and then modified to have the
# CHAINED collections that we expect (path is the repo on JKP's desktop):
# make_preloaded_export.py --src-rep /data/ap_verify_ci_hits2015
butler create central_repo
butler import --export-file export.yaml --transfer copy central_repo/ $AP_VERIFY_CI_HITS2015_DIR/preloaded/

# Empty out files and make them size 0 in the registry.
# We do not empty the camera description in ``DECAM/calib/unbounded`
# because we need it to load the camera geometry.
find central_repo/refcats -name "*.fits" -execdir sh -c '> "$1"' -- {} \;
find central_repo/templates -name "*.fits" -execdir sh -c '> "$1"' -- {} \;
find central_repo/DECam/calib/20150218T000000Z -name "*.fits" -execdir sh -c '> "$1"' -- {} \;
find central_repo/DECam/calib/20150313T000000Z -name "*.fits" -execdir sh -c '> "$1"' -- {} \;
find central_repo/DECam/calib/curated -name "*.fits" -execdir sh -c '> "$1"' -- {} \;
# find central_repo/skymaps -name "*.fits" -execdir sh -c '> "$1"' -- {} \;

sqlite3 central_repo/gen3.sqlite3 "update file_datastore_records set file_size=0;"
