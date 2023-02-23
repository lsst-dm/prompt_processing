#!/bin/bash -e
# Create a central repo that contains calibs, refcats, templates for use in
# tests, making all the files be empty and adjusting the sqlite registry to
# match.

# This export file was created with this command, and then added a comment on the first line:
# make_preloaded_export.py --src-rep $AP_VERIFY_CI_HITS2015_DIR
butler create central_repo
butler import --export-file export.yaml --transfer copy central_repo/ $AP_VERIFY_CI_HITS2015_DIR/preloaded/

butler collection-chain central_repo refcats refcats/gen2
butler collection-chain central_repo templates templates/deep
butler collection-chain central_repo DECam/defaults DECam/calib,refcats,templates,skymaps

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
