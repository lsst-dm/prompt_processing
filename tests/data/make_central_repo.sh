#!/bin/bash -e
# Create a central repo that contains calibs, refcats, templates for use in
# tests, making all the files be empty and adjusting the sqlite registry to
# match.

# This export file was created with this command, and then added a comment on the first line:
# make_preloaded_export.py --src-rep $AP_VERIFY_CI_HITS2015_DIR/preloaded
butler create central_repo
butler import --export-file export.yaml --transfer copy central_repo/ $AP_VERIFY_CI_HITS2015_DIR/preloaded/

butler collection-chain central_repo refcats refcats/DM-28636
butler collection-chain central_repo DECam/templates u/elhoward/DM-38243/templates/20230320T214145Z
butler collection-chain central_repo DECam/defaults DECam/calib,refcats,DECam/templates,skymaps
butler collection-chain central_repo pretrained_models pretrained_models/rbResnet50-DC2

# Empty out files and make them size 0 in the registry.
# We do not empty the camera description in ``DECAM/calib/unbounded`
# because we need it to load the camera geometry.
find central_repo/refcats -name "*.fits" -execdir sh -c '> "$1"' -- {} \;
find central_repo -name "*templates*fits" -execdir sh -c '> "$1"' -- {} \;
find central_repo -name "*pretrainedModel*zip" -execdir sh -c '> "$1"' -- {} \;
find central_repo/DECam/calib/20150218T000000Z -name "*.fits" -execdir sh -c '> "$1"' -- {} \;
find central_repo/DECam/calib/20150313T000000Z -name "*.fits" -execdir sh -c '> "$1"' -- {} \;
find central_repo/DECam/calib/curated -name "*.fits" -execdir sh -c '> "$1"' -- {} \;
# find central_repo/skymaps -name "*.fits" -execdir sh -c '> "$1"' -- {} \;

# The camera and skymap files are not emptied out. The records need to be consistent so that
# the datastore does not complain about size mismatch when reading them.
sqlite3 central_repo/gen3.sqlite3 'update file_datastore_records set file_size=0 where path != "DECam/calib/unbounded/camera/camera_DECam_DECam_calib_unbounded.fits" and path != "skymaps/skyMap/skyMap_decam_rings_v1_skymaps.pickle";'
