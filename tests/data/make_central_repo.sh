#!/bin/bash -e
# Create a central repo that contains calibs, refcats, templates for use in
# tests, making all the files be empty and adjusting the sqlite registry to
# match.

REPO="${PROMPT_PROCESSING_DIR:?Can\'t find prompt_processing repo; is it set up?}/tests/data/central_repo"

# For compatibility testing, use the lowest version we offer support for
butler create "$REPO" --dimension-config "$DAF_BUTLER_DIR/python/lsst/daf/butler/configs/old_dimensions/daf_butler_universe7.yaml"
butler register-instrument "$REPO" lsst.obs.lsst.LsstCam
butler register-dataset-type "$REPO" gain_correction IsrCalib instrument detector --is-calibration

# Import datasets
butler write-curated-calibrations "$REPO" LSSTCam --collection LSSTCam/calib/DM-50520
butler prune-datasets "$REPO" LSSTCam/calib/curated/19700101T000000Z --where "instrument='LSSTCam' and detector not in (90, 91)" --unstore --purge LSSTCam/calib/curated/19700101T000000Z --no-confirm

butler transfer-datasets /repo/main "$REPO" --transfer copy --register-dataset-types --transfer-dimensions --dataset-type bfk --collections LSSTCam/calib/DM-49175 --where "instrument='LSSTCam' and detector in (90, 91)"
butler transfer-datasets /repo/main "$REPO" --transfer copy --register-dataset-types --transfer-dimensions --dataset-type bias --collections LSSTCam/calib/DM-49175 --where "instrument='LSSTCam' and detector in (90, 91)"
butler transfer-datasets /repo/main "$REPO" --transfer copy --register-dataset-types --transfer-dimensions --dataset-type cti --collections LSSTCam/calib/DM-49175 --where "instrument='LSSTCam' and detector in (90, 91)"
butler transfer-datasets /repo/main "$REPO" --transfer copy --register-dataset-types --transfer-dimensions --dataset-type dark --collections LSSTCam/calib/DM-49175 --where "instrument='LSSTCam' and detector in (90, 91)"
# write-curated-calibrations produces manual_defects but not defects
butler transfer-datasets /repo/main "$REPO" --transfer copy --register-dataset-types --transfer-dimensions --dataset-type defects --collections LSSTCam/calib/DM-49175 --where "instrument='LSSTCam' and detector in (90, 91)"
butler transfer-datasets /repo/main "$REPO" --transfer copy --register-dataset-types --transfer-dimensions --dataset-type flat --collections LSSTCam/calib/DM-52163 --where "instrument='LSSTCam' and detector in (90, 91) and physical_filter='g_6'"
butler transfer-datasets /repo/main "$REPO" --transfer copy --register-dataset-types --transfer-dimensions --dataset-type linearizer --collections LSSTCam/calib/DM-49175 --where "instrument='LSSTCam' and detector in (90, 91)"
butler transfer-datasets /repo/main "$REPO" --transfer copy --register-dataset-types --transfer-dimensions --dataset-type ptc --collections LSSTCam/calib/DM-50336 --where "instrument='LSSTCam' and detector in (90, 91)"
butler transfer-datasets /repo/main "$REPO" --transfer copy --register-dataset-types --transfer-dimensions --dataset-type pretrainedModelPackage --collections pretrained_models/dummy
butler transfer-datasets /repo/main "$REPO" --transfer copy --register-dataset-types --transfer-dimensions --dataset-type skyMap --where "skymap='lsst_cells_v1'" --collections skymaps
# The tract constraint is not strictly necessary but just to filter out templates from another overlapping tract.
butler transfer-datasets embargo "$REPO" --transfer copy --register-dataset-types --transfer-dimensions --dataset-type template_coadd --collections LSSTCam/templates/DM-51716/SV_225/run --where "instrument='LSSTCam' and detector=90 and visit=2025052100138 and skymap='lsst_cells_v1' and tract=3534"
butler transfer-datasets /repo/main "$REPO" --transfer copy --register-dataset-types --transfer-dimensions --dataset-type the_monster_20250219 --collections refcats --where "instrument='LSSTCam' and detector=90 and visit=2025052100138"

# Certify non-curated calibs
butler certify-calibrations "$REPO" LSSTCam/calib/DM-49175/run7/bfkGen.20250320a/20250324T220123Z LSSTCam/calib/DM-50520 bfk --begin-date "2025-05-15T12:00:00" --end-date "2025-06-25T12:00:00"
butler certify-calibrations "$REPO" LSSTCam/calib/DM-49175/run7/biasGen.20250320a/20250325T205118Z LSSTCam/calib/DM-50520 bias --begin-date "2025-05-15T12:00:00" --end-date "2025-06-25T12:00:00"
butler certify-calibrations "$REPO" LSSTCam/calib/DM-49175/run7/ctiGen.20250320a/20250325T152139Z LSSTCam/calib/DM-50520 cti --begin-date "2025-05-15T12:00:00" --end-date "2025-06-25T12:00:00"
butler certify-calibrations "$REPO" LSSTCam/calib/DM-49175/run7/darkGen.20250320a/20250326T000943Z LSSTCam/calib/DM-50520 dark --begin-date "2025-05-15T12:00:00" --end-date "2025-06-25T12:00:00"
butler certify-calibrations "$REPO" LSSTCam/calib/DM-49175/run7/defectGen.20250401a/20250401T232630Z LSSTCam/calib/DM-50520 defects --begin-date "2025-05-15T12:00:00" --end-date "2025-06-25T12:00:00"
butler certify-calibrations "$REPO" LSSTCam/calib/DM-52163/flats-2s-v30-nograd-ugrizy/flatTwoLedGen-g.20250812a/20250812T182450Z LSSTCam/calib/DM-50520 flat --begin-date "2025-05-15T12:00:00" --end-date "2025-06-25T12:00:00"
butler certify-calibrations "$REPO" LSSTCam/calib/DM-49175/run7/linearizerGen.20250320a/20250321T052032Z LSSTCam/calib/DM-50520 linearizer --begin-date "2025-05-15T12:00:00" --end-date "2025-06-25T12:00:00"
butler certify-calibrations "$REPO" LSSTCam/calib/DM-50336/run7/ptcGen.20250422a/20250422T162135Z LSSTCam/calib/DM-50520 ptc --begin-date "2025-05-15T12:00:00" --end-date "2025-06-25T12:00:00"

# Alias all the runs
butler collection-chain "$REPO" pretrained_models pretrained_models/dummy
butler collection-chain "$REPO" refcats refcats/DM-49042/the_monster_20250219
butler collection-chain "$REPO" LSSTCam/calib LSSTCam/calib/DM-50520
butler collection-chain "$REPO" LSSTCam/templates LSSTCam/templates/DM-51716/SV_225/run
butler collection-chain "$REPO" LSSTCam/defaults LSSTCam/calib skymaps refcats pretrained_models

# Create init-outputs (and chain)
# UI designed to be run from container, not command line
export RUBIN_INSTRUMENT=LSSTCam
export TEMP_APDB=apdb.db  # Needed for persistent metadata table
export CENTRAL_REPO="$REPO"
export CONFIG_APDB=foo.yaml
export PREPROCESSING_PIPELINES_CONFIG="- survey: SURVEY
  pipelines:
  - ${PROMPT_PROCESSING_DIR}/tests/data/Preprocess.yaml
  - ${PROMPT_PROCESSING_DIR}/tests/data/MinPrep.yaml
  - ${PROMPT_PROCESSING_DIR}/tests/data/NoPrep.yaml
"
export MAIN_PIPELINES_CONFIG="- survey: SURVEY
  pipelines:
  - ${PROMPT_PROCESSING_DIR}/tests/data/ApPipe.yaml
  - ${PROMPT_PROCESSING_DIR}/tests/data/SingleFrame.yaml
  - ${PROMPT_PROCESSING_DIR}/tests/data/ISR.yaml
"
apdb-cli create-sql "sqlite:///${TEMP_APDB}" "${CONFIG_APDB}"
butler ingest-raws "$REPO" s3://embargo@rubin-summit/LSSTCam/20250521/MC_O_20250521_000138/MC_O_20250521_000138_R22_S00.fits -t copy
butler define-visits "$REPO" lsst.obs.lsst.LsstCam
pipetask run -b "$REPO" -i LSSTCam/raw/all,LSSTCam/defaults,LSSTCam/templates -o u/add-dataset-type -d "instrument='LSSTCam' and exposure=2025052100138 and detector=90" -p $AP_PIPE_DIR/pipelines/LSSTCam/ApPipe.yaml -c parameters:apdb_config=${CONFIG_APDB}  -c associateApdb:doPackageAlerts=False --register-dataset-types --init-only
# Clean up data that are no longer needed.
butler remove-runs "$REPO" LSSTCam/raw/all --no-confirm --force
rm -rf "$REPO"/raw
butler remove-runs "$REPO" u/add-dataset-type/* --no-confirm --force
butler remove-collections "$REPO" u/add-dataset-type --no-confirm
rm -rf "$REPO"/u

python "${PROMPT_PROCESSING_DIR}/bin/write_init_outputs.py"
rm -f "${CONFIG_APDB}" "${TEMP_APDB}"

# Empty out files and make them size 0 in the registry.
# We do not empty the camera or skymap because we actually need to read them.
for x in `find "$REPO/LSSTCam/calib/"DM-* -name "*.fits"`; do : > $x; done
for x in `find "$REPO/LSSTCam/calib/curated/" -name "*.fits"`; do : > $x; done
for x in `find "$REPO/LSSTCam/templates/" -name "*.fits"`; do : > $x; done
for x in `find "$REPO/pretrained_models/" -name "*.zip"`; do : > $x; done
for x in `find "$REPO/refcats/DM-49042/" -name "*.fits"`; do : > $x; done
for x in `find "$REPO/LSSTCam/prompt/" -type f`; do : > $x; done
