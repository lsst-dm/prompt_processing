#!/bin/bash -e
# Create a central repo that contains calibs, refcats, templates for use in
# tests, making all the files be empty and adjusting the sqlite registry to
# match.

REPO="${PROMPT_PROCESSING_DIR}/tests/data/central_repo/:?Can't find prompt_processing repo; is it set up?"

# Force version 6 repo for compatibility testing
butler create "$REPO" --dimension-config "$DAF_BUTLER_DIR/python/lsst/daf/butler/configs/old_dimensions/daf_butler_universe6.yaml"
butler register-instrument "$REPO" lsst.obs.lsst.LsstComCamSim

# Import datasets
butler write-curated-calibrations "$REPO" LSSTComCamSim --collection LSSTComCamSim/calib/DM-46312
butler transfer-datasets embargo_or4 "$REPO" --transfer copy --register-dataset-types --transfer-dimensions --dataset-type bfk --collections u/abrought/bfk_70240217_w_2024_07_final/20240227T033735Z --where "instrument='LSSTComCamSim' and detector in (4, 5)"
butler transfer-datasets embargo_or4 "$REPO" --transfer copy --register-dataset-types --transfer-dimensions --dataset-type bias --collections LSSTComCamSim/calib/DM-42287 --where "instrument='LSSTComCamSim' and detector in (4, 5)"
butler transfer-datasets embargo_or4 "$REPO" --transfer copy --register-dataset-types --transfer-dimensions --dataset-type dark --collections LSSTComCamSim/calib/DM-42287 --where "instrument='LSSTComCamSim' and detector in (4, 5)"
butler transfer-datasets embargo_or4 "$REPO" --transfer copy --register-dataset-types --transfer-dimensions --dataset-type flat --collections LSSTComCamSim/calib/DM-44910 --where "instrument='LSSTComCamSim' and detector in (4, 5) and physical_filter='g_01'"
butler transfer-datasets /repo/main "$REPO" --transfer copy --register-dataset-types --transfer-dimensions --dataset-type pretrainedModelPackage --collections pretrained_models/dummy
butler transfer-datasets embargo_or4 "$REPO" --transfer copy --register-dataset-types --transfer-dimensions --dataset-type skyMap --where "skymap='ops_rehersal_prep_2k_v1'"
butler transfer-datasets embargo_or4 "$REPO" --transfer copy --register-dataset-types --transfer-dimensions --dataset-type goodSeeingCoadd --collections LSSTComCamSim/runs/OR4_templates/w_2024_23/DM-44718/20240610T044930Z --where "instrument='LSSTComCamSim' and detector=4 and visit=7024061700046"
butler transfer-datasets embargo_or4 "$REPO" --transfer copy --register-dataset-types --transfer-dimensions --dataset-type uw_stars_20240524 --collections refcats --where "instrument='LSSTComCamSim' and detector=4 and visit=7024061700046"

# Certify non-curated calibs
butler certify-calibrations "$REPO" u/abrought/bfk_70240217_w_2024_07_final/20240227T033735Z LSSTComCamSim/calib/DM-46312 bfk --begin-date "2024-06-15T12:00:00" --end-date "2024-06-25T12:00:00"
butler certify-calibrations "$REPO" u/jchiang/bias_70240217_w_2024_07/20240218T190659Z LSSTComCamSim/calib/DM-46312 bias --begin-date "2024-06-15T12:00:00" --end-date "2024-06-25T12:00:00"
butler certify-calibrations "$REPO" u/jchiang/dark_70240217_w_2024_07/20240218T191310Z LSSTComCamSim/calib/DM-46312 dark --begin-date "2024-06-15T12:00:00" --end-date "2024-06-25T12:00:00"
butler certify-calibrations "$REPO" u/jchiang/flat_70240417_w_2024_15/20240418T050546Z LSSTComCamSim/calib/DM-46312 flat --begin-date "2024-06-15T12:00:00" --end-date "2024-06-25T12:00:00"

# Alias all the runs
butler collection-chain "$REPO" pretrained_models pretrained_models/dummy
butler collection-chain "$REPO" refcats refcats/DM-42510
butler collection-chain "$REPO" LSSTComCamSim/calib LSSTComCamSim/calib/DM-46312
butler collection-chain "$REPO" LSSTComCamSim/templates LSSTComCamSim/runs/OR4_templates/w_2024_23/DM-44718/20240610T044930Z
butler collection-chain "$REPO" LSSTComCamSim/defaults LSSTComCamSim/calib skymaps refcats pretrained_models

# Empty out files and make them size 0 in the registry.
# We do not empty the camera or skymap because we actually need to read them.
for x in `find "$REPO/LSSTComCamSim/calib/curated/" -name "*.fits"`; do : > $x; done
for x in `find "$REPO/LSSTComCamSim/calib/DM-43441/" -name "*.fits"`; do : > $x; done
for x in `find "$REPO/LSSTComCamSim/runs/" -name "*.fits"`; do : > $x; done
for x in `find "$REPO/pretrained_models/" -name "*.zip"`; do : > $x; done
for x in `find "$REPO/refcats/DM-42510/" -name "*.fits"`; do : > $x; done
for x in `find "$REPO/u/abrought/" -name "*.fits"`; do : > $x; done
for x in `find "$REPO/u/jchiang/" -name "*.fits"`; do : > $x; done
