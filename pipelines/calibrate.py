# Config override for lsst.pipe.tasks.calibrate.CalibrateTask
# This config is for the ap_verify datasets that use gaia/panstarrs as their
# refcat names.

# Use gaia for astrometry (phot_g_mean for everything, as that is the broadest
# band with the most depth).

config.connections.astromRefCat = "gaia_dr2_20200414"
config.astromRefObjLoader.anyFilterMapsToThis = "phot_g_mean"
config.astromRefObjLoader.filterMap = {}

# Use panstarrs for photometry (grizy filters).
config.connections.photoRefCat = "ps1_pv3_3pi_20170110"
