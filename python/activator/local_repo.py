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


__all__ = ["LocalRepo"]


import collections.abc
import logging
import os
import tempfile
import warnings

import sqlalchemy.exc

import lsst.daf.butler as daf_butler
import lsst.obs.base as obs_base
import lsst.utils.timer

import shared.connect_utils as connect
from .caching import DatasetCache
from .mw_retries import repo_retry, DATASTORE_EXCEPTIONS


_log = logging.getLogger("lsst." + __name__)
_log.setLevel(logging.DEBUG)
# See https://developer.lsst.io/stack/logging.html#logger-trace-verbosity
_log_trace = logging.getLogger("TRACE1.lsst." + __name__)
_log_trace.setLevel(logging.CRITICAL)  # Turn off by default.


# TODO: rationalize config options on DM-52695
# The number of calib datasets to keep, including the current run.
base_keep_limit = int(os.environ.get("LOCAL_REPO_CACHE_SIZE", 3))
# An optional file with local butler repo config overrides.
local_repo_config = os.environ.get("LOCAL_REPO_CONFIG", None)


class LocalRepo:
    """A worker-local Butler repository and all state needed to manage it.

    This class is responsible for creating/cleaning up the local repository, as
    well as for maintaining data integrity. Science-related decisions such as
    populating the repository with pipeline inputs are the responsibility of
    this class's clients.

    This object guarantees that the repository exists (until it is closed), and
    that it contains standard collections and the registration of
    ``instrument``.

    Parameters
    ----------
    local_storage : `str`
        A path to a space where this class can create a local
        Butler repo.
    central_butler : `lsst.daf.butler.Butler`
        Butler repo containing instrument and skymap definitions.
    instrument : `str`
        Name of the instrument taking the data, for populating
        butler collections and dataIds. May be either the fully qualified class
        name or the short name. Examples: "LsstCam", "lsst.obs.lsst.LsstCam".
    """

    @property
    def butler(self):
        """A writeable Butler giving access to this repository
        (`lsst.daf.butler.Butler`).

        The Butler defaults to the "defaults" chained collection, which
        contains all pipeline inputs.
        """
        if self._closed:
            raise RuntimeError("Local repo has been cleaned up, no Butler operations are possible.")
        return self._butler

    @property
    def repo_location(self):
        """A file path (not a URI) identifying the repository.

        This path should only be used to handle registration of the repository
        with `~activator.repo_tracker.LocalRepoTracker`. All other repository
        operations should be done through this class or the `butler` property.
        """
        if self._closed:
            raise RuntimeError("Local repo has been cleaned up and no longer exists.")
        return self._repo.name

    def __init__(self, local_storage: str, central_butler: daf_butler.Butler, instrument: str):
        self._closed = False
        try:
            self._instrument = obs_base.Instrument.from_string(instrument, central_butler.registry)
        except RuntimeError as e:
            raise ValueError(f"Invalid instrument {instrument!r}") from e
        self._repo = self._make_local_repo(local_storage, central_butler, self._instrument)
        self._butler = self._make_local_butler(self._repo.name, self._instrument)
        self._cache = self._make_local_cache()

    @classmethod
    def _make_local_repo(cls,
                         local_storage: str,
                         central_butler: daf_butler.Butler,
                         instrument: obs_base.Instrument,
                         ) -> tempfile.TemporaryDirectory:
        """Create and configure a new local repository.

        The repository is represented by a temporary directory object, which can be
        used to manage its lifetime.

        Parameters
        ----------
        local_storage : `str`
            A path to a space where this function can create a local
            Butler repo.
        central_butler : `lsst.daf.butler.Butler`
            Butler repo containing instrument and skymap definitions.
        instrument : `lsst.obs.base.Instrument`
            The instrument taking the data, for populating butler collections
            and dataIds.

        Returns
        -------
        repo_dir : `tempfile.TemporaryDirectory`
            An object pointing to the local repo location.
        """
        dimension_config = central_butler.dimensions.dimensionConfig
        repo_dir = tempfile.TemporaryDirectory(dir=local_storage, prefix="butler-")
        config = daf_butler.Config(local_repo_config)
        new_config = daf_butler.Butler.makeRepo(repo_dir.name,
                                                config=config,
                                                dimensionConfig=dimension_config,
                                                )
        _log.info("Created local Butler repo at %s with dimensions-config %s %d.",
                  repo_dir.name, dimension_config["namespace"], dimension_config["version"])

        # Run-once repository initialization
        with daf_butler.Butler(new_config, writeable=True) as butler:
            instrument.register(butler.registry)

            # Need standard collections to exist, but don't need chains to be initialized
            butler.collections.register(instrument.makeUmbrellaCollectionName(),
                                        daf_butler.CollectionType.CHAINED)
            butler.collections.register(instrument.makeCalibrationCollectionName(),
                                        daf_butler.CollectionType.CHAINED)
            butler.collections.register(instrument.makeUnboundedCalibrationRunName(),
                                        daf_butler.CollectionType.RUN)
            butler.collections.register(instrument.makeRefCatCollectionName(),
                                        daf_butler.CollectionType.CHAINED)
            butler.collections.register(instrument.makeDefaultRawIngestRunName(),
                                        daf_butler.CollectionType.RUN)

        return repo_dir

    @classmethod
    def _make_local_butler(cls, repo_dir: str, instrument: obs_base.Instrument) -> daf_butler.Butler:
        """Set up a persistent writeable Butler for the local repo.

        Parameters
        ----------
        repo_dir : `str`
            A path to the repo location.
        instrument : `lsst.obs.base.Instrument`
            The instrument whose data should be searched.

        Returns
        -------
        butler : `lsst.daf.butler.Butler`
            A general-purpose Butler.
        """
        return daf_butler.Butler(repo_dir,
                                 collections=instrument.makeUmbrellaCollectionName(),
                                 writeable=True,
                                 )

    @classmethod
    def _make_local_cache(cls):
        """Set up a cache for preloaded datasets.

        Returns
        -------
        cache : `activator.caching.DatasetCache`
            An empty cache with configured caching strategy and limits.
        """
        return DatasetCache(base_keep_limit)

    def __del__(self):
        """Safety net finalizer for LocalRepo.
        """
        if not self._closed:
            warnings.warn(f"{self!r} has not been properly closed, attempting to close it.", ResourceWarning)
        self.close()

    def close(self):
        """Clean up the repository and all associated resources.
        """
        # Object may be only partially initialized
        if hasattr(self, "_butler") and self._butler:
            self._butler.close()
        if hasattr(self, "_repo") and self._repo:
            self._repo.cleanup()
        self._closed = True

    def load_from(self, src_butler, refs, *, no_cache_runs=None):
        """Ensure the indicated datasets are loaded into this repository.

        Datasets are not re-loaded if they're already present. Other datasets
        may be removed to make room, according to this object's caching
        strategy.

        Parameters
        ----------
        src_butler : `lsst.daf.butler.Butler`
            The Butler from which to transfer any missing datasets.
        refs : collection [`lsst.daf.butler.DatasetRef`]
            The datasets to load into this repository.
        no_cache_runs : collection [`str`], optional
            Any collection(s) whose contents should not be managed by a cache
            (e.g., because the collections will be deleted later).

        Returns
        -------
        present_refs : collection [`lsst.daf.butler.DatasetRef`]
            The datasets either loaded into or already present in this repository.
        """
        if no_cache_runs is None:
            no_cache_runs = set()

        present = set(self._find_in_repo(refs))
        missing = set(refs) - present
        _log_trace.debug("Found %d matching datasets. %d present locally, %d to download.",
                         len(refs), len(present), len(missing))
        # Update cache as late as possible in case of earlier errors
        cacheable = {d for d in refs
                     # TODO: is there an efficient test for collection membership?
                     if not d.datasetType.dimensions.spatial and d.run not in no_cache_runs}
        self._cache_datasets(cacheable)
        transferred = self._transfer_data(src_butler, missing)
        return set(transferred) | present

    def _find_in_repo(self,
                      datasets: collections.abc.Collection[lsst.daf.butler.DatasetRef]
                      ) -> collections.abc.Iterable[lsst.daf.butler.DatasetRef]:
        """Identify which of a collection of datasets is present in this repo.

        Parameters
        ----------
        datasets : collection [`~lsst.daf.butler.DatasetRef`]
            The datasets to search for. Any past dataset transfers must preserve
            dataset ID.

        Returns
        -------
        datasets : iterable [`lsst.daf.butler.DatasetRef`]
            The subset of ``datasets`` that exists in ``repo``.
        """
        return self.butler.get_many_datasets(ref.id for ref in datasets)

    def _cache_datasets(self, refs: collections.abc.Iterable[daf_butler.DatasetRef]):
        """Add or mark requested datasets in the cache.

        Parameters
        ----------
        refs : iterable [`lsst.daf.butler.DatasetRef`]
            The datasets to cache. Assumed to all fit inside the cache.
        """
        evicted = self._cache.update(refs)
        self.butler.pruneDatasets(evicted, disassociate=True, unstore=True, purge=True)
        try:
            self._cache.access(refs)
        except LookupError as e:
            raise RuntimeError("Cache is too small for one run's worth of datasets.") from e

    @connect.retry(2, DATASTORE_EXCEPTIONS, wait=repo_retry)
    def _transfer_data(self, src_butler, datasets):
        """Transfer datasets from the central repo to the local repo.

        Parameters
        ----------
        src_butler : `lsst.daf.butler.Butler`
            The source of the datasets to transfer.
        datasets : set [`~lsst.daf.butler.DatasetRef`]
            The datasets to transfer into the local repo. Assumed to be absent.

        Returns
        -------
        transferred : collection [`lsst.daf.butler.DatasetRef`]
            The datasets that were successfully transferred.
        """
        with lsst.utils.timer.time_this(_log, msg="load_from (transfer datasets)", level=logging.DEBUG):
            transferred = self.butler.transfer_from(src_butler,
                                                    datasets,
                                                    transfer="copy",
                                                    skip_missing=True,
                                                    register_dataset_types=True,
                                                    transfer_dimensions=True,
                                                    )
        return transferred

    def export_calib_associations(self, src_butler, calib_collection, datasets):
        """Export the associations between a set of datasets and a
        calibration collection.

        Parameters
        ----------
        src_butler : `lsst.daf.butler.Butler`
            The Butler from which to copy associations.
        calib_collection : `str`
            The calibration collection, or a chain thereof, containing the
            associations. The collection and any children must exist in both
            the central and local repos.
        datasets : iterable [`lsst.daf.butler.DatasetRef']
            The calib datasets whose associations must be exported. Must be
            certified in ``calib_collection`` in the central repo, and must
            exist in the local repo.
        """
        collections = src_butler.collections
        with src_butler.query() as query, \
                src_butler.registry.caching_context():  # Nested loops produce lots of queries
            for dataset in datasets:
                dtype = dataset.datasetType
                result = query.where(dataset.dataId) \
                    .join_dataset_search(dtype, calib_collection) \
                    .general(dtype.dimensions,
                             dataset_fields={dtype.name: {"dataset_id", "run", "collection", "timespan"}},
                             find_first=False,  # Required for timespan queries.
                             )
                # Associations include run membership, and possibly multiple calibration collections.
                for association in daf_butler.DatasetAssociation.from_query_result(result, dtype):
                    if collections.get_info(association.collection).type \
                            == daf_butler.CollectionType.CALIBRATION \
                            and association.ref == dataset:
                        # TODO: workaround for DM-54682, raises MissingCollectionError if collection missing
                        self.butler.collections.get_info(association.collection)
                        # certify is designed to work on groups of datasets; in practice,
                        # the total number of calibs (~1 of each type) is small enough that
                        # grouping by timespan isn't worth it.
                        try:
                            self.butler.registry.certify(association.collection,
                                                         [dataset],
                                                         association.timespan)
                        except sqlalchemy.exc.IntegrityError as e:
                            raise ValueError(f"Dataset {dataset} does not exist in the local repo.") from e
