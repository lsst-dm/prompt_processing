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


__all__ = ["DatasetCache"]


import collections
from collections.abc import Collection, Iterable, Iterator, Mapping, Set
import copy
import functools
import itertools
import logging
from typing import Callable
import warnings

import lsst.daf.butler as daf_butler

from .evictingSet import EvictingSet, RandomReplacementSet


_log = logging.getLogger("lsst." + __name__)
_log.setLevel(logging.DEBUG)
# See https://developer.lsst.io/stack/logging.html#logger-trace-verbosity
_log_trace = logging.getLogger("TRACE1.lsst." + __name__)
_log_trace.setLevel(logging.CRITICAL)  # Turn off by default.


def _check_component(dataset_type: daf_butler.DatasetType):
    """Test whether an operation tries to insert a component dataset.

     Parameters
    ----------
    dataset_type : `lsst.daf.butler.DatasetType`
        The type to test.

    Raises
    ------
    ValueError
        Raised if the type is a component.
    """
    if dataset_type.isComponent():
        raise ValueError(f"Cannot store a dataset of type {dataset_type}, "
                         f"store a {dataset_type.makeCompositeDatasetType()} instead.")


def _get_parent_ref(dataset_ref: daf_butler.DatasetRef) -> daf_butler.DatasetRef:
    """Return a reference to a dataset's top-level dataset.

     Parameters
    ----------
    dataset_ref : `lsst.daf.butler.DatasetRef`
        The reference to promote.

    Returns
    -------
    parent_ref : `lsst.daf.butler.DatasetRef`
        The top-level reference associated with ``dataset_ref``. May be the
        same object if ``dataset_ref`` is not a component.
    """
    if dataset_ref.isComponent():
        return _get_parent_ref(dataset_ref.makeCompositeRef())
    else:
        return dataset_ref


class DatasetCache(collections.abc.Collection[daf_butler.DatasetRef]):
    """A container for storing a limited number of datasets.

    Attempts to add datasets beyond the configured limits remove objects from
    the cache.

    This object can only store references to top-level datasets, not
    components.

    Parameters
    ----------
    default_cache_size : `int`
        The number of datasets per type to cache, unless overridden by
        ``cache_sizes``.
    cache_sizes : mapping [`str` or `~lsst.daf.butler.DatasetType`, `int`], optional
        A map from dataset types to the number of datasets of that type to
        cache. Any types not in the map are handled according to
        ``default_cache_size``.
    cache_factory : callable [(`int`), `activator.evictingSet.EvictingSet`], optional
        A callable for creating new caches for each dataset type. It must take
        the desired cache size and return an empty set. If not provided, a
        default cache implementation is used.
    """
    def __init__(self,
                 default_cache_size: int,
                 cache_sizes: Mapping[str | daf_butler.DatasetType, int] | None = None,
                 cache_factory: Callable[[int], EvictingSet] | None = None,
                 ):
        super().__init__()

        if default_cache_size < 0:
            raise ValueError(f"Cannot create negative-sized cache, got {default_cache_size}.")

        if cache_sizes is None:
            cache_sizes = {}
        if cache_factory is None:
            cache_factory = RandomReplacementSet

        # Index by str
        self._caches = collections.defaultdict(
            functools.partial(cache_factory, default_cache_size),
            {t if isinstance(t, str) else t.name: cache_factory(n)
             for t, n in cache_sizes.items()}
        )
        bad_types = {name for name in self._caches.keys() if "." in name}
        if bad_types:
            raise ValueError(f"Cannot cache component datasets: {bad_types}")

    def __contains__(self, item: daf_butler.DatasetRef):
        # Don't index the defaultdict with a component.
        if item.isComponent():
            return False
        return item in self._caches[item.datasetType.name]

    def __iter__(self) -> Iterator[daf_butler.DatasetRef]:
        return itertools.chain.from_iterable(self._caches.values())

    def __len__(self):
        total = 0
        for cache in self._caches.values():
            total += len(cache)
        return total

    def _merge_into_cache(self, inputs: Mapping[str, Set[daf_butler.DatasetRef]]) \
            -> [Set[daf_butler.DatasetRef], Set[daf_butler.DatasetRef], Mapping[str, EvictingSet]]:
        """Compute a bulk update of caches for multiple dataset types.

        This method finds a combination of insertions and evictions that
        satisfies the configured caching strategy. It does not update any
        state itself.

        If ``inputs`` cannot all fit in the cache, this method emits a warning.

        Parameters
        ----------
        inputs : mapping [`str`, set [`lsst.daf.butler.DatasetRef`]]
            The dataset refs to insert, after any trimming for invalid inputs.
            The keys are the dataset type names.

        Returns
        -------
        to_evict : set [`lsst.daf.butler.DatasetRef`]
            The datasets to remove from the Butler repo and the cache.
        to_add : set [`lsst.daf.butler.DatasetRef`]
            The datasets to add to the repo and the cache. May be a subset of
            the refs in ``inputs``.
        caches : mapping [`str`,  `activator.evictingSet.EvictingSet`]
            The new cache states after removing ``to_evict`` and inserting
            ``to_add``. The mapping need not include caches that don't need to
            be updated.
        """
        to_evict = set()
        to_add = set()
        excess = set()
        caches = {}
        for name in inputs.keys():
            caches[name] = copy.copy(self._caches[name])
            caches[name] |= inputs[name]
            assert isinstance(caches[name], EvictingSet)  # guard against Python operand-swapping
            to_evict.update(self._caches[name] - caches[name])
            to_add.update(caches[name] - self._caches[name])
            excess.update(inputs[name] - caches[name])
        if excess:
            warnings.warn(f"Cannot store all inputs in cache; dropping {excess}.", RuntimeWarning)
        return to_evict, to_add, caches

    def update(self, others: Iterable[daf_butler.DatasetRef]) -> Collection[daf_butler.DatasetRef]:
        """Add multiple datasets to the cache.

        Datasets may be evicted from the cache, but the datasets in ``others``
        will be dropped only if they cannot all fit.

        Parameters
        ----------
        others : iterable [`lsst.daf.butler.DatasetRef`]
            The datasets to attempt to add to the cache.

        Returns
        -------
        evicted : collection [`lsst.daf.butler.DatasetRef`]
            The datasets evicted from cache. Does not include any members of
            ``others`` that could not be added.

        Notes
        -----
        The cache is not updated in the event of an exception.
        """
        grouped_refs = collections.defaultdict(set)
        for ref in others:
            _check_component(ref.datasetType)
            grouped_refs[ref.datasetType.name].add(ref)

        # Copy-and-swap caches for atomic behavior.
        to_evict, _, new_caches = self._merge_into_cache(grouped_refs)

        self._caches.update(new_caches)
        if to_evict:
            _log.debug("Evicting datasets: %s", to_evict)
        return to_evict

    def access(self, others: Iterable[daf_butler.DatasetRef]):
        """Mark use of multiple datasets.

        Usage statistics may be used by the cache algorithm to prioritize
        evictions.

        Parameters
        ----------
        others : iterable [`lsst.daf.butler.DatasetRef`]
            The datasets to mark as used.

        Raises
        ------
        LookupError
            Raised if one or more of the datasets are not in the cache.

        Notes
        -----
        Usage statistics are not updated in the event of an exception.
        """
        grouped_refs = collections.defaultdict(set)
        for ref in others:
            parent_ref = _get_parent_ref(ref)
            grouped_refs[parent_ref.datasetType.name].add(parent_ref)

        # Copy-and-swap caches for atomic behavior
        _temp = {}
        for name, refs in grouped_refs.items():
            _temp[name] = copy.copy(self._caches[name])
            for ref in refs:
                _temp[name].get(ref)
        self._caches.update(_temp)
