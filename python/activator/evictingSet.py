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


__all__ = ["EvictingSet", "RandomReplacementSet"]


import abc
import collections.abc
import copy
from typing import Any, Self

import numpy


# TODO: implementing the full MutableSet API is overkill, and both the API's
# reliance on updating operators and the default implementations in MutableSet
# are potentially dangerous. Reimplement as a Collection that does not claim to
# be a set.
class EvictingSet(collections.abc.MutableSet):
    """An interface for sets that store a maximum number of elements, removing
    excess elements as needed.

    All operations are guaranteed to be atomic; in particular, a failed
    operation never evicts. The actual eviction policy is up to
    the implementation.

    Notes
    -----
    The definitions of binary operators guarantee that the values from the
    right-hand-side will not be evicted unless necessary. This convention allows
    the creating and in-place operations to behave consistently (e.g.,
    ``a |= b`` is equivalent to ``a = a | b``). However, this means that binary
    operations are not symmetric.
    """
    @property
    @abc.abstractmethod
    def max_size(self) -> int:
        """The maximum set size (`int`, read-only).

        Any insertions above this size trigger eviction.
        """

    def __and__(self, other: collections.abc.Set) -> Self:
        """Return a new set with elements common to the set and ``other``.

        The returned set is of the same type as the first operand, and has the
        same `max_size`.
        """
        return super().__and__(other)

    def __rand__(self, other: collections.abc.Set) -> collections.abc.Set:
        # Ignore eviction on the RHS
        return other & frozenset(self)

    def __or__(self, other: collections.abc.Set) -> Self:
        """Return a new set with elements from the set and ``other``.

        The returned set is of the same type as the first operand, and has the
        same `max_size`. If the length of the result would exceed `max_size`,
        evictions are made as if this set had been copied, then updated with |=.
        """
        return super().__or__(other)

    def __ror__(self, other: collections.abc.Set) -> collections.abc.Set:
        # Ignore eviction on the RHS
        return other | frozenset(self)

    def __sub__(self, other: collections.abc.Set) -> Self:
        """Return a new set with elements in the set that are not in the others.

        The returned set is of the same type as the first operand, and has the
        same `max_size`.
        """
        return super().__sub__(other)

    def __rsub__(self, other: collections.abc.Set) -> collections.abc.Set:
        # Ignore eviction on the RHS
        return other - frozenset(self)

    def __xor__(self, other: collections.abc.Set) -> Self:
        """Return a new set with elements in either the set or ``other`` but
        not both.

        The returned set is of the same type as the first operand, and has the
        same `max_size`. If the length of the result would exceed `max_size`,
        evictions are made as if this set had been copied, then updated with ^=.
        """
        return super().__xor__(other)

    def __rxor__(self, other: collections.abc.Set) -> collections.abc.Set:
        # Ignore eviction on the RHS
        return other ^ frozenset(self)

    @abc.abstractmethod
    def add(self, elem: Any) -> Any | None:
        """Add element ``elem`` to the set.

        Parameters
        ----------
        elem
            The element to add to the set.

        Returns
        -------
        evicted : optional
            The element removed to make way for ``elem``, or `None` if no
            eviction was done.
        """

    @abc.abstractmethod
    def get(self, elem: Any) -> Any:
        """\"Read\" an element from the set.

        This method exists to support eviction algorithms that depend on
        elements' usage patterns. Clients can use it to prioritize elements
        for retention.

        Parameters
        ----------
        elem
            The element to "retrieve" from the set.

        Returns
        -------
        elem
            The element, which may or may not be the same object as the
            input argument.

        Raises
        ------
        KeyError
            Raised if the set does not contain ``elem``.
        """

    def __ior__(self, other: collections.abc.Iterable) -> Self:
        """Update the set, adding elements from all others.

        If the length of the result would exceed `max_size`, evictions are made
        as if each object returned from ``other`` had been inserted
        simultaneously. The new objects are only evicted if the length of
        ``other`` also exceeds `max_size`. This guarantee gives behavior
        inconsistent with iterating and calling `add`, but gives sensible
        results for eviction strategies that penalize recently-added objects.

        Parameters
        ----------
        other : iterable
            The iterable from which to add elements.
        """
        return super().__ior__(other)

    def __ixor__(self, other: collections.abc.Iterable) -> Self:
        """Update the set, keeping only elements found in either set, but not
        in both.

        If the length of the result would exceed `max_size`, evictions are made
        as if all ineligible members of this set had been removed first, then
        each eligible object returned from ``other`` had been inserted
        simultaneously. The new objects are only evicted if their number
        also exceeds `max_size`. This guarantee gives behavior
        inconsistent with iterating and calling `add`, but gives sensible
        results for eviction strategies that penalize recently-added objects.

        Parameters
        ----------
        other : iterable
            The iterable from which to update elements.
        """
        return super().__ixor__(other)

    # set also implements issubset, issuperset, union, intersection, difference,
    # symmetric_difference, copy, update, intersection_update,
    # difference_update, and symmetric_difference_update, but MutableSet does
    # not require these.

    def _setlike(self) -> str:
        """Return a representation of the set's contents in the form
        {A, B, ...}.

        This method is intended for use in implementations of `__str__`
        and `__repr__`.
        """
        return "{" + ", ".join(repr(x) for x in self) + "}"

    def __str__(self) -> str:
        return self._setlike()


class RandomReplacementSet(EvictingSet):
    """An EvictingSet that evicts elements at random, without considering usage
    or insertion order.

    This implementation requires that elements be hashable.

    Parameters
    ----------
    max_size : `int`
        The maximum number of elements allowed in the set.
    iterable
        The initial elements of the set. If larger than ``max_size``, this set
        will contain a random subset of ``iterable``.
    seed : `int`, optional
        A seed for the random evictions.
    """

    def __init__(self,
                 max_size: int,
                 iterable: collections.abc.Iterable = frozenset(),
                 *,
                 seed: int | None = None
                 ):
        self._max_size = max_size
        if self._max_size < 0:
            raise ValueError(f"Maximum size must be nonnegative, gave {self._max_size}.")
        self._impl = set(iterable)
        self._rng = numpy.random.default_rng(seed)
        self._seed = seed  # Needed for _from_iterable before Numpy 1.25
        self._evict(self._impl, self.max_size)

    # This override is an instance method (explicitly allowed by the Set docs)
    # so that the LHS of any binary op can copy over its max_size.
    def _from_iterable(self, it: collections.abc.Iterable) -> Self:
        return type(self)(max_size=self.max_size, iterable=it, seed=self._seed)

    def _evict(self, target_set: set, desired: int) -> collections.abc.Collection[Any]:
        """Remove elements until the set is capped at a given size.

        Parameters
        ----------
        target_set : `set`
            The set from which to evict elements.
        desired : `int`
            The number of elements that should remain after eviction.

        Returns
        -------
        evicted : collection
            The elements that were removed.
        """
        if len(target_set) > desired:
            n_extra = len(target_set) - desired
            evicted = self._rng.choice(list(target_set), n_extra, replace=False, shuffle=False)
            target_set.difference_update(evicted)
            assert len(target_set) == desired
            return evicted
        else:
            return set()

    @property
    def max_size(self) -> int:
        return self._max_size

    def __contains__(self, item: Any) -> bool:
        return item in self._impl

    def __iter__(self) -> collections.abc.Iterator:
        return iter(self._impl)

    def __len__(self) -> int:
        return len(self._impl)

    def add(self, elem: Any) -> Any | None:
        # Copy-and-swap to ensure atomic behavior
        _temp = self._impl.copy()
        # Evict first to ensure elem is not removed.
        if elem not in _temp and self.max_size > 0:
            evicted = self._evict(_temp, self.max_size - 1)
            _temp.add(elem)
            self._impl = _temp
        else:
            evicted = set()
        if evicted:
            return next(iter(evicted))
        else:
            return None

    def get(self, elem: Any) -> Any:
        if elem in self:
            return elem
        else:
            raise KeyError(f"No such element: {elem!r}")

    def discard(self, elem: Any):
        return self._impl.discard(elem)

    def __or__(self, other: collections.abc.Set) -> Self:
        # Enforce eviction order guarantees
        merged = copy.copy(self)
        merged.__ior__(other)  # DO NOT use |=; it can change the type of the LHS!
        return merged

    def __xor__(self, other: collections.abc.Set) -> Self:
        # Enforce eviction order guarantees
        merged = copy.copy(self)
        merged.__ixor__(other)  # DO NOT user ^=; it can change the type of the LHS!
        return merged

    def __ior__(self, other: collections.abc.Iterable) -> Self:
        # Let a, b, c be disjoint sets such that A = a | c and B = b | c
        # Let clip(S, N) denote the eviction of S down to size N
        # Method must guarantee clip(A | B, N) = { a | b | c               if |A | B| <= N
        #                                          clip(a, N-|B|) | b | c  if |A | B| > N and |B| <= N
        #                                          clip(b | c, N)          if |B| > N
        # It can be shown that implementing clip(A | B, N) = clip(a, N-|B|) | clip(B, N)
        # guarantees this for all a, b, c, and N.

        # Copy-and-swap to ensure atomic behavior
        _temp = self._impl.copy()
        # Evict first to ensure new inputs are not removed unless necessary.
        incoming_values = set(other)

        # If something needs to be evicted, first cut is values that aren't in the RHS
        low_priority_values = _temp - incoming_values
        to_keep = max(self.max_size - len(incoming_values), 0)
        first_evicted = self._evict(low_priority_values, to_keep)
        _temp.difference_update(first_evicted)  # DO NOT use -=; it can change the type of the LHS!

        self._evict(incoming_values, self.max_size)
        _temp.update(incoming_values)  # DO NOT use |=
        self._impl = _temp
        return self

    def __ixor__(self, other: collections.abc.Iterable) -> Self:
        # Let a, b, c be disjoint sets such that A = a | c and B = b | c
        # Let clip(S, N) denote the eviction of S down to size N
        # Method must guarantee clip(A ^ B, N) = { a | b               if |a | b| <= N
        #                                          clip(a, N-|b|) | b  if |a | b| > N and |b| <= N
        #                                          clip(b, N)          if |b| > N
        # It can be shown that implementing clip(A ^ B, N) = clip(a, N-|b|) | clip(b, N)
        # guarantees this for all a, b, c, and N.

        # Copy-and-swap to ensure atomic behavior
        _temp = self._impl.copy()
        incoming_values = set(other)
        excess_values = _temp & incoming_values
        _temp.difference_update(excess_values)  # DO NOT use -=; it can change the type of the LHS!

        # If something needs to be evicted, first cut is values that aren't in the RHS
        new_values = incoming_values - excess_values
        to_keep = max(self.max_size - len(new_values), 0)
        self._evict(_temp, to_keep)

        self._evict(new_values, self.max_size)
        _temp.update(new_values)  # DO NOT use |=
        self._impl = _temp
        return self

    def __repr__(self) -> str:
        # It's not possible to extract the original seed from a Generator
        return f"{type(self).__name__}({self.max_size}, {self._setlike()})"
