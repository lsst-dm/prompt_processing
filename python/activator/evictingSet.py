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


__all__ = ["EvictingSet"]


import abc
import collections.abc
from typing import Any, Self


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

    def __or__(self, other: collections.abc.Set) -> Self:
        """Return a new set with elements from the set and ``other``.

        The returned set is of the same type as the first operand, and has the
        same `max_size`. If the length of the result would exceed `max_size`,
        evictions are made as if this set had been copied, then updated with |=.
        """
        return super().__or__(other)

    def __sub__(self, other: collections.abc.Set) -> Self:
        """Return a new set with elements in the set that are not in the others.

        The returned set is of the same type as the first operand, and has the
        same `max_size`.
        """
        return super().__sub__(other)

    def __xor__(self, other: collections.abc.Set) -> Self:
        """Return a new set with elements in either the set or ``other`` but
        not both.

        The returned set is of the same type as the first operand, and has the
        same `max_size`. If the length of the result would exceed `max_size`,
        evictions are made as if this set had been copied, then updated with ^=.
        """
        return super().__xor__(other)

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
