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

import unittest

from lsst.daf.butler import Butler, DataCoordinate, DatasetRef, DatasetType, DimensionUniverse
import lsst.daf.butler.tests as butler_tests

from activator.caching import DatasetCache
from activator.evictingSet import RandomReplacementSet


class DatasetCacheTest(unittest.TestCase):
    def _cache_factory(self, size):
        # Variable seed gives better coverage of edge cases.
        return RandomReplacementSet(size, seed=abs(hash(self.id())))

    @classmethod
    def _make_test_repo(cls, location):
        """Create and initialize a test repo.

        Parameters
        ----------
        location : `str`
            The directory in which to create a repo.

        Returns
        -------
        butler : `lsst.daf.butler.Butler`
            A Butler pointing to the newly-created repo.
        """
        # Don't use butler_tests.makeTestRepo; in-memory repos can't handle export or transfer_from
        butler = Butler(Butler.makeRepo(location), run="testRun")

        butler_tests.addDataIdValue(butler, "instrument", "NotACam")
        butler_tests.addDataIdValue(butler, "physical_filter", "k2024", band="k")
        butler_tests.addDataIdValue(butler, "exposure", 42)
        butler_tests.addDataIdValue(butler, "detector", 0)
        butler_tests.addDataIdValue(butler, "detector", 1)
        butler_tests.addDataIdValue(butler, "detector", 2)

        butler_tests.addDatasetType(butler, "int1", {"instrument"}, "int")
        butler_tests.addDatasetType(butler, "int2", {"instrument", "exposure", "detector"}, "int")
        butler_tests.addDatasetType(butler, "int3", {"instrument", "exposure", "detector"}, "int")
        butler_tests.addDatasetType(butler, "exp", {"instrument", "exposure", "detector"}, "Exposure")

        return butler

    def setUp(self):
        universe = DimensionUniverse()

        self.type1 = DatasetType("int1", {"instrument"}, "int", universe=universe)
        self.type2 = DatasetType("int2", {"instrument", "exposure", "detector"}, "int", universe=universe)
        self.type3 = DatasetType("int3", {"instrument", "exposure", "detector"}, "int", universe=universe)
        self.typeE = DatasetType("exp", {"instrument", "exposure", "detector"}, "Exposure", universe=universe)

        self.ref1 = DatasetRef(self.type1,
                               DataCoordinate.standardize({"instrument": "NotACam"}, universe=universe),
                               run="testRun")
        self.ref20 = DatasetRef(
            self.type2,
            DataCoordinate.standardize({"instrument": "NotACam", "exposure": 42, "detector": 0},
                                       universe=universe),
            run="testRun")
        self.ref21 = DatasetRef(
            self.type2,
            DataCoordinate.standardize({"instrument": "NotACam", "exposure": 42, "detector": 1},
                                       universe=universe),
            run="testRun")
        self.ref30 = DatasetRef(
            self.type3,
            DataCoordinate.standardize({"instrument": "NotACam", "exposure": 42, "detector": 0},
                                       universe=universe),
            run="testRun")
        self.ref31 = DatasetRef(
            self.type3,
            DataCoordinate.standardize({"instrument": "NotACam", "exposure": 42, "detector": 1},
                                       universe=universe),
            run="testRun")
        self.ref32 = DatasetRef(
            self.type3,
            DataCoordinate.standardize({"instrument": "NotACam", "exposure": 42, "detector": 2},
                                       universe=universe),
            run="testRun")
        self.refExp0 = DatasetRef(
            self.typeE,
            DataCoordinate.standardize({"instrument": "NotACam", "exposure": 42, "detector": 0},
                                       universe=universe),
            run="testRun")
        # Only safe way to make a component ref
        self.refPsf0 = self.refExp0.makeComponentRef("psf")

    def _assert_cache_state(self, cache, datasets):
        """Generic assertion of expected cache contents.

        Parameters
        ----------
        cache : `activator.caching.DatasetCache`
            The cache to test.
        datasets : mapping [`lsst.daf.butler.DatasetRef`, `bool`]
            A mapping from dataset refs to whether or not it is expected.
        """
        for ref, expected in datasets.items():
            if expected:
                self.assertIn(ref, cache)
            else:
                self.assertNotIn(ref, cache)

    def test_constructor_initial_state(self):
        cache = DatasetCache(2, cache_sizes={self.type1: 0, "int2": 1})

        self.assertFalse(cache)
        self.assertEqual(len(cache), 0)
        self.assertEqual(list(cache), [])

    def test_constructor_nonnegative(self):
        with self.assertRaises(ValueError):
            DatasetCache(-1)
        DatasetCache(0)

        with self.assertRaises(ValueError):
            DatasetCache(2, cache_sizes={"int1": -1, "int2": 1})
        DatasetCache(2, cache_sizes={self.type1: 0, "int2": 1})

    def test_constructor_nocomponents(self):
        with self.assertRaises(ValueError):
            DatasetCache(2, cache_sizes={"exp.psf": 1})
        DatasetCache(2, cache_sizes={"exp": 1})

    # NOTE to maintainers: the following tests make no assumptions about what
    # eviction strategy DatasetCache is configured with. To test specific
    # strategies, make separate test cases (likely with a larger cache and a
    # controlled history of reads).

    def test_update(self):
        cache = DatasetCache(2, cache_sizes={"int2": 1}, cache_factory=self._cache_factory)

        self._assert_cache_state(
            cache,
            {ref: False for ref in [self.ref1, self.ref20, self.ref21, self.ref30, self.ref31, self.ref32,
                                    self.refExp0]}
        )

        # Should not evict
        evicted = cache.update([self.ref1, self.ref20, self.ref30, self.ref31])
        self.assertFalse(evicted)
        self._assert_cache_state(cache, {self.ref1: True,
                                         self.ref20: True,
                                         self.ref21: False,
                                         self.ref30: True,
                                         self.ref31: True,
                                         self.ref32: False,
                                         self.refExp0: False,
                                         })

        # Redundant, should not evict
        evicted = cache.update([self.ref20])
        self.assertFalse(evicted)
        self._assert_cache_state(cache, {self.ref1: True,
                                         self.ref20: True,
                                         self.ref21: False,
                                         self.ref30: True,
                                         self.ref31: True,
                                         self.ref32: False,
                                         self.refExp0: False,
                                         })

        # Should evict existing int2
        evicted = cache.update([self.ref21])
        self.assertEqual(set(evicted), {self.ref20})
        self._assert_cache_state(cache, {self.ref1: True,
                                         self.ref20: False,
                                         self.ref21: True,
                                         self.ref30: True,
                                         self.ref31: True,
                                         self.ref32: False,
                                         self.refExp0: False,
                                         })

        # Should evict one of the other int3s
        evicted = cache.update({self.ref32})
        self.assertEqual(len(evicted), 1)
        self.assertLess(set(evicted), {self.ref30, self.ref31})
        self._assert_cache_state(cache, {self.ref1: True,
                                         self.ref20: False,
                                         self.ref21: True,
                                         self.ref32: True,
                                         self.refExp0: False,
                                         })
        self.assertTrue((self.ref30 in cache) ^ (self.ref31 in cache), msg=f"cache = {cache}")
        self.assertTrue((self.ref30 in evicted) ^ (self.ref30 in cache),
                        msg=f"evicted = {evicted}, cache = {cache}")
        self.assertTrue((self.ref31 in evicted) ^ (self.ref31 in cache),
                        msg=f"evicted = {evicted}, cache = {cache}")

        # Should cache by parent but allow component retrieval
        with self.assertRaises(ValueError):
            cache.update({self.refPsf0})
        cache.update({self.refExp0})
        self.assertIn(self.refExp0, cache)
        self.assertNotIn(self.refPsf0, cache)
