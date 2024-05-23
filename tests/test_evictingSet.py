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

import abc
import unittest

from activator.evictingSet import RandomReplacementSet


class EvictingSetTest(metaclass=abc.ABCMeta):
    @property
    @abc.abstractmethod
    def factory(self):
        """A callable that takes an iterable and returns a test set.

        Parameters
        ----------
        max_size : `int`
            The maximum size of the set.
        elements : iterable, optional
            The initial contents of the set, subject to ``max_size``.
        """

    def test_contains(self):
        testbed = self.factory(3)
        self.assertNotIn(3, testbed)
        self.assertNotIn(2, testbed)
        self.assertNotIn(42, testbed)

        testbed = self.factory(3, [3, 2])
        self.assertIn(3, testbed)
        self.assertIn(2, testbed)
        self.assertNotIn(42, testbed)

        testbed.add(42)
        self.assertIn(3, testbed)
        self.assertIn(2, testbed)
        self.assertIn(42, testbed)

        # Eviction triggered, only latest entry guaranteed to be present.
        testbed.add(101)
        self.assertIn(101, testbed)

    def test_get(self):
        testbed = self.factory(3)
        with self.assertRaises(KeyError):
            testbed.get(3)
        with self.assertRaises(KeyError):
            testbed.get(2)
        with self.assertRaises(KeyError):
            testbed.get(42)

        testbed = self.factory(3, [3, 2])
        self.assertEqual(testbed.get(3), 3)
        self.assertEqual(testbed.get(2), 2)
        with self.assertRaises(KeyError):
            testbed.get(42)

        testbed.add(42)
        self.assertEqual(testbed.get(3), 3)
        self.assertEqual(testbed.get(2), 2)
        self.assertEqual(testbed.get(42), 42)

        # Eviction triggered, only latest entry guaranteed to be present.
        testbed.add(101)
        self.assertEqual(testbed.get(101), 101)

    def test_iter(self):
        testbed = self.factory(2)
        contents = {x for x in testbed}
        self.assertEqual(contents, set())

        testbed = self.factory(3, [3, 2])
        contents = {x for x in testbed}
        self.assertEqual(contents, {3, 2})

        testbed.add(24)
        contents = {x for x in testbed}
        self.assertEqual(contents, {3, 2, 24})

        # Eviction triggered, only latest entry guaranteed to be present.
        testbed.add(101)
        contents = {x for x in testbed}
        self.assertEqual(len(contents), 3)
        self.assertIn(101, contents)

        testbed.remove(101)
        contents = {x for x in testbed}
        self.assertEqual(len(contents), 2)

        # Immediate eviction, can only guarantee number of elements.
        testbed = self.factory(1, [3, 2, 42, 5])
        contents = {x for x in testbed}
        self.assertEqual(len(contents), 1)

    def test_len(self):
        testbed = self.factory(42)
        self.assertEqual(len(testbed), 0)

        testbed = self.factory(3, [3, 2])
        self.assertEqual(len(testbed), 2)

        testbed.add(24)
        self.assertEqual(len(testbed), 3)

        # Eviction triggered.
        testbed.add(101)
        self.assertEqual(len(testbed), 3)

        testbed.remove(101)
        self.assertEqual(len(testbed), 2)

        # Immediate eviction.
        testbed = self.factory(1, [3, 2, 42, 5])
        self.assertEqual(len(testbed), 1)

    def test_comparisons(self):
        self.assertLess(self.factory(5, [2, 3]), {1, 2, 3})
        self.assertLess(self.factory(5, [2, 3]), self.factory(3, [1, 2, 3]))
        self.assertLessEqual(self.factory(5, [2, 3]), {1, 2, 3})
        self.assertLessEqual(self.factory(5, [2, 3]), self.factory(3, [1, 2, 3]))
        self.assertGreater(self.factory(3, [1, 2, 3]), {2, 3})
        self.assertGreater(self.factory(3, [1, 2, 3]), self.factory(5, [2, 3]))
        self.assertGreaterEqual(self.factory(3, [1, 2, 3]), {2, 3})
        self.assertGreaterEqual(self.factory(3, [1, 2, 3]), self.factory(5, [2, 3]))
        self.assertFalse(self.factory(3, [1, 2, 3]) < {2, 3})
        self.assertFalse(self.factory(3, [1, 2, 3]) < self.factory(5, [2, 3]))
        self.assertFalse(self.factory(5, [2, 3]) > {1, 2, 3})
        self.assertFalse(self.factory(5, [2, 3]) > self.factory(3, [1, 2, 3]))
        self.assertFalse(self.factory(3, [1, 2, 3]) <= {2, 3})
        self.assertFalse(self.factory(3, [1, 2, 3]) <= self.factory(5, [2, 3]))
        self.assertFalse(self.factory(5, [2, 3]) >= {1, 2, 3})
        self.assertFalse(self.factory(5, [2, 3]) >= self.factory(3, [1, 2, 3]))

        # Evictions should never add elements.
        self.assertLess(self.factory(2, [1, 2, 3]), {1, 2, 3})
        self.assertLess(self.factory(2, [1, 2, 3]), self.factory(5, [1, 2, 3]))
        self.assertLessEqual(self.factory(2, [1, 2, 3]), {1, 2, 3})
        self.assertLessEqual(self.factory(2, [1, 2, 3]), self.factory(5, [1, 2, 3]))
        self.assertFalse(self.factory(5, [1, 2, 3]) < self.factory(3, [1, 2, 3]))
        self.assertGreater(self.factory(5, [1, 2, 3]), self.factory(2, [1, 2, 3]))
        self.assertGreaterEqual(self.factory(5, [1, 2, 3]), self.factory(2, [1, 2, 3]))

        self.assertEqual(self.factory(5, [2, 3]), {2, 3})
        self.assertEqual(self.factory(5, [2, 3]), self.factory(5, [2, 3]))
        self.assertEqual(self.factory(5, [2, 3]), self.factory(2, [2, 3]))
        self.assertLessEqual(self.factory(5, [2, 3]), {2, 3})
        self.assertLessEqual(self.factory(5, [2, 3]), self.factory(5, [2, 3]))
        self.assertLessEqual(self.factory(5, [2, 3]), self.factory(2, [2, 3]))
        self.assertGreaterEqual(self.factory(5, [2, 3]), {2, 3})
        self.assertGreaterEqual(self.factory(5, [2, 3]), self.factory(5, [2, 3]))
        self.assertGreaterEqual(self.factory(5, [2, 3]), self.factory(2, [2, 3]))
        self.assertNotEqual(self.factory(5, [2, 3]), self.factory(1, [2, 3]))
        self.assertFalse(self.factory(5, [2, 3]) != {2, 3})
        self.assertFalse(self.factory(5, [2, 3]) != self.factory(5, [2, 3]))
        self.assertFalse(self.factory(5, [2, 3]) != self.factory(2, [2, 3]))
        self.assertFalse(self.factory(5, [2, 3]) == self.factory(1, [2, 3]))

        self.assertNotEqual(self.factory(5, [2, 3]), {1, 2, 3})
        self.assertNotEqual(self.factory(5, [2, 3]), self.factory(3, [1, 2, 3]))
        self.assertFalse(self.factory(5, [2, 3]) == {1, 2, 3})
        self.assertFalse(self.factory(5, [2, 3]) == self.factory(3, [1, 2, 3]))

        # Disjoint relations (all false) can only be expressed with explicit operators.
        self.assertFalse(self.factory(3, [10, 12]) < {2, 3})
        self.assertFalse(self.factory(3, [10, 12]) < self.factory(5, [2, 3]))
        self.assertFalse(self.factory(3, [10, 12]) <= {2, 3})
        self.assertFalse(self.factory(3, [10, 12]) <= self.factory(5, [2, 3]))
        self.assertFalse(self.factory(3, [10, 12]) > {2, 3})
        self.assertFalse(self.factory(3, [10, 12]) > self.factory(5, [2, 3]))
        self.assertFalse(self.factory(3, [10, 12]) >= {2, 3})
        self.assertFalse(self.factory(3, [10, 12]) >= self.factory(5, [2, 3]))

    def test_and(self):
        testbed = self.factory(3, [2, 3])
        anded = testbed & {1, 2, 3}
        anded2 = {1, 2, 3} & testbed
        testbed &= {1, 2, 3}
        self.assertIsInstance(anded, type(self.factory(0)))
        self.assertEqual(anded.max_size, 3)
        self.assertEqual(anded, {2, 3})
        self.assertEqual(testbed, anded)
        self.assertIsInstance(anded2, set)
        self.assertEqual(anded2, {2, 3})

        testbed = self.factory(2, [2, 3])
        anded = testbed & self.factory(3, [1, 2])
        anded2 = self.factory(3, [1, 2]) & testbed
        testbed &= self.factory(3, [1, 2])
        self.assertIsInstance(anded, type(self.factory(0)))
        self.assertEqual(anded.max_size, 2)
        self.assertEqual(anded, {2})
        self.assertEqual(testbed, anded)
        self.assertIsInstance(anded2, type(self.factory(0)))
        self.assertEqual(anded2.max_size, 3)
        self.assertEqual(anded2, {2})

        testbed = self.factory(3, [1, 2, 3])
        anded = testbed & self.factory(2, [10, 11])
        anded2 = self.factory(2, [10, 11]) & testbed
        testbed &= self.factory(2, [10, 11])
        self.assertIsInstance(anded, type(self.factory(0)))
        self.assertEqual(anded.max_size, 3)
        self.assertEqual(anded, set())
        self.assertEqual(testbed, anded)
        self.assertIsInstance(anded2, type(self.factory(0)))
        self.assertEqual(anded2.max_size, 2)
        self.assertEqual(anded2, set())

    def test_or(self):
        testbed = self.factory(3, [2, 3])
        ored = testbed | {1, 2, 3}
        ored2 = {1, 2, 3} | testbed
        testbed |= {1, 2, 3}
        self.assertIsInstance(ored, type(self.factory(0)))
        self.assertEqual(ored.max_size, 3)
        self.assertEqual(ored, {1, 2, 3})
        self.assertEqual(testbed, ored)
        self.assertIsInstance(ored2, set)
        self.assertEqual(ored2, {1, 2, 3})

        testbed = self.factory(2, [2, 3])
        ored = testbed | self.factory(3, [1, 2])
        ored2 = self.factory(3, [1, 2]) | testbed
        testbed |= self.factory(3, [1, 2])
        self.assertIsInstance(ored, type(self.factory(0)))
        self.assertEqual(ored.max_size, 2)
        # New elements take precedence
        self.assertEqual(ored, {1, 2})
        self.assertEqual(testbed, ored)
        self.assertIsInstance(ored2, type(self.factory(0)))
        self.assertEqual(ored2.max_size, 3)
        self.assertEqual(ored2, {1, 2, 3})

        testbed = self.factory(3, [1, 2, 3])
        ored = testbed | self.factory(2, [10, 11])
        ored2 = self.factory(2, [10, 11]) | testbed
        testbed |= self.factory(2, [10, 11])
        self.assertIsInstance(ored, type(self.factory(0)))
        self.assertEqual(ored.max_size, 3)
        self.assertEqual(len(ored), 3)
        # New elements take precedence
        self.assertGreater(ored, {10, 11})
        # testbed need not equal ored if eviction strategy is non-deterministic
        self.assertEqual(testbed.max_size, 3)
        self.assertEqual(len(testbed), 3)
        self.assertGreater(testbed, {10, 11})
        self.assertIsInstance(ored2, type(self.factory(0)))
        self.assertEqual(ored2.max_size, 2)
        self.assertEqual(len(ored2), 2)
        # New elements take precedence
        self.assertLess(ored2, {1, 2, 3})

    def test_sub(self):
        testbed = self.factory(3, [2, 3])
        diffed = testbed - {1, 2, 3}
        diffed2 = {1, 2, 3} - testbed
        testbed -= {1, 2, 3}
        self.assertIsInstance(diffed, type(self.factory(0)))
        self.assertEqual(diffed.max_size, 3)
        self.assertEqual(diffed, set())
        self.assertEqual(testbed, diffed)
        self.assertIsInstance(diffed2, set)
        self.assertEqual(diffed2, {1})

        testbed = self.factory(2, [2, 3])
        diffed = testbed - self.factory(3, [1, 2])
        diffed2 = self.factory(3, [1, 2]) - testbed
        testbed -= self.factory(3, [1, 2])
        self.assertIsInstance(diffed, type(self.factory(0)))
        self.assertEqual(diffed.max_size, 2)
        self.assertEqual(diffed, {3})
        self.assertEqual(testbed, diffed)
        self.assertIsInstance(diffed2, type(self.factory(0)))
        self.assertEqual(diffed2.max_size, 3)
        self.assertEqual(diffed2, {1})

        testbed = self.factory(3, [1, 2, 3])
        diffed = testbed - self.factory(2, [10, 11])
        diffed2 = self.factory(2, [10, 11]) - testbed
        testbed -= self.factory(2, [10, 11])
        self.assertIsInstance(diffed, type(self.factory(0)))
        self.assertEqual(diffed.max_size, 3)
        self.assertEqual(diffed, {1, 2, 3})
        self.assertEqual(testbed, diffed)
        self.assertIsInstance(diffed2, type(self.factory(0)))
        self.assertEqual(diffed2.max_size, 2)
        self.assertEqual(diffed2, {10, 11})

    def test_xor(self):
        testbed = self.factory(3, [2, 3])
        xored = testbed ^ {1, 2, 3}
        xored2 = {1, 2, 3} ^ testbed
        testbed ^= {1, 2, 3}
        self.assertIsInstance(xored, type(self.factory(0)))
        self.assertEqual(xored.max_size, 3)
        self.assertEqual(xored, {1})
        self.assertEqual(testbed, xored)
        self.assertIsInstance(xored2, set)
        self.assertEqual(xored2, {1})

        testbed = self.factory(2, [2, 3])
        xored = testbed ^ self.factory(3, [1, 2])
        xored2 = self.factory(3, [1, 2]) ^ testbed
        testbed ^= self.factory(3, [1, 2])
        self.assertIsInstance(xored, type(self.factory(0)))
        self.assertEqual(xored.max_size, 2)
        self.assertEqual(len(xored), 2)
        self.assertEqual(xored, {1, 3})
        self.assertEqual(testbed, xored)
        self.assertIsInstance(xored2, type(self.factory(0)))
        self.assertEqual(xored2.max_size, 3)
        self.assertEqual(xored2, {1, 3})

        testbed = self.factory(2, [2, 3])
        xored = testbed ^ self.factory(3, [1, 2, 4])
        xored2 = self.factory(3, [1, 2, 4]) ^ testbed
        testbed ^= self.factory(3, [1, 2, 4])
        self.assertIsInstance(xored, type(self.factory(0)))
        self.assertEqual(xored.max_size, 2)
        self.assertEqual(len(xored), 2)
        # New elements take precedence
        self.assertEqual(xored, {1, 4})
        self.assertEqual(testbed, xored)
        self.assertIsInstance(xored2, type(self.factory(0)))
        self.assertEqual(xored2.max_size, 3)
        self.assertEqual(xored2, {1, 3, 4})

        testbed = self.factory(3, [1, 2, 3])
        xored = testbed ^ self.factory(2, [10, 11])
        xored2 = self.factory(2, [10, 11]) ^ testbed
        testbed ^= self.factory(2, [10, 11])
        self.assertIsInstance(xored, type(self.factory(0)))
        self.assertEqual(xored.max_size, 3)
        self.assertEqual(len(xored), 3)
        self.assertLess(xored, {1, 2, 3, 10, 11})
        # New elements take precedence
        self.assertGreater(xored, {10, 11})
        # testbed need not equal xored if eviction strategy is non-deterministic
        self.assertEqual(testbed.max_size, 3)
        self.assertEqual(len(testbed), 3)
        self.assertLess(testbed, {1, 2, 3, 10, 11})
        self.assertGreater(testbed, {10, 11})
        self.assertIsInstance(xored2, type(self.factory(0)))
        self.assertEqual(xored2.max_size, 2)
        self.assertEqual(len(xored2), 2)
        # New elements take precedence
        self.assertLess(xored2, {1, 2, 3})

    def test_isdisjoint(self):
        self.assertFalse(self.factory(3, [2, 3]).isdisjoint({1, 2, 3}))
        self.assertFalse(self.factory(2, [2, 3]).isdisjoint(self.factory(3, [1, 2])))
        self.assertTrue(self.factory(2, [2, 3]).isdisjoint({10, 11}))
        self.assertTrue(self.factory(2, [2, 3]).isdisjoint(self.factory(2, [10, 11])))

    def test_add(self):
        testbed = self.factory(2)
        self.assertEqual(testbed.max_size, 2)
        self.assertEqual(testbed, set())

        evicted = testbed.add(42)
        self.assertIsNone(evicted)
        self.assertEqual(testbed.max_size, 2)
        self.assertEqual(testbed, {42})

        evicted = testbed.add(32)
        self.assertIsNone(evicted)
        self.assertEqual(testbed.max_size, 2)
        self.assertEqual(testbed, {32, 42})

        evicted = testbed.add(101)
        self.assertIsNotNone(evicted)
        self.assertEqual(testbed.max_size, 2)
        self.assertIn(101, testbed)
        self.assertEqual(testbed, {32, 42, 101} - {evicted})

        # Duplicate insert should not cause eviction
        old_state = set(testbed)
        evicted = testbed.add(101)
        self.assertIsNone(evicted)
        self.assertEqual(testbed.max_size, 2)
        self.assertEqual(testbed, old_state)

    def test_discard(self):
        testbed = self.factory(3, {32, 42, 102})
        self.assertEqual(testbed.max_size, 3)
        self.assertEqual(testbed, {32, 42, 102})

        testbed.discard(42)
        self.assertEqual(testbed.max_size, 3)
        self.assertEqual(testbed, {32, 102})

        # Nothing to remove
        testbed.discard(101)
        self.assertEqual(testbed.max_size, 3)
        self.assertEqual(testbed, {32, 102})

        # History of removed datasets should not cause evictions
        evicted = testbed.add(101)
        self.assertIsNone(evicted)
        self.assertEqual(testbed.max_size, 3)
        self.assertEqual(testbed, {32, 101, 102})

    def test_clear(self):
        testbed = self.factory(2, {32, 31})
        self.assertEqual(testbed.max_size, 2)
        self.assertEqual(testbed, {32, 31})

        testbed.clear()
        self.assertEqual(testbed.max_size, 2)
        self.assertEqual(testbed, set())

        # History of removed datasets should not cause evictions
        evicted = testbed.add(101)
        self.assertIsNone(evicted)
        evicted = testbed.add(102)
        self.assertIsNone(evicted)
        self.assertEqual(testbed.max_size, 2)
        self.assertEqual(testbed, {101, 102})

    def pop(self):
        testbed = self.factory(3, {32, 42, 102})
        self.assertEqual(testbed.max_size, 3)
        self.assertEqual(testbed, {32, 42, 102})

        popped = testbed.pop()
        self.assertEqual(testbed.max_size, 3)
        self.assertEqual(testbed, {32, 42, 102} - {popped})

        # History of removed datasets should not cause evictions
        evicted = testbed.add(101)
        self.assertIsNone(evicted)
        self.assertEqual(testbed.max_size, 3)
        self.assertEqual(testbed, {32, 42, 101, 102} - {popped})

        testbed = self.factory(2)
        self.assertEqual(testbed, set())
        with self.assertRaises(KeyError):
            testbed.pop()

    def test_remove(self):
        testbed = self.factory(3, {32, 42, 102})
        self.assertEqual(testbed.max_size, 3)
        self.assertEqual(testbed, {32, 42, 102})

        testbed.remove(42)
        self.assertEqual(testbed.max_size, 3)
        self.assertEqual(testbed, {32, 102})

        with self.assertRaises(KeyError):
            testbed.remove(101)

        # History of removed datasets should not cause evictions
        evicted = testbed.add(101)
        self.assertIsNone(evicted)
        self.assertEqual(testbed.max_size, 3)
        self.assertEqual(testbed, {32, 101, 102})


class RandomReplacementSetTest(unittest.TestCase, EvictingSetTest):
    @property
    def factory(self):
        def make_set(max_size, elements=frozenset()):
            # Variable seed gives better coverage of edge cases.
            return RandomReplacementSet(max_size, elements, seed=abs(hash(self.id())))
        return make_set

    def test_constructor(self):
        # Legal, if of dubious utility
        testbed = RandomReplacementSet(0)
        self.assertFalse(testbed)
        testbed.add(42)
        self.assertFalse(testbed)

        with self.assertRaises(ValueError):
            RandomReplacementSet(-1)

        testbed = RandomReplacementSet(2, {1, 2, 3, 4, 5})
        self.assertTrue(testbed)
        self.assertEqual(len(testbed), 2)
        self.assertLess(testbed, {1, 2, 3, 4, 5})
