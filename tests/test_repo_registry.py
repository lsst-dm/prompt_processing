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

import functools
import os.path
import tempfile
import unittest

from activator.repo_registry import LocalRepoRegistry


def patched_registry(method):
    @functools.wraps(method)
    def wrapper(self, *args, **kwargs):
        # Need separate registry if running tests in same process
        # Need separate location if running tests in parallel
        with tempfile.TemporaryDirectory() as reg_dir, \
            unittest.mock.patch("activator.repo_registry.LocalRepoRegistry._REGISTRY_FILE",
                                os.path.join(reg_dir, "test.csv")):
            registry = LocalRepoRegistry()
            with unittest.mock.patch("activator.repo_registry.LocalRepoRegistry.get",
                                     return_value=registry):
                registry.init_registry()
                try:
                    return method(self, *args, **kwargs)
                finally:
                    registry.cleanup_registry()
    return wrapper


class LocalRepoRegistryTest(unittest.TestCase):
    """Test the LocalRepoRegistry class's functionality.

    The class has a hardcoded location for the registry, and no built-in way to
    clean it up (it is designed to have pod lifetime). As a workaround, use
    `patched_registry` to decorate all tests.
    """

    @patched_registry
    def test_singleton(self):
        self.assertIs(LocalRepoRegistry.get(), LocalRepoRegistry.get())

    @patched_registry
    def test_register_pop(self):
        test_data = {42: "/foo/bar/repo",
                     101: "/baz/bak/butler.yaml",
                     }
        testbed = LocalRepoRegistry.get()

        testbed.register(101, test_data[101])
        testbed.register(42, test_data[42])
        for pid, repo in test_data.items():
            self.assertEqual(testbed.get_owner(repo), pid)
        for pid, repo in test_data.items():
            self.assertEqual(testbed.pop(pid), repo)
            with self.assertRaises(ValueError):
                testbed.get_owner(repo)

    @patched_registry
    def test_register_twice(self):
        testbed = LocalRepoRegistry.get()

        testbed.register(42, "/foo/bar/repo")
        self.assertEqual(testbed.get_owner("/foo/bar/repo"), 42)
        with self.assertRaises(ValueError):
            testbed.get_owner("/baz/bak/butler.yaml")
        with self.assertRaises(ValueError):
            testbed.register(42, "/baz/bak/butler.yaml")  # duplicate pid
        with self.assertRaises(ValueError):
            testbed.register(101, "/foo/bar/repo")  # duplicate repo
        testbed.register(101, "/baz/bak/butler.yaml")

    @patched_registry
    def test_empty_pop(self):
        testbed = LocalRepoRegistry.get()

        with self.assertRaises(ValueError):
            testbed.pop(42)
        with self.assertRaises(ValueError):
            testbed.get_owner("/foo/bar/repo")

    @patched_registry
    def test_double_pop(self):
        testbed = LocalRepoRegistry.get()

        testbed.register(42, "/foo/bar/repo")
        self.assertEqual(testbed.pop(42), "/foo/bar/repo")
        with self.assertRaises(ValueError):
            testbed.pop(42)

    @patched_registry
    def test_pop_register(self):
        testbed = LocalRepoRegistry.get()

        testbed.register(42, "/foo/bar/repo")
        self.assertEqual(testbed.pop(42), "/foo/bar/repo")
        testbed.register(42, "/baz/bak/butler.yaml")
        self.assertEqual(testbed.pop(42), "/baz/bak/butler.yaml")

    @patched_registry
    def test_get_owner_change(self):
        testbed = LocalRepoRegistry.get()

        testbed.register(42, "/foo/bar/repo")
        self.assertEqual(testbed.pop(42), "/foo/bar/repo")
        testbed.register(101, "/foo/bar/repo")
        self.assertEqual(testbed.get_owner("/foo/bar/repo"), 101)
