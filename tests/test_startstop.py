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
import unittest.mock

from activator.startstop import ServiceManager


class ServiceManagerTest(unittest.TestCase):
    def setUp(self):
        super().setUp()
        ServiceManager.reset()
        self.addCleanup(ServiceManager.reset)

    def test_init_empty(self):
        # Correct behavior is a no-op, but impossible to prove a negative
        ServiceManager.run_init_checks()

    def test_init_one_func(self):
        func1 = unittest.mock.Mock()

        ServiceManager.check_on_init(func1)
        func1.assert_not_called()
        ServiceManager.run_init_checks()
        func1.assert_called_once_with()

        func1.reset_mock()
        ServiceManager.reset()
        ServiceManager.run_init_checks()
        func1.assert_not_called()

    def test_init_two_func(self):
        func1 = unittest.mock.Mock()
        func2 = unittest.mock.Mock()

        ServiceManager.check_on_init(func1)
        ServiceManager.check_on_init(func2)
        ServiceManager.run_init_checks()
        func1.assert_called_once_with()
        func2.assert_called_once_with()

        func1.reset_mock()
        func2.reset_mock()
        ServiceManager.reset()
        ServiceManager.run_init_checks()
        func1.assert_not_called()
        func2.assert_not_called()

    def test_cleanup_empty(self):
        # Correct behavior is a no-op, but impossible to prove a negative
        ServiceManager.run_cleanups()

    def test_cleanup_one_func(self):
        closer1 = unittest.mock.Mock(name="closer1")
        rv1 = unittest.mock.Mock(finalize=closer1, name="obj1")
        factory1 = unittest.mock.Mock(return_value=rv1, name="factory1")

        wrapped1 = ServiceManager.clean_on_exit(closer=rv1.finalize)(factory1)
        closer1.assert_not_called()
        ServiceManager.run_cleanups()
        closer1.assert_not_called()  # wrapped1 has not been called, no object to register
        obj1 = wrapped1()
        ServiceManager.run_cleanups()
        closer1.assert_called_once_with(obj1)

        closer1.reset_mock()
        ServiceManager.reset()
        ServiceManager.run_cleanups()
        closer1.assert_not_called()

    def test_cleanup_two_funcs(self):
        closer1 = unittest.mock.Mock()
        rv1 = unittest.mock.Mock(finalize=closer1, name="obj1")
        factory1 = unittest.mock.Mock(return_value=rv1, name="factory1")
        closer2 = unittest.mock.Mock()
        rv2 = unittest.mock.Mock(finalize=closer2, name="obj2")
        factory2 = unittest.mock.Mock(return_value=rv2, name="factory2")
        # To make it easier to test relative order
        parent = unittest.mock.Mock()
        parent.attach_mock(closer1, "closer1")
        parent.attach_mock(closer2, "closer2")

        wrapped1 = ServiceManager.clean_on_exit(closer=rv1.finalize)(factory1)
        wrapped2 = ServiceManager.clean_on_exit(closer=rv2.finalize)(factory2)

        # Should clean up obj2, then obj1
        obj1 = wrapped1()
        obj2 = wrapped2()
        ServiceManager.run_cleanups()
        closer1.assert_called_once_with(obj1)
        closer2.assert_called_once_with(obj2)
        self.assertEqual(parent.method_calls, [unittest.mock.call.closer2(obj2),
                                               unittest.mock.call.closer1(obj1),
                                               ])

        # Should clean up obj1, then obj2
        parent.reset_mock()
        ServiceManager.reset()
        obj2 = wrapped2()
        obj1 = wrapped1()
        ServiceManager.run_cleanups()
        closer1.assert_called_once_with(obj1)
        closer2.assert_called_once_with(obj2)
        self.assertEqual(parent.method_calls, [unittest.mock.call.closer1(obj1),
                                               unittest.mock.call.closer2(obj2),
                                               ])

        parent.reset_mock()
        ServiceManager.reset()
        ServiceManager.run_cleanups()
        closer1.assert_not_called()
        closer2.assert_not_called()
