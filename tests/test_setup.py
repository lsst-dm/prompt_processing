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

from activator.setup import ServiceSetup


class ServiceSetupTest(unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.addCleanup(ServiceSetup.reset)

    def test_empty(self):
        # Correct behavior is a no-op, but impossible to prove a negative
        ServiceSetup.run_init_checks()

    def test_one_func(self):
        func1 = unittest.mock.Mock()

        ServiceSetup.check_on_init(func1)
        func1.assert_not_called()
        ServiceSetup.run_init_checks()
        func1.assert_called_once_with()

        func1.reset_mock()
        ServiceSetup.reset()
        ServiceSetup.run_init_checks()
        func1.assert_not_called()

    def test_two_func(self):
        func1 = unittest.mock.Mock()
        func2 = unittest.mock.Mock()

        ServiceSetup.check_on_init(func1)
        ServiceSetup.check_on_init(func2)
        ServiceSetup.run_init_checks()
        func1.assert_called_once_with()
        func2.assert_called_once_with()

        func1.reset_mock()
        func2.reset_mock()
        ServiceSetup.reset()
        ServiceSetup.run_init_checks()
        func1.assert_not_called()
        func2.assert_not_called()
