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

__all__ = ["MockTestCase"]


import unittest


class MockTestCase(unittest.TestCase):
    """TestCase specialization with better mock support.
    """

    @classmethod
    def setupclass_patcher(cls, patcher):
        """Apply a patch to all unit tests.

        This method is intended to be run from within `setUpClass`. It avoids having
        to add an argument to every single test, but it cannot access the Mock
        from within the test.

        Parameters
        ----------
        patcher : `unittest.mock` patcher object
           An object returned from `patch`,
           `patch.object <unittest.mock.patch.object>`, or a similar callable.
        """
        patcher.start()
        cls.addClassCleanup(patcher.stop)

    def setup_patcher(self, patcher):
        """Apply a patch to all unit tests.

        This method is intended to be run from within `setUp`. It avoids having
        to add an argument to every single test, but it cannot access the Mock
        from within the test.

        Parameters
        ----------
        patcher : `unittest.mock` patcher object
           An object returned from `patch`,
           `patch.object <unittest.mock.patch.object>`, or a similar callable.
        """
        patcher.start()
        self.addCleanup(patcher.stop)
