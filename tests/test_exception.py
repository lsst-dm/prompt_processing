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

from activator.exception import GracefulShutdownInterrupt, TimeoutInterrupt, \
    NonRetriableError, RetriableError


class NonRetriableErrorTest(unittest.TestCase):
    def setUp(self):
        super().setUp()

        self.inner = RuntimeError("Foo!")

    def test_raise_chained(self):
        try:
            raise NonRetriableError("Cannot compute!") from self.inner
        except NonRetriableError as e:
            self.assertIs(e.nested, self.inner)

    def test_raise_context(self):
        try:
            try:
                raise self.inner
            except Exception:
                raise NonRetriableError("Cannot compute!")
        except NonRetriableError as e:
            self.assertIs(e.nested, self.inner)

    def test_raise_orphaned(self):
        try:
            raise NonRetriableError("Cannot compute!")
        except NonRetriableError as e:
            self.assertIs(e.nested, None)

        try:
            raise NonRetriableError("Cannot compute!") from None
        except NonRetriableError as e:
            self.assertIs(e.nested, None)

        try:
            try:
                raise self.inner
            except Exception:
                raise NonRetriableError("Cannot compute!") from None
        except NonRetriableError as e:
            self.assertIs(e.nested, None)


class RetriableErrorTest(unittest.TestCase):
    def setUp(self):
        super().setUp()

        self.inner = RuntimeError("Foo!")

    def test_raise_chained(self):
        try:
            raise RetriableError("Cannot compute!") from self.inner
        except RetriableError as e:
            self.assertIs(e.nested, self.inner)

    def test_raise_context(self):
        try:
            try:
                raise self.inner
            except Exception:
                raise RetriableError("Cannot compute!")
        except RetriableError as e:
            self.assertIs(e.nested, self.inner)

    def test_raise_orphaned(self):
        try:
            raise RetriableError("Cannot compute!")
        except RetriableError as e:
            self.assertIs(e.nested, None)

        try:
            raise RetriableError("Cannot compute!") from None
        except RetriableError as e:
            self.assertIs(e.nested, None)

        try:
            try:
                raise self.inner
            except Exception:
                raise RetriableError("Cannot compute!") from None
        except RetriableError as e:
            self.assertIs(e.nested, None)


class GracefulShutdownInterruptTest(unittest.TestCase):
    def test_catchable(self):
        try:
            raise GracefulShutdownInterrupt("Last call!")
        except GracefulShutdownInterrupt:
            pass
        else:
            self.fail("Did not catch GracefulShutdownInterrupt.")

    def test_uncatchable(self):
        with self.assertRaises(GracefulShutdownInterrupt):
            try:
                raise GracefulShutdownInterrupt("Last call!")
            except Exception:
                pass  # assertRaises should fail


class TimeoutInterruptTest(unittest.TestCase):
    def test_catchable(self):
        try:
            raise TimeoutInterrupt("What's the holdup?")
        except TimeoutInterrupt:
            pass
        else:
            self.fail("Did not catch TimeoutInterrupt.")

    def test_uncatchable(self):
        with self.assertRaises(TimeoutInterrupt):
            try:
                raise TimeoutInterrupt("What's the holdup?")
            except Exception:
                pass  # assertRaises should fail
