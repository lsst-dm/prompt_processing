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

"""Shared definitions for retrying Butler I/O operations.

These definitions will need to be updated as Middleware behavior changes.
"""


__all__ = ["repo_retry", "SQL_EXCEPTIONS", "DATASTORE_EXCEPTIONS"]


import os

import botocore.exceptions
import sqlalchemy.exc


# TODO: rationalize config options on DM-52695
# The (jittered) number of seconds to delay retrying connections to the central Butler.
repo_retry = float(os.environ.get("REPO_RETRY_DELAY", 30))

# TODO: revisit which cases should be retried after DM-50934
# TODO: catch ButlerConnectionError once it's available
SQL_EXCEPTIONS = (sqlalchemy.exc.OperationalError, sqlalchemy.exc.InterfaceError)
DATASTORE_EXCEPTIONS = SQL_EXCEPTIONS + (botocore.exceptions.ClientError, )
