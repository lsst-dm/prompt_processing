# This file is part of prompt_prototype.
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


__all__ = ["make_pgpass"]


import os
import stat


PSQL_USER = "postgres"


def make_pgpass():
    """Create a .pgpass file that contains the service's database credentials.

    This function is designed to work in the Prompt Processing Service's docker
    container, and no other environment.

    Raises
    ------
    RuntimeError
        Raised if the database passwords cannot be found.
    """
    try:
        ip_apdb = os.environ["IP_APDB"]
        db_apdb = os.environ["DB_APDB"]
        user_apdb = os.environ.get("USER_APDB", PSQL_USER)
        pass_apdb = os.environ["PSQL_APDB_PASS"]
        ip_registry = os.environ["IP_REGISTRY"]
        db_registry = os.environ["DB_REGISTRY"]
        user_registry = os.environ.get("USER_REGISTRY", PSQL_USER)
        pass_registry = os.environ["PSQL_REGISTRY_PASS"]
    except KeyError as e:
        raise RuntimeError("Addresses and passwords have not been configured") from e

    filename = os.path.join(os.environ["HOME"], ".pgpass")
    with open(filename, mode="wt") as file:
        file.write(f"{ip_apdb}:{db_apdb}:{user_apdb}:{pass_apdb}\n")
        file.write(f"{ip_registry}:{db_registry}:{user_registry}:{pass_registry}\n")
    # Only user may access the file
    os.chmod(filename, stat.S_IRUSR)
