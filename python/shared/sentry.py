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

__all__ = ["init_sentry"]

import logging
import os

_log = logging.getLogger("lsst." + __name__)


def init_sentry(integrations=None):
    """Initialize the Sentry SDK if SENTRY_DSN is configured.

    Reads SENTRY_DSN and SENTRY_ENVIRONMENT from the environment. If
    SENTRY_DSN is absent or empty the function returns without doing
    anything, so environments without the secret are unaffected.

    Parameters
    ----------
    integrations : list, optional
        Additional ``sentry_sdk`` integrations to enable (e.g.
        ``FlaskIntegration()``). ``LoggingIntegration`` is always included.
    """
    dsn = os.environ.get("SENTRY_DSN", "")
    if not dsn:
        return

    import sentry_sdk
    from sentry_sdk.integrations.logging import LoggingIntegration

    sentry_logging = LoggingIntegration(
        level=logging.INFO,      # breadcrumb threshold
        event_level=logging.ERROR,  # captured-as-event threshold
    )
    all_integrations = [sentry_logging] + (integrations or [])

    kwargs = dict(dsn=dsn, integrations=all_integrations, traces_sample_rate=1.0)
    environment = os.environ.get("SENTRY_ENVIRONMENT", "")
    if environment:
        kwargs["environment"] = environment

    sentry_sdk.init(**kwargs)
    _log.info("Sentry initialized (environment=%r).", environment or "<unset>")
