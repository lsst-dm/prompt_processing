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

__all__ = ["create_app"]


import json
import logging
import os
import sys

import cloudevents.http
import flask

from shared.astropy import import_iers_cache
from shared.logger import setup_usdf_logger
from shared.visit import FannedOutVisit
from .activator import is_processable, process_visit
from .exception import GracefulShutdownInterrupt, IgnorableVisit, InvalidVisitError, \
    NonRetriableError, RetriableError
from .setup import ServiceSetup


# The short name for the instrument.
instrument_name = os.environ["RUBIN_INSTRUMENT"]
# The time (in seconds) after which to ignore old nextVisit messages.
visit_expire = float(os.environ.get("MESSAGE_EXPIRATION", 3600))


_log = logging.getLogger("lsst." + __name__)
_log.setLevel(logging.DEBUG)


def parse_next_visit(http_request):
    """Parse a next_visit event and extract its data.

    Parameters
    ----------
    http_request : `flask.Request`
        The request to be parsed.

    Returns
    -------
    next_visit : `shared.visit.FannedOutVisit`
        The next_visit message contained in the request.

    Raises
    ------
    ValueError
        Raised if ``http_request`` is not a valid message.
    """
    event = cloudevents.http.from_http(http_request.headers, http_request.get_data())
    if not event:
        raise ValueError("no CloudEvent received")
    if not event.data:
        raise ValueError("empty CloudEvent received")

    # Message format is determined by the nextvisit-start deployment.
    data = json.loads(event.data)
    visit = FannedOutVisit(**data)
    _log.debug("Unpacked message as %r.", visit)
    return visit


def create_app():
    try:
        setup_usdf_logger(
            labels={"instrument": instrument_name},
        )

        # Check initialization and abort early
        import_iers_cache()
        ServiceSetup.run_init_checks()

        app = flask.Flask(__name__)
        app.add_url_rule("/next-visit", view_func=next_visit_handler, methods=["POST"])
        app.register_error_handler(IgnorableVisit, skip_visit)
        app.register_error_handler(InvalidVisitError, invalid_visit)
        app.register_error_handler(RetriableError, request_retry)
        app.register_error_handler(NonRetriableError, forbid_retry)
        app.register_error_handler(500, server_error)
        _log.info("Worker ready to handle requests.")
        return app
    except Exception as e:
        _log.critical("Failed to start worker; aborting.")
        _log.exception(e)
        # gunicorn assumes exit code 3 means "Worker failed to boot", though this is not documented
        sys.exit(3)


def next_visit_handler() -> tuple[str, int]:
    """A Flask view function for handling next-visit events.

    Like all Flask handlers, this function accepts input through the
    ``flask.request`` global rather than parameters.

    Returns
    -------
    message : `str`
        The HTTP response reason to return to the client.
    status : `int`
        The HTTP response status code to return to the client.
    """
    _log.info(f"Starting next_visit_handler for {flask.request}.")

    try:
        try:
            expected_visit = parse_next_visit(flask.request)
        except ValueError as e:
            _log.exception("Bad Request")
            return f"Bad Request: {e}", 400
        if is_processable(expected_visit, visit_expire):
            process_visit(expected_visit)
            return "Pipeline executed", 200
        else:
            return "Stale request, ignoring", 403
    except GracefulShutdownInterrupt:
        # Safety net to minimize chance of interrupt propagating out of the worker.
        # Ideally, this would be a Flask.errorhandler, but Flask ignores BaseExceptions.
        _log.error("Service interrupted. Shutting down *without* syncing to the central repo.")
        return "The worker was interrupted before it could complete the request. " \
               "Retrying the request may not be safe.", 500
    finally:
        # Want to know when the handler exited for any reason.
        _log.info("next_visit handling completed.")


def invalid_visit(e: InvalidVisitError) -> tuple[str, int]:
    _log.exception("Invalid visit")
    return f"Cannot process visit: {e}.", 422


def skip_visit(e: IgnorableVisit) -> tuple[str, int]:
    _log.info("Skipping visit: %s", e)
    return f"Skipping visit without processing: {e}.", 422


def request_retry(e: RetriableError):
    error = e.nested if e.nested else e
    _log.error("Processing failed but can be retried: ", exc_info=error)
    # Service unavailable is not quite right, but no better standard response
    response = flask.make_response(
        f"The server could not process the request, but it can be retried: {error}",
        503,
        {"Retry-After": 30})
    return response


def forbid_retry(e: NonRetriableError) -> tuple[str, int]:
    error = e.nested if e.nested else e
    _log.error("Processing failed: ", exc_info=error)
    return f"An error occurred during processing: {error}.\nThe system's state has " \
           "permanently changed, so this request should **NOT** be retried.", 500


def server_error(e: Exception) -> tuple[str, int]:
    _log.exception("An error occurred during a request.")
    return (
        f"""
    An internal error occurred: <pre>{e}</pre>
    See logs for full stacktrace.
    """,
        500,
    )
