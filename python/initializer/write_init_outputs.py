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


__all__ = ["main", "make_parser"]


import argparse
import collections.abc
import logging
import os
import yaml

import astropy.time
import botocore.exceptions
import sqlalchemy.exc

import lsst.daf.butler
import lsst.pipe.base

from shared.config import PipelinesConfig
from shared import connect_utils
from shared.logger import setup_usdf_logger
from shared import run_utils


_log = logging.getLogger("lsst." + __name__)
_log.setLevel(logging.DEBUG)

# The (jittered) number of seconds to delay retrying connections to the central Butler.
repo_retry = float(os.environ.get("REPO_RETRY_DELAY", 30))


SQL_EXCEPTIONS = (sqlalchemy.exc.OperationalError, sqlalchemy.exc.InterfaceError)
DATASTORE_EXCEPTIONS = SQL_EXCEPTIONS + (botocore.exceptions.ClientError, )


def _config_from_yaml(yaml_string):
    """Initialize a PipelinesConfig from a YAML-formatted string.

    Parameters
    ----------
    yaml_string : `str`
        A YAML representation of the structured config. See
        `~activator.config.PipelineConfig` for details.

    Returns
    -------
    config : `activator.config.PipelineConfig`
        The corresponding config object.
    """
    return PipelinesConfig(yaml.safe_load(yaml_string))


def make_parser():
    parser = argparse.ArgumentParser()
    # Instrument, repo, pipelines configs, and apdb submitted through envvars
    # to parallel the activator. Pipelines are too complex for CLI anyway.
    parser.add_argument(
        "--deploy-id",
        required=False,
        help="Override the unique ID for this deployment. By default, an "
             "internal calculation is used.",
    )
    return parser


@connect_utils.retry(2, SQL_EXCEPTIONS, wait=repo_retry)
def _connect_butler(repo):
    """Connect to a particular repo.

    Parameters
    ----------
    repo : `str`
        The URI for the repo to connect to.
    """
    return lsst.daf.butler.Butler(repo, writeable=True)


def main(args=None):
    """Generate init-outputs for a particular Prompt Processing configuration.

    Parameters
    ----------
    args : `list` [`str`]
        The command-line arguments for this program. Defaults to `sys.argv`.
    """
    instrument_name = os.environ["RUBIN_INSTRUMENT"]
    setup_usdf_logger(labels={"instrument": instrument_name},)
    try:
        repo = os.environ["CENTRAL_REPO"]
        _log.info("Preparing init-outputs for %s in %s.", instrument_name, repo)

        parsed = make_parser().parse_args(args)
        # URI to the repository that should contain init-outputs.
        butler = _connect_butler(repo)
        # The preprocessing pipelines to execute and the conditions in which to choose them.
        pre_pipelines = _config_from_yaml(os.environ["PREPROCESSING_PIPELINES_CONFIG"])
        # The main pipelines to execute and the conditions in which to choose them.
        main_pipelines = _config_from_yaml(os.environ["MAIN_PIPELINES_CONFIG"])
        # URI to an APDB config file.
        apdb = os.environ["CONFIG_APDB"]
        deploy_id = parsed.deploy_id or run_utils.get_deployment(apdb)

        instrument = lsst.obs.base.Instrument.from_string(instrument_name, butler.registry)

        preload_run = run_utils.get_preload_run(instrument, deploy_id, _get_current_day_obs())
        butler.collections.register(preload_run, lsst.daf.butler.CollectionType.RUN)
        runs = [preload_run]
        for pipeline in pre_pipelines.get_all_pipeline_files():
            runs.append(_make_init_outputs(butler, instrument, apdb, deploy_id, pipeline))
        for pipeline in main_pipelines.get_all_pipeline_files():
            runs.append(_make_init_outputs(butler, instrument, apdb, deploy_id, pipeline))
        _log.info("Registered init-outputs in %s.", repo)

        butler.registry.refresh()  # Ensure Butler reflects new collections
        _make_output_chain(butler, instrument, runs)
        return 0
    except Exception:
        _log.exception("Failed to prepare init-outputs for %s in %s.", instrument_name, repo)
        return 1


def _get_current_day_obs() -> str:
    """Generate the current day_obs value.

    Returns
    -------
    day_obs : `str`
        The day_obs value in YYYY-MM-DD format.
    """
    return run_utils.get_day_obs(astropy.time.Time.now())


@connect_utils.retry(2, DATASTORE_EXCEPTIONS, wait=repo_retry)
def _make_init_outputs(base_butler: lsst.daf.butler.Butler,
                       instrument: lsst.obs.base.Instrument,
                       apdb: str,
                       deployment_id: str,
                       pipe_file: str,
                       ) -> str:
    """Generate the init-outputs for a particular Prompt Processing pipeline.

    The init-outputs are automatically placed in the pipeline's correct
    output run.

    Parameters
    ----------
    base_butler : `lsst.daf.butler.Butler`
        A writeable Butler pointing to the output repository.
    instrument : `lsst.obs.base.Instrument`
        The instrument for which to generate init-outputs.
    apdb : `str`
        A URI to an APDB config file.
    deployment_id : `str`
        A unique identifier to distinguish runs with potentially different
        configurations.
    pipe_file : `str`
        An absolute path to the pipeline to process.

    Returns
    -------
    run : `str`
        The name of the run collection in which the init-outputs were placed.

    Raises
    ------
    lsst.daf.butler.MissingDatasetTypeError
        Raised if the pipeline requires a dataset type that has not
        been registered.
    lsst.daf.butler.ConflictingDefinitionError
        Raised if the pipeline's definition of a dataset type does not match
        the existing one.
    """
    run = run_utils.get_output_run(instrument, deployment_id, pipe_file, _get_current_day_obs())
    output_butler = base_butler.clone(run=run)
    _log.debug("Creating init-outputs for run %s...", run)

    pipeline = lsst.pipe.base.Pipeline.fromFile(pipe_file)
    pipeline.addConfigOverride("parameters", "apdb_config", apdb)
    graph = pipeline.to_graph(output_butler.registry)
    # Don't register dataset types; automated registration in shared repos is frowned upon severely.
    graph.check_dataset_type_registrations(output_butler, include_packages=True)

    graph.init_output_run(output_butler)
    return run


@connect_utils.retry(2, SQL_EXCEPTIONS, wait=repo_retry)
def _make_output_chain(butler: lsst.daf.butler.Butler,
                       instrument: lsst.obs.base.Instrument,
                       runs: collections.abc.Sequence[str],
                       ) -> str:
    """Set up a shared chained collection for the output runs.

    If the chain already exists, the new runs are prepended to the existing
    contents.

    Parameters
    ----------
    butler : `lsst.daf.butler.Butler`
        A writeable Butler pointing to the target repository.
    instrument : `lsst.obs.base.Instrument`
        The instrument for which to generate init-outputs.
    runs : sequence [`str`]
        The runs to add to the chain, in search order.

    Returns
    -------
    chain : `str`
        The newly created chained collection.
    """
    chain = run_utils.get_output_chain(instrument, _get_current_day_obs())
    _log.info("Creating output chain %s...", chain)

    butler.collections.register(chain, lsst.daf.butler.CollectionType.CHAINED)
    butler.collections.prepend_chain(chain, runs)

    return chain
