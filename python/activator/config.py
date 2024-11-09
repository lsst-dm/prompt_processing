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


__all__ = ["PipelinesConfig"]


import collections
import collections.abc
import os
import typing

from .visit import FannedOutVisit


class PipelinesConfig:
    """A pipeline configuration for the Prompt Processing service.

    This class provides the execution framework with a simple interface for
    identifying the pipeline to execute. It attempts to abstract the details of
    which factors affect the choice of pipeline to make it easier to add new
    features in the future.

    Parameters
    ----------
    config : sequence [mapping]
        A sequence of mappings ("nodes"), each with the following keys:

        ``"survey"``
            The survey that triggers the pipelines (`str`).
        ``"pipelines"``
            A list of zero or more pipelines (sequence [`str`] or `None`). Each
            pipeline path may contain environment variables, and the list of
            pipelines may be replaced by `None` to mean no pipeline should
            be run.

        Nodes are arranged by precedence, i.e., the first node that matches a
        visit is used.

    Examples
    --------
    A single-survey, single-pipeline config:

    >>> PipelinesConfig([{"survey": "TestSurvey",
    ...                   "pipelines": ["/etc/pipelines/SingleFrame.yaml"]},
    ...                  ])  # doctest: +ELLIPSIS
    <config.PipelinesConfig object at 0x...>

    A config with multiple surveys and pipelines, and environment variables:

    >>> PipelinesConfig([{"survey": "TestSurvey",
    ...                   "pipelines": ["/etc/pipelines/ApPipe.yaml", "/etc/pipelines/ISR.yaml"]},
    ...                  {"survey": "Camera Test",
    ...                   "pipelines": ["${AP_PIPE_DIR}/pipelines/LSSTComCam/Isr.yaml"]},
    ...                  {"survey": "",
    ...                   "pipelines": ["${AP_PIPE_DIR}/pipelines/LSSTComCam/Isr.yaml"]},
    ...                  ])  # doctest: +ELLIPSIS
    <config.PipelinesConfig object at 0x...>

    A config that omits a pipeline for non-sky data:

    >>> PipelinesConfig([{"survey": "TestSurvey",
                          "pipelines": ["/etc/pipelines/ApPipe.yaml"]},
                         {"survey": "Dome Flats",
                          "pipelines": None},
    ...                  ])  # doctest: +ELLIPSIS
    <config.PipelinesConfig object at 0x...>
    """

    class _Spec:
        """A single case of which pipelines should be run in particular
        circumstances.

        Parameters
        ----------
        config : mapping [`str`]
            A config node with the same keys as documented for the
            `PipelinesConfig` constructor.
        """

        def __init__(self, config: collections.abc.Mapping[str, typing.Any]):
            specs = dict(config)
            try:
                pipelines_value = specs.pop('pipelines')
                if pipelines_value is None or pipelines_value == "None":
                    self._filenames = []
                elif isinstance(pipelines_value, str):  # Strings are sequences!
                    raise ValueError(f"Pipelines spec must be list or None, got {pipelines_value}")
                elif isinstance(pipelines_value, collections.abc.Sequence):
                    if any(not isinstance(x, str) for x in pipelines_value):
                        raise ValueError(f"Pipeline list {pipelines_value} has invalid paths.")
                    self._filenames = pipelines_value
                else:
                    raise ValueError(f"Pipelines spec must be list or None, got {pipelines_value}")

                survey_value = specs.pop('survey')
                if isinstance(survey_value, str):
                    self._survey = survey_value
                else:
                    raise ValueError(f"{survey_value} is not a valid survey name.")

                if specs:
                    raise ValueError(f"Got unexpected keywords: {specs.keys()}")
            except KeyError as e:
                raise ValueError from e

        def matches(self, visit: FannedOutVisit) -> bool:
            """Test whether a visit matches the conditions for this spec.

            Parameters
            ----------
            visit : `activator.visit.FannedOutVisit`
                The visit to test against this spec.

            Returns
            -------
            matches : `bool`
                `True` if the visit meets all conditions, `False` otherwise.
            """
            return self._survey == visit.survey

        @property
        def pipeline_files(self) -> collections.abc.Sequence[str]:
            """An ordered list of pipelines to run in this spec (sequence [`str`]).
            """
            return self._filenames

    def __init__(self, config: collections.abc.Sequence):
        if not config:
            raise ValueError("Must configure at least one pipeline.")

        self._specs = self._expand_config(config)

        for spec in self._specs:
            self._check_pipelines(spec.pipeline_files)

    @staticmethod
    def _expand_config(config: collections.abc.Sequence) -> collections.abc.Sequence[_Spec]:
        """Turn a config spec into structured config information.

        Parameters
        ----------
        config : sequence [mapping]
            A sequence of mappings, see class docs for details.

        Returns
        -------
        config : sequence [`PipelinesConfig._Spec`]
            A sequence of node objects specifying the pipeline(s) to run and the
            conditions in which to run them.

        Raises
        ------
        ValueError
            Raised if the input config is invalid.
        """
        items = []
        for node in config:
            items.append(PipelinesConfig._Spec(node))
        return items

    @staticmethod
    def _check_pipelines(pipelines: collections.abc.Sequence[str]):
        """Test the correctness of a list of pipelines.

        At present, the only test is that no two pipelines have the same
        filename, which is used as a pipeline ID elsewhere in the Prompt
        Processing service.

        Parameters
        ----------
        pipelines : sequence [`str`]

        Raises
        ------
        ValueError
            Raised if the pipeline list is invalid.
        """
        filenames = collections.Counter(os.path.splitext(os.path.basename(path))[0] for path in pipelines)
        duplicates = [filename for filename, num in filenames.items() if num > 1]
        if duplicates:
            raise ValueError(f"Pipeline names must be unique, found multiple copies of {duplicates}.")

    def get_pipeline_files(self, visit: FannedOutVisit) -> list[str]:
        """Identify the pipeline to be run, based on the provided visit.

        The first node that matches the visit is returned, and no other nodes
        are considered even if they would provide a "tighter" match.

        Parameters
        ----------
        visit : `activator.visit.FannedOutVisit`
            The visit for which a pipeline must be selected.

        Returns
        -------
        pipeline : `list` [`str`]
            Path(s) to the configured pipeline file(s). An empty list means
            that *no* pipeline should be run on this visit.
        """
        for node in self._specs:
            if node.matches(visit):
                return [os.path.expandvars(path) for path in node.pipeline_files]
        raise RuntimeError(f"Unsupported survey: {visit.survey}")
