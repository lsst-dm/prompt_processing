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


__all__ = ["PipelinesConfig"]


import collections.abc
import os
import re

from .visit import FannedOutVisit


class PipelinesConfig:
    """A pipeline configuration for the Prompt Processing service.

    This class provides the execution framework with a simple interface for
    identifying the pipeline to execute. It attempts to abstract the details of
    which factors affect the choice of pipeline to make it easier to add new
    features in the future.

    Parameters
    ----------
    config : `str`
        A string describing pipeline selection criteria. The current format is
        a space-delimited list of mappings, each of which has the format
        ``(survey="<survey>")=<pipeline>``. The pipeline path may contain
        environment variables. No key or value may contain the "=" character.
        See examples below.

    Notes
    -----
    While it is not expected that there will ever be more than one
    PipelinesConfig instance in a program's lifetime, this class is *not* a
    singleton and objects must be passed explicitly to the code that
    needs them.

    Examples
    --------
    A single-survey config:

    >>> PipelinesConfig('(survey="TestSurvey")=/etc/pipelines/SingleFrame.yaml')  # doctest: +ELLIPSIS
    <config.PipelinesConfig object at 0x...>

    A config with multiple surveys, and environment variables:

    >>> PipelinesConfig('(survey="TestSurvey")=/etc/pipelines/ApPipe.yaml '
    ...                 '(survey="Camera Test")=${AP_PIPE_DIR}/pipelines/LSSTComCam/Isr.yaml '
    ...                 '(survey="")=${AP_PIPE_DIR}/pipelines/LSSTComCam/Isr.yaml ')
    ... # doctest: +ELLIPSIS
    <config.PipelinesConfig object at 0x...>
    """

    def __init__(self, config: str):
        if not config:
            raise ValueError("Must configure at least one pipeline.")

        self._mapping = self._parse_config(config)

    @staticmethod
    def _parse_config(config: str) -> collections.abc.Mapping:
        """Turn a config string into structured config information.

        Parameters
        ----------
        config : `str`
            A string describing pipeline selection criteria. The current format
            is a space-delimited list of mappings, each of which has the format
            '(survey="<survey>")=<pipeline>'. The pipeline path may contain
            environment variables. No key or value may contain the "="
            character.

        Returns
        -------
        config : mapping [`str`, `str`]
            A mapping from the survey type to the pipeline to run for that
            survey. A more complex key or container type may be needed in the
            future, if other pipeline selection criteria are added.

        Raises
        ------
        ValueError
            Raised if the string cannot be parsed.
        """
        # Use regex instead of str.split, in case keys or values also have spaces.
        node = re.compile(r'\s*\(survey="(?P<survey>[\w\s]*)"\)='
                          r'(?P<filename>[-\w./${} ]*[-\w./${}])(?:\s+|$)')

        items = {}
        pos = 0
        match = node.match(config, pos)
        while match:
            items[match['survey']] = match['filename']

            pos = match.end()
            match = node.match(config, pos)
        if pos != len(config):
            raise ValueError(f"Unexpected text at position {pos}: '{config[pos:]}'.")

        return items

    def get_pipeline_file(self, visit: FannedOutVisit) -> str:
        """Identify the pipeline to be run, based on the provided visit.

        Parameters
        ----------
        visit : `activator.visit.FannedOutVisit`
            The visit for which a pipeline must be selected.

        Returns
        -------
        pipeline : `str`
            A path to a configured pipeline file.
        """
        try:
            return os.path.expandvars(self._mapping[visit.survey])
        except KeyError as e:
            raise RuntimeError(f"Unsupported survey: {visit.survey}") from e
