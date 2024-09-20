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
        ``(survey="<survey>")=[<pipelines>]``. The zero or more pipelines are
        comma-delimited, and each pipeline path may contain environment
        variables. The list may be replaced by the keyword "None" to mean no
        pipeline should be run. No key or value may contain the "=" character,
        and no two pipelines may share the same name.
        See examples below.

    Examples
    --------
    A single-survey, single-pipeline config:

    >>> PipelinesConfig('(survey="TestSurvey")=[/etc/pipelines/SingleFrame.yaml]')  # doctest: +ELLIPSIS
    <config.PipelinesConfig object at 0x...>

    A config with multiple surveys and pipelines, and environment variables:

    >>> PipelinesConfig('(survey="TestSurvey")=[/etc/pipelines/ApPipe.yaml, /etc/pipelines/ISR.yaml] '
    ...                 '(survey="Camera Test")=[${AP_PIPE_DIR}/pipelines/LSSTComCam/Isr.yaml] '
    ...                 '(survey="")=[${AP_PIPE_DIR}/pipelines/LSSTComCam/Isr.yaml] ')
    ... # doctest: +ELLIPSIS
    <config.PipelinesConfig object at 0x...>

    A config that omits a pipeline for non-sky data:

    >>> PipelinesConfig('(survey="TestSurvey")=[/etc/pipelines/ApPipe.yaml] '
    ...                 '(survey="Dome Flats")=None ')  # doctest: +ELLIPSIS
    <config.PipelinesConfig object at 0x...>
    """

    def __init__(self, config: str):
        if not config:
            raise ValueError("Must configure at least one pipeline.")

        self._mapping = self._parse_config(config)

        for pipelines in self._mapping.values():
            self._check_pipelines(pipelines)

    @staticmethod
    def _parse_config(config: str) -> collections.abc.Mapping:
        """Turn a config string into structured config information.

        Parameters
        ----------
        config : `str`
            A string describing pipeline selection criteria. The current format
            a space-delimited list of mappings, each of which has the format
            ``(survey="<survey>")=[<pipelines>]``. The zero or more pipelines
            are comma-delimited, and each pipeline path may contain environment
            variables. The list may be replaced by the keyword "None" to mean
            no pipeline should be run. No key or value may contain the "="
            character.

        Returns
        -------
        config : mapping [`str`, `list` [`str`]]
            A mapping from the survey type to the pipeline(s) to run for that
            survey. A more complex key or container type may be needed in the
            future, if other pipeline selection criteria are added.

        Raises
        ------
        ValueError
            Raised if the string cannot be parsed.
        """
        # Use regex instead of str.split, in case keys or values also have
        # spaces.
        # Allow anything between the [ ] to avoid catastrophic backtracking
        # when the input is invalid. If pickier matching is needed in the
        # future, use a separate regex for filelist instead of making node
        # more complex.
        node = re.compile(r'\s*\(survey="(?P<survey>[^"\n=]*)"\)='
                          r'(?:\[(?P<filelist>[^]]*)\]|none)(?:\s+|$)',
                          re.IGNORECASE)

        items = {}
        pos = 0
        match = node.match(config, pos)
        while match:
            if match['filelist']:  # exclude None and ""; latter gives unexpected behavior with split
                filenames = []
                for file in match['filelist'].split(','):
                    file = file.strip()
                    if "\n" in file:
                        raise ValueError(f"Unexpected newline in '{file}'.")
                    filenames.append(file)
                items[match['survey']] = filenames
            else:
                items[match['survey']] = []

            pos = match.end()
            match = node.match(config, pos)
        if pos != len(config):
            raise ValueError(f"Unexpected text at position {pos}: '{config[pos:]}'.")

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
        try:
            values = self._mapping[visit.survey]
        except KeyError as e:
            raise RuntimeError(f"Unsupported survey: {visit.survey}") from e
        return [os.path.expandvars(path) for path in values]
