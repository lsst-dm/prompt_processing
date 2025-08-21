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

from __future__ import annotations

__all__ = ("KafkaButlerWriter",)

from datetime import date
from typing import Literal
from uuid import uuid4

from confluent_kafka import Producer
import pydantic

from lsst.daf.butler import (
    Butler,
    DatasetRef,
    SerializedDimensionRecord,
    SerializedFileDataset,
)
from lsst.resources import ResourcePath

from .middleware_interface import ButlerWriter, GroupedDimensionRecords


class KafkaButlerWriter(ButlerWriter):
    def __init__(self, producer: Producer, *, output_topic: str, file_output_path: str) -> None:
        self._producer = producer
        self._output_topic = output_topic
        self._file_output_path = ResourcePath(file_output_path, forceDirectory=True)

    def transfer_outputs(
        self, local_butler: Butler, dimension_records: GroupedDimensionRecords, datasets: list[DatasetRef]
    ) -> list[DatasetRef]:
        # Create a subdirectory in the output root distinct to this processing
        # run.
        date_string = date.today().strftime("%Y-%m-%d")
        subdirectory = f"{date_string}/{uuid4()}/"
        output_directory = self._file_output_path.join(subdirectory, forceDirectory=True)
        # There is no such thing as a directory in S3, but the Butler complains
        # if there is not an object at the prefix of the export path.
        output_directory.mkdir()

        # Copy files to the output directory, and retrieve metadata required to
        # ingest them into the central Butler.
        file_datasets = local_butler._datastore.export(datasets, directory=output_directory, transfer="copy")

        # Serialize Butler data as a JSON string.
        event = PromptProcessingOutputEvent(
            type="pp-output",
            dimension_records=_serialize_dimension_records(dimension_records),
            datasets=[dataset.to_simple() for dataset in file_datasets],
            root_directory=subdirectory,
        )
        message = event.model_dump_json()

        self._producer.produce(self._output_topic, message)
        self._producer.flush()

        return datasets


class PromptProcessingOutputEvent(pydantic.BaseModel):
    type: Literal["pp-output"]
    root_directory: str
    dimension_records: list[SerializedDimensionRecord]
    datasets: list[SerializedFileDataset]


def _serialize_dimension_records(grouped_records: GroupedDimensionRecords) -> list[SerializedDimensionRecord]:
    output = []
    for records in grouped_records.values():
        for item in records:
            output.append(item.to_simple())
    return output
