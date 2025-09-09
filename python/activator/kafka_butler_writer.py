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

from typing import Literal

from confluent_kafka import Producer
import pydantic

from lsst.daf.butler import (
    Butler,
    ButlerConfig,
    DatasetRef,
    SerializedDimensionRecord,
    SerializedFileDataset,
)
from lsst.daf.butler._rubin.file_datasets import transfer_datasets_to_datastore

from .middleware_interface import ButlerWriter, GroupedDimensionRecords


class KafkaButlerWriter(ButlerWriter):
    """
    Writes Butler outputs to the central repository by copying datasets
    to their destination in S3, and sending a Kafka message to
    butler-writer-service asking it to ingest the files into the Butler
    database.

    Parameters
    ----------
    producer : `confluent_kafka.Producer`
        Kafka producer that will be uses to send messages to
        butler-writer-service.
    output_topic : `str`
        Name of the Kafka topic messages will be written to.
    output_repo : `str`
        Butler repository label or path to Butler configuration file for
        the central repository to which we will write datasets.
        The Butler datastore configuration is loaded from this to determine
        the output paths for files being written to S3.
    """

    def __init__(self, producer: Producer, *, output_topic: str, output_repo: str) -> None:
        self._producer = producer
        self._output_topic = output_topic
        self._output_butler_config = ButlerConfig(output_repo)

    def transfer_outputs(
        self, local_butler: Butler, dimension_records: GroupedDimensionRecords, datasets: list[DatasetRef]
    ) -> list[DatasetRef]:
        # Copy files to their final location in the central Butler's datastore,
        # and retrieve metadata needed to ingest them into the central
        # Butler database.
        file_datasets = transfer_datasets_to_datastore(local_butler, self._output_butler_config, datasets)

        # Serialize Butler data as a JSON string.
        event = PromptProcessingOutputEvent(
            type="pp-output",
            dimension_records=_serialize_dimension_records(dimension_records),
            datasets=[dataset.to_simple() for dataset in file_datasets],
        )
        message = event.model_dump_json()

        self._producer.produce(self._output_topic, message)
        self._producer.flush()

        return datasets


class PromptProcessingOutputEvent(pydantic.BaseModel):
    type: Literal["pp-output"]
    dimension_records: list[SerializedDimensionRecord]
    datasets: list[SerializedFileDataset]


def _serialize_dimension_records(grouped_records: GroupedDimensionRecords) -> list[SerializedDimensionRecord]:
    output = []
    for records in grouped_records.values():
        for item in records:
            output.append(item.to_simple())
    return output
