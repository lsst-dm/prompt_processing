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

import os
from pathlib import Path
import tempfile
import unittest
import unittest.mock

from confluent_kafka import Producer

from lsst.daf.butler import Butler

from activator.kafka_butler_writer import KafkaButlerWriter, PromptProcessingOutputEvent


class KafkaButlerWriterTest(unittest.TestCase):
    def setUp(self):
        data_dir = os.path.join(os.path.abspath(os.path.dirname(__file__)), "data")
        repository_dir = os.path.join(data_dir, "central_repo")
        self.butler = Butler(repository_dir, writeable=False)

    def test_transfer_outputs(self):
        butler = self.butler

        # Pull up a list of datasets to send
        collection = "LSSTCam/defaults"
        datasets = []
        datasets.extend(butler.query_datasets("bias", collection))
        datasets.extend(butler.query_datasets("dark", collection))
        num_datasets = len(datasets)

        # Pull up some dimension records to send
        dimension_records = {}
        dimension_records["instrument"] = butler.query_dimension_records("instrument")
        dimension_records["detector"] = butler.query_dimension_records("detector", instrument="LSSTCam")
        num_dimension_records = len(dimension_records["instrument"]) + len(dimension_records["detector"])

        kafka_producer_mock = unittest.mock.Mock(Producer)
        with tempfile.TemporaryDirectory() as output_directory:
            topic = "topic-name"
            # Simulate a transfer, writing the datasets into a temporary
            # Butler repository.
            Butler.makeRepo(output_directory)
            writer = KafkaButlerWriter(
                producer=kafka_producer_mock,
                output_topic=topic,
                output_repo=output_directory
            )
            datasets_transferred = writer.transfer_outputs(butler, dimension_records, datasets)

            self.assertEqual(datasets, datasets_transferred)
            self.assertEqual(kafka_producer_mock.produce.call_args.args[0], topic)

            # Check that the serialized metadata sent to Kafka looks correct.
            event_json = kafka_producer_mock.produce.call_args.args[1]
            model = PromptProcessingOutputEvent.model_validate_json(event_json)
            self.assertEqual(len(model.datasets), num_datasets)
            self.assertEqual(len(model.dimension_records), num_dimension_records)

            # Check that datasets were written to the output directory.
            output_files = list(Path(output_directory).rglob("*.fits"))
            self.assertEqual(len(output_files), num_datasets)
