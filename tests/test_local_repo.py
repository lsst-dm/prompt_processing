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
import os.path
import random
import tempfile
import unittest
import unittest.mock

import lsst.afw.image as afw_image
import lsst.daf.butler as daf_butler
import lsst.daf.butler.tests as butler_tests
import lsst.obs.base

import shared.run_utils
import activator.caching
from activator.local_repo import LocalRepo
import activator.repo_tracker

from mock_central_repo import TestRepo


class LocalRepoTest(unittest.TestCase):
    data_dir = os.path.join(os.path.abspath(os.path.dirname(__file__)), "data")
    central_repo = os.path.join(data_dir, "central_repo")

    def _known_refs(self, butler):
        """Identify the loadable datasets in the test repo.

        Parameters
        ----------
        butler : `lsst.daf.butler.Butler`
            A Butler that serves as the source for the refs.

        Returns
        -------
        refs : set [`lsst.daf.butler.DatasetRef`]
            The datasets suitable for preload emulation tests.
        """
        types = {"camera", "skyMap", "bias", "flat", "pretrainedModelPackage", "template_coadd",
                 }
        return set(butler.query_all_datasets(collections="*", name=types, find_first=False))

    def setUp(self):
        # Reproducible across Python upgrades
        random.seed(hash(self.id()), version=2)

        self.central_butler = daf_butler.Butler(self.central_repo, writeable=False, inferDefaults=False)
        self.addCleanup(self.central_butler.close)
        self.local_space = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        # TemporaryDirectory warns on leaks
        self.addCleanup(self.local_space.cleanup)

        self.testbed = LocalRepo(self.local_space.name, self.central_butler, TestRepo.instname)
        self.addCleanup(self.testbed.close)

        self.instrument = lsst.obs.base.Instrument.fromName("LSSTCam", registry=self.central_butler.registry)

    def test_init_errors(self):
        with self.assertRaises(OSError):
            LocalRepo("/not/a/real/path", self.central_butler, TestRepo.instname)
        with self.assertRaises(ValueError):
            LocalRepo(self.local_space.name, self.central_butler, "NotACam")

    def test_repo(self):
        # Is self.local_space a prefix of self.testbed._repo?
        self.assertEqual(os.path.commonpath([self.testbed._repo.name, self.local_space.name]),
                         self.local_space.name)

    def test_close(self):
        repo_dir = self.testbed._repo.name
        self.assertTrue(os.path.exists(repo_dir))
        self.testbed.close()
        self.assertFalse(os.path.exists(repo_dir))
        with self.assertRaises(RuntimeError):
            self.testbed.butler
        # Should be idempotent
        self.testbed.close()
        self.assertFalse(os.path.exists(repo_dir))
        with self.assertRaises(RuntimeError):
            self.testbed.butler

    def _check_load(self, butler, expected, exclusive=False):
        """Test that the local repo has the expected data.

        Parameters
        ----------
        butler : `lsst.daf.butler.Butler`
            The Butler to use for local repo queries.
        expected : set [`lsst.daf.butler.DatasetRef`]
            The datasets that should be present.
        exclusive : boolean, optional
            If set, it's an error for the repo to contain datasets other than
            ``expected``.
        """
        all_datasets = set(butler.query_all_datasets("*", find_first=False))
        if exclusive:
            self.assertEqual(all_datasets, expected)
        else:
            self.assertGreaterEqual(all_datasets, expected)

    def test_load_from_clean(self):
        datasets = self._known_refs(self.central_butler)
        present = set(self.testbed.load_from(self.central_butler, datasets))
        self.assertEqual(present, datasets)
        self._check_load(daf_butler.Butler(self.testbed._repo.name), datasets, exclusive=True)

    def test_load_from_partial(self):
        all_datasets = self._known_refs(self.central_butler)
        prior_datasets = set(random.sample(list(all_datasets), len(all_datasets) // 2))

        self.testbed.load_from(self.central_butler, prior_datasets)
        self._check_load(daf_butler.Butler(self.testbed._repo.name), prior_datasets, exclusive=True)

        extra = all_datasets - prior_datasets
        with unittest.mock.patch.object(self.testbed.butler,
                                        "transfer_from",
                                        wraps=self.testbed.butler.transfer_from,
                                        ) as transfer:
            present = set(self.testbed.load_from(self.central_butler, all_datasets))
            transfer.assert_called_once()
            # No unnecessary transfers
            self.assertEqual(transfer.call_args.args[1], extra)
            self.assertEqual(present, all_datasets)
        self._check_load(daf_butler.Butler(self.testbed._repo.name), all_datasets, exclusive=True)

    def test_load_from_missing(self):
        datasets = self._known_refs(self.central_butler)
        absent = list(datasets)[0].replace(id=None, run="fake_run")

        with unittest.mock.patch.object(self.testbed.butler,
                                        "transfer_from",
                                        wraps=self.testbed.butler.transfer_from,
                                        ) as transfer:
            present = set(self.testbed.load_from(self.central_butler, datasets | {absent}))
            transfer.assert_called_once()
            # Attempted to transfer `absent`
            self.assertEqual(transfer.call_args.args[1], datasets | {absent})
            self.assertEqual(present, datasets)
        self._check_load(daf_butler.Butler(self.testbed._repo.name), datasets, exclusive=True)

    def test_load_from_exclude(self):
        datasets = self._known_refs(self.central_butler)
        # For best test power, these should be datasets that are otherwise cacheable
        missing_runs = ["skymaps", "pretrained_models/dummy"]
        uncached = {ref for ref in datasets if ref.run in missing_runs}
        self.assertGreater(len(uncached), 0)

        with unittest.mock.patch.object(self.testbed.butler,
                                        "transfer_from",
                                        wraps=self.testbed.butler.transfer_from,
                                        ) as transfer:
            present = set(self.testbed.load_from(self.central_butler, datasets, no_cache_runs=missing_runs))
            transfer.assert_called_once()
            # Still attempted to transfer uncachable datasets
            self.assertEqual(transfer.call_args.args[1], datasets)
            self.assertEqual(present, datasets)
        self._check_load(daf_butler.Butler(self.testbed._repo.name), datasets, exclusive=True)
        # Don't see a cleaner way to check if a dataset is cache-managed
        for ref in uncached:
            self.assertNotIn(ref, self.testbed._cache)

    def test_load_from_overflow(self):
        all_datasets = self._known_refs(self.central_butler)
        grouped_datasets = daf_butler.DatasetRef.groupByType(all_datasets)
        # Assumed by cache retention test
        singleton_type = "pretrainedModelPackage"
        self.assertEqual(len(grouped_datasets[singleton_type]), 1)

        # Don't see a cleaner way of doing this, given that I don't want
        # a cache dependency in the API
        cache_size = 1
        mock_cache = activator.caching.DatasetCache(cache_size)
        with unittest.mock.patch.object(self.testbed, "_cache", wraps=mock_cache):
            # LocalRepo may have safeguards against overloading the cache in one sitting
            for i in range(cache_size + 1):
                generation = {refs[i] for refs in grouped_datasets.values() if i < len(refs)}
                present = set(self.testbed.load_from(self.central_butler, generation))
                self.assertEqual(present, generation)
            test_butler = daf_butler.Butler(self.testbed._repo.name)
            # Non-exclusive: final generation probably didn't update all dataset types
            self._check_load(test_butler, generation)
            # Have any datasets actually been evicted?
            with self.assertRaises(self.failureException):
                self._check_load(test_butler, all_datasets)
            # Are old datasets kept if there's no need to evict them?
            self._check_load(test_butler, set(grouped_datasets[singleton_type]))

    def test_export_calib_associations_ok(self):
        datasets = self._known_refs(self.central_butler)
        calib_chain = self.instrument.makeCalibrationCollectionName()
        # Preconditions: datasets and calib collections have been transferred
        self.testbed.load_from(self.central_butler, datasets)
        for calib_collection in self.central_butler.collections.query(
                "*", daf_butler.CollectionType.CALIBRATION):
            self.testbed.butler.collections.register(calib_collection, daf_butler.CollectionType.CALIBRATION)
            self.testbed.butler.collections.extend_chain(calib_chain, calib_collection)

        calibs = {ref for ref in datasets if ref.datasetType.isCalibration()}
        self.testbed.export_calib_associations(self.central_butler, calib_chain, calibs)

        test_butler = daf_butler.Butler(self.testbed._repo.name)
        for calib in calibs:
            # Search for the calib *through the collection* (we know the dataset itself exists)
            self.assertTrue(test_butler.exists(calib.datasetType, calib.dataId, collections=calib_chain),
                            msg=f"{calib} not found in {calib_chain}")

    def test_export_calib_associations_nodataset(self):
        datasets = self._known_refs(self.central_butler)
        calib_chain = self.instrument.makeCalibrationCollectionName()
        # Precondition: calib collections have been transferred
        for calib_collection in self.central_butler.collections.query(
                "*", daf_butler.CollectionType.CALIBRATION):
            self.testbed.butler.collections.register(calib_collection, daf_butler.CollectionType.CALIBRATION)
            self.testbed.butler.collections.extend_chain(calib_chain, calib_collection)
        # Failed precondition: datasets not transferred
        # Define dataset types to eliminate that as a variable
        for ref in datasets:
            self.testbed.butler.registry.registerDatasetType(ref.datasetType)

        calibs = {ref for ref in datasets if ref.datasetType.isCalibration()}
        with self.assertRaises(ValueError):
            self.testbed.export_calib_associations(self.central_butler, calib_chain, calibs)

    def test_export_calib_associations_nocollection(self):
        datasets = self._known_refs(self.central_butler)
        calib_chain = self.instrument.makeCalibrationCollectionName()
        # Preconditions: datasets
        self.testbed.load_from(self.central_butler, datasets)
        # Failed precondition: calib collections have not been transferred

        calibs = {ref for ref in datasets if ref.datasetType.isCalibration()}
        with self.assertRaises(daf_butler.MissingCollectionError):
            self.testbed.export_calib_associations(self.central_butler, calib_chain, calibs)

    def _check_collection(self, collection, src, dest):
        """Test whether a collection exists and has the expected properties.

        Parameters
        ----------
        collection : `str`
            The collection to test.
        src : `lsst.daf.butler.ButlerCollections`
            The source of the collection, which serves as the test oracle.
        dest : `lsst.daf.butler.ButlerCollections`
            The destination in which the collection should be reproduced from `src`.
        """
        src_info = src.get_info(collection, include_parents=False, include_summary=False)
        try:
            dest_info = dest.get_info(collection, include_parents=False, include_summary=False)
        except daf_butler.MissingCollectionError:
            self.fail(f"{collection} does not exist in destination repo.")
        # Tests name, type, doc, and children
        self.assertEqual(dest_info, src_info)

    def test_sync_collections_single(self):
        # Not one of the standard collections recognized by obs.base.Instrument
        target = "skymaps"
        self.testbed.sync_collections(self.central_butler, target)

        test_butler = daf_butler.Butler(self.testbed._repo.name)
        self._check_collection(target, self.central_butler.collections, test_butler.collections)

    def test_sync_collections_1level(self):
        target = "pretrained_models"
        self.testbed.sync_collections(self.central_butler, target)

        test_butler = daf_butler.Butler(self.testbed._repo.name)
        self._check_collection(target, self.central_butler.collections, test_butler.collections)
        for child in self.central_butler.collections.query(target, include_chains=True):
            self._check_collection(child, self.central_butler.collections, test_butler.collections)

    def test_sync_collections_2level(self):
        target = self.instrument.makeUmbrellaCollectionName()
        self.testbed.sync_collections(self.central_butler, target)

        test_butler = daf_butler.Butler(self.testbed._repo.name)
        self._check_collection(target, self.central_butler.collections, test_butler.collections)
        for child in self.central_butler.collections.get_info(target).children:
            self._check_collection(child, self.central_butler.collections, test_butler.collections)
            if self.central_butler.collections.get_info(child).type == daf_butler.CollectionType.CHAINED:
                for grandchild in self.central_butler.collections.get_info(child).children:
                    self._check_collection(grandchild,
                                           self.central_butler.collections,
                                           test_butler.collections)

    def test_sync_collections_missing(self):
        target = "DoesNotExist"
        with self.assertRaises(daf_butler.MissingCollectionError):
            self.testbed.sync_collections(self.central_butler, target)

        test_butler = daf_butler.Butler(self.testbed._repo.name)
        with self.assertRaises(daf_butler.MissingCollectionError):
            test_butler.collections.get_info(target)

    def test_clean_empty(self):
        # Should be a no-op
        self.testbed.clean()

    def test_clean_outputs(self):
        target = shared.run_utils.get_output_run(
            self.instrument, "test-deployment", "Pipeline.yaml", 20260427)

        test_butler = daf_butler.Butler(self.testbed._repo.name, writeable=True)
        test_butler.registry.registerDatasetType(daf_butler.DatasetType(
            "initial_pvi", {"instrument", "visit", "detector"}, "ExposureF", universe=test_butler.dimensions
        ))
        butler_tests.addDataIdValue(test_butler, "visit", 101)
        test_butler.collections.register(target)
        pvi = afw_image.ExposureF(10, 10)
        test_butler.put(pvi,
                        "initial_pvi",
                        {"instrument": "LSSTCam", "visit": 101, "detector": 42},
                        run=target,
                        )
        self.assertEqual(len(test_butler.query_datasets("initial_pvi", collections=target,
                                                        find_first=False, explain=False)),
                         1)

        self.testbed.clean()
        self.assertEqual(len(test_butler.query_datasets("initial_pvi", collections=target,
                                                        find_first=False, explain=False)),
                         0)

    def test_clean_raws(self):
        target = self.instrument.makeDefaultRawIngestRunName()

        # Not a proper ingest, but should be good enough
        test_butler = daf_butler.Butler(self.testbed._repo.name, writeable=True)
        test_butler.registry.registerDatasetType(daf_butler.DatasetType(
            "raw", {"instrument", "exposure", "detector"}, "ExposureF", universe=test_butler.dimensions
        ))
        butler_tests.addDataIdValue(test_butler, "exposure", 42)
        test_butler.collections.register(target)
        raw = afw_image.ExposureF(10, 10)
        test_butler.put(raw,
                        "raw",
                        {"instrument": "LSSTCam", "exposure": 42, "detector": 0},
                        run=target,
                        )
        self.assertEqual(
            len(test_butler.query_datasets("raw", collections=target, find_first=False, explain=False)),
            1)

        self.testbed.clean()
        self.assertEqual(
            len(test_butler.query_datasets("raw", collections=target, find_first=False, explain=False)),
            0)

    def test_clean_cached_evict(self):
        datasets = self._known_refs(self.central_butler)
        # Don't use load_from, because it should guarantee there are no excess cacheable datasets
        self.testbed.butler.transfer_from(self.central_butler, datasets,
                                          register_dataset_types=True, transfer_dimensions=True)
        self.assertEqual(len(self.testbed._cache), 0)

        test_butler = daf_butler.Butler(self.testbed._repo.name)
        self.assertGreater(
            len(test_butler.query_datasets("bias", collections="*", find_first=False, explain=False)),
            0)

        # Nothing is cached, so everything should be removed
        self.testbed.clean()
        self.assertEqual(
            len(test_butler.query_datasets("bias", collections="*", find_first=False, explain=False)),
            0)

    def test_clean_cached_keep(self):
        datasets = self._known_refs(self.central_butler)
        self.testbed.load_from(self.central_butler, datasets)

        test_butler = daf_butler.Butler(self.testbed._repo.name)
        cached_biases = test_butler.query_datasets("bias", collections="*", find_first=False, explain=False)
        self.assertGreater(len(cached_biases), 0)

        # repo should already be in sync with the cache
        self.testbed.clean()
        self.assertEqual(
            set(test_butler.query_datasets("bias", collections="*", find_first=False, explain=False)),
            set(cached_biases))

    def test_clean_uncached(self):
        datasets = self._known_refs(self.central_butler)
        self.testbed.load_from(self.central_butler, datasets)

        test_butler = daf_butler.Butler(self.testbed._repo.name)
        self.assertGreater(
            len(test_butler.query_datasets("template_coadd", collections="*",
                                           find_first=False, explain=False)),
            0)

        self.testbed.clean()
        self.assertEqual(
            len(test_butler.query_datasets("template_coadd", collections="*",
                                           find_first=False, explain=False)),
            0)

    def test_butler(self):
        # Butler is a tagged class, so the only way to test writeability is to
        # actually write something
        test_type = daf_butler.DatasetType(
            "TestData",
            dimensions=set(),
            storageClass="StructuredDataDict",
            universe=self.testbed.butler.dimensions,
        )
        self.testbed.butler.registry.registerDatasetType(test_type)
        self.testbed.butler.collections.register("test_run")
        self.testbed.butler.put({"foo": "bar", "answer": 42}, test_type, {}, run="test_run")
