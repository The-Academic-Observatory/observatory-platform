# Copyright 2020 Curtin University
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Author: Aniek Roelofs

import logging
import os
import shutil
import unittest
from unittest.mock import patch

from click.testing import CliRunner

from academic_observatory.telescopes.geonames import (
    GeonamesRelease,
    GeonamesTelescope,
    transform_release
)
from academic_observatory.utils.config_utils import telescope_path, SubFolder
from academic_observatory.utils.data_utils import _hash_file
from tests.academic_observatory.config import test_fixtures_path


class TestGeonames(unittest.TestCase):
    """ Tests for the functions used by the geonames telescope """

    def __init__(self, *args, **kwargs):
        """ Constructor which sets up variables used by tests.

        :param args: arguments.
        :param kwargs: keyword arguments.
        """

        super(TestGeonames, self).__init__(*args, **kwargs)

        # Geonames test release
        self.geonames_test_path = os.path.join(test_fixtures_path(), 'telescopes', 'geonames.txt')
        # self.geonames_test_url = GeonamesTelescope.TELESCOPE_DEBUG_URL
        self.geonames_test_date = '3000-01-01'
        self.geonames_test_download_file_name = 'geonames_3000_01_01'
        self.geonames_test_decompress_file_name = 'geonames_3000_01_01.txt'
        self.geonames_test_transform_file_name = 'geonames_3000_01_01.jsonl'
        self.geonames_test_decompress_hash = 'de1bf005df4840d16faf598999d72051'
        self.geonames_test_transform_hash = 'aa8089bb450010f55eaa2fcfbd52fcd5'

        logging.info("Check that test fixtures exist")
        self.assertTrue(os.path.isfile(self.geonames_test_path))
        self.assertTrue(self.geonames_test_decompress_hash, _hash_file(self.geonames_test_path, algorithm='md5'))

        # Turn logging to warning because vcr prints too much at info level
        logging.basicConfig()
        logging.getLogger().setLevel(logging.WARNING)

    @patch('academic_observatory.utils.config_utils.pathlib.Path.home')
    def test_filepath_download(self, home_mock):
        """ Test that path of downloaded file is correct.

        :param home_mock: Mock observatory home path
        :return: None.
        """
        with CliRunner().isolated_filesystem():
            # Create home path and mock getting home path
            home_path = 'user-home'
            os.makedirs(home_path, exist_ok=True)
            home_mock.return_value = home_path

            with CliRunner().isolated_filesystem():
                geonames_release = GeonamesRelease(self.geonames_test_date)
                file_path_download = geonames_release.filepath_download
                path = telescope_path(GeonamesTelescope.DAG_ID, SubFolder.downloaded)
                self.assertEqual(os.path.join(path, self.geonames_test_download_file_name), file_path_download)

    @patch('academic_observatory.utils.config_utils.pathlib.Path.home')
    def test_filepath_decompress(self, home_mock):
        """ Test that path of downloaded file is correct.

        :param home_mock: Mock observatory home path
        :return: None.
        """
        with CliRunner().isolated_filesystem():
            # Create home path and mock getting home path
            home_path = 'user-home'
            os.makedirs(home_path, exist_ok=True)
            home_mock.return_value = home_path

            with CliRunner().isolated_filesystem():
                geonames_release = GeonamesRelease(self.geonames_test_date)
                file_path_decompress = geonames_release.filepath_extract
                path = telescope_path(GeonamesTelescope.DAG_ID, SubFolder.extracted)
                self.assertEqual(os.path.join(path, self.geonames_test_decompress_file_name), file_path_decompress)

    @patch('academic_observatory.utils.config_utils.pathlib.Path.home')
    def test_filepath_extract(self, home_mock):
        """ Test that path of decompressed/extracted file is correct for given url.

        :param home_mock: Mock observatory home path
        :return: None.
        """
        with CliRunner().isolated_filesystem():
            # Create home path and mock getting home path
            home_path = 'user-home'
            os.makedirs(home_path, exist_ok=True)
            home_mock.return_value = home_path

            with CliRunner().isolated_filesystem():
                geonames_release = GeonamesRelease(self.geonames_test_date)
                file_path_extract = geonames_release.filepath_extract
                path = telescope_path(GeonamesTelescope.DAG_ID, SubFolder.extracted)
                self.assertEqual(os.path.join(path, self.geonames_test_decompress_file_name), file_path_extract)

    @patch('academic_observatory.utils.config_utils.pathlib.Path.home')
    def test_filepath_transform(self, home_mock):
        """ Test that path of transformed file is correct for given url.

        :param home_mock: Mock observatory home path
        :return: None.
        """
        with CliRunner().isolated_filesystem():
            # Create home path and mock getting home path
            home_path = 'user-home'
            os.makedirs(home_path, exist_ok=True)
            home_mock.return_value = home_path

            with CliRunner().isolated_filesystem():
                geonames_release = GeonamesRelease(self.geonames_test_date)
                file_path_transform = geonames_release.filepath_transform
                path = telescope_path(GeonamesTelescope.DAG_ID, SubFolder.transformed)
                self.assertEqual(os.path.join(path, self.geonames_test_transform_file_name), file_path_transform)

    def test_transform_release(self):
        """ Test that the release is transformed as expected.

        :return: None.
        """

        with CliRunner().isolated_filesystem():
            geonames_release = GeonamesRelease(self.geonames_test_date)
            shutil.copyfile(self.geonames_test_path, geonames_release.filepath_extract)

            transform_file_path = transform_release(geonames_release)
            transform_file_name = os.path.basename(transform_file_path)

            self.assertTrue(os.path.exists(transform_file_path))
            self.assertEqual(self.geonames_test_transform_file_name, transform_file_name)
            self.assertEqual(self.geonames_test_transform_hash, _hash_file(transform_file_path, algorithm='md5'))
