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

import datetime
import logging
import os
import shutil
import unittest
from typing import List, Dict
from unittest.mock import patch

import vcr
from click.testing import CliRunner

from academic_observatory.telescopes.fundref import (
    FundrefRelease,
    FundrefTelescope,
    decompress_release,
    geonames_dump,
    get_filepath_geonames,
    get_geoname_data,
    list_releases,
    parse_fundref_registry_rdf,
    recursive_funders,
    transform_release
)
from academic_observatory.utils.config_utils import telescope_path, SubFolder
from academic_observatory.utils.data_utils import _hash_file
from tests.academic_observatory.config import test_fixtures_path


class TestFundref(unittest.TestCase):
    """ Tests for the functions used by the fundref telescope """

    def __init__(self, *args, **kwargs):
        """ Constructor which sets up variables used by tests.

        :param args: arguments.
        :param kwargs: keyword arguments.
        """

        super(TestFundref, self).__init__(*args, **kwargs)

        # Fundref releases list
        self.list_fundref_releases_path = os.path.join(test_fixtures_path(), 'vcr_cassettes',
                                                       'list_fundref_releases.yaml')
        self.list_fundref_releases_hash = ''

        # Fundref test release
        self.fundref_test_path = os.path.join(test_fixtures_path(), 'telescopes', 'fundref.tar.gz')
        self.fundref_test_url = FundrefTelescope.TELESCOPE_DEBUG_URL

        self.fundref_test_date = '3000-01-01'
        self.fundref_test_download_file_name = 'fundref_3000_01_01.tar.gz'
        self.fundref_test_decompress_file_name = 'fundref_3000_01_01.rdf'
        self.fundref_test_transform_file_name = 'fundref_3000_01_01.rdf'
        self.fundref_test_download_hash = ''
        self.fundref_test_decompress_hash = ''
        self.fundref_test_transform_hash = ''

        logging.info("Check that test fixtures exist")
        self.assertTrue(os.path.isfile(self.list_fundref_releases_path))
        self.assertTrue(os.path.isfile(self.fundref_test_path))
        self.assertTrue(self.list_fundref_releases_hash,
                        _hash_file(self.list_fundref_releases_path, algorithm='md5'))
        self.assertTrue(self.fundref_test_download_hash, _hash_file(self.fundref_test_path, algorithm='md5'))

        # Turn logging to warning because vcr prints too much at info level
        logging.basicConfig()
        logging.getLogger().setLevel(logging.WARNING)

    def test_list_releases(self):
        """ Test that list releases returns a list of string with urls.

        :return: None.
        """
        with vcr.use_cassette(self.list_fundref_releases_path):
            releases = list_releases(FundrefTelescope.TELESCOPE_URL)
            self.assertIsInstance(releases, List)
            for release in releases:
                self.assertIsInstance(release, Dict)
                self.assertIn('url', release)
                self.assertIn('date', release)

    def test_release_date(self):
        """ Test that date obtained from url is string and in correct format.

        :return: None.
        """
        with vcr.use_cassette(self.list_fundref_releases_path):
            releases_list = list_releases(FundrefTelescope.TELESCOPE_URL)
            for release in releases_list:
                release = FundrefRelease(release['url'], release['date'])
                date = release.date
                self.assertIsInstance(date, str)
                self.assertTrue(datetime.datetime.strptime(date, "%Y-%m-%d"))

    @patch('academic_observatory.utils.config_utils.pathlib.Path.home')
    def test_filepath_download(self, home_mock):
        """ Test that path of downloaded file is correct for given url.

        :param home_mock: Mock observatory home path
        :return: None.
        """
        with CliRunner().isolated_filesystem():
            # Create home path and mock getting home path
            home_path = 'user-home'
            os.makedirs(home_path, exist_ok=True)
            home_mock.return_value = home_path

            with CliRunner().isolated_filesystem():
                release = FundrefRelease(self.fundref_test_url, self.fundref_test_date)
                file_path_download = release.filepath_download
                path = telescope_path(FundrefTelescope.DAG_ID, SubFolder.downloaded)
                self.assertEqual(os.path.join(path, self.fundref_test_download_file_name), file_path_download)

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
                release = FundrefRelease(self.fundref_test_url, self.fundref_test_date)
                file_path_extract = release.filepath_extract
                path = telescope_path(FundrefTelescope.DAG_ID, SubFolder.extracted)
                self.assertEqual(os.path.join(path, self.fundref_test_decompress_file_name), file_path_extract)

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
                release = FundrefRelease(self.fundref_test_url, self.fundref_test_date)
                file_path_transform = release.filepath_transform
                path = telescope_path(FundrefTelescope.DAG_ID, SubFolder.transformed)
                self.assertEqual(os.path.join(path, self.fundref_test_transform_file_name), file_path_transform)

    def test_download_release_date(self):
        """ Test that the test url contains the correct date.

        :return: None.
        """
        with CliRunner().isolated_filesystem():
            release = FundrefRelease(self.fundref_test_url, self.fundref_test_date)
            self.assertEqual(self.fundref_test_date, release.date)

    def test_decompress_release(self):
        """ Test that the release is decompressed as expected.

        :return: None.
        """

        with CliRunner().isolated_filesystem():
            release = FundrefRelease(self.fundref_test_url, self.fundref_test_date)
            # 'download' release
            shutil.copyfile(self.fundref_test_path, release.filepath_download)

            decompress_file_path = decompress_release(release)
            decompress_file_name = os.path.basename(decompress_file_path)

            self.assertTrue(os.path.exists(decompress_file_path))
            self.assertEqual(self.fundref_test_decompress_file_name, decompress_file_name)
            self.assertEqual(self.fundref_test_decompress_hash, _hash_file(decompress_file_path, algorithm='md5'))

    def test_transform_release(self):
        """ Test that the release is transformed as expected.

        :return: None.
        """

        with CliRunner().isolated_filesystem():
            release = FundrefRelease(self.fundref_test_url, self.fundref_test_date)
            # 'download' release
            shutil.copyfile(self.fundref_test_path, release.filepath_download)
            # 'download' geonames
            #TODO

            decompress_release(release)
            transform_file_path = transform_release(release)
            transform_file_name = os.path.basename(transform_file_path)

            self.assertTrue(os.path.exists(transform_file_path))
            self.assertEqual(self.fundref_test_transform_file_name, transform_file_name)
            self.assertEqual(self.fundref_test_transform_hash, _hash_file(transform_file_path, algorithm='md5'))
