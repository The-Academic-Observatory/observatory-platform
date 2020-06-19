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

# Author: James Diprose

import glob
import logging
import os
import subprocess
import unittest
from subprocess import Popen
from typing import List, Dict

import vcr
from click.testing import CliRunner

from academic_observatory.telescopes.grid import list_grid_releases, download_grid_release, extract_grid_release, \
    transform_grid_release
from academic_observatory.utils.data_utils import _hash_file
from academic_observatory.utils.proc_utils import wait_for_process
from tests.academic_observatory.config import test_fixtures_path


def gzip_file_crc(file_path: str) -> str:
    """ Get the crc of a gzip file.

    :param file_path: the path to the file.
    :return: the crc.
    """

    proc: Popen = subprocess.Popen(['gzip', '-vl', file_path], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    output, error = wait_for_process(proc)
    return output.splitlines()[1].split(' ')[1].strip()


class TestGrid(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(TestGrid, self).__init__(*args, **kwargs)

        # Logging and file isolation
        logging.basicConfig()
        logging.getLogger().setLevel(logging.WARNING)
        self.runner = CliRunner()

        # Paths
        self.vcr_cassettes_path = os.path.join(test_fixtures_path(), 'vcr_cassettes')
        self.list_grid_releases_path = os.path.join(self.vcr_cassettes_path, 'list_grid_releases.yaml')
        self.work_dir = '.'

        # GRID Release  2015-09-22
        self.grid_2015_09_22_article_id = 1553267
        self.grid_2015_09_22_title = 'GRID release 2015-09-22 in JSON'
        self.grid_2015_09_22_path = os.path.join(self.vcr_cassettes_path, 'download_grid_release_2015-09-22.yaml')

        # GRID Release 2020-03-15
        self.grid_2020_03_15_article_id = 12022722
        self.grid_2020_03_15_title = 'GRID release 2020-03-15'
        self.grid_2020_03_15_path = os.path.join(self.vcr_cassettes_path, 'download_grid_release_2020-03-15.yaml')
        self.grid_2020_03_15_download_expected_hash = '3d300affce1666ac50b8d945c6ca4c5a'
        self.grid_2020_03_15_transform_version = 'release_2020_03_15'
        self.grid_2020_03_15_transform_file_name = 'grid_release_2020_03_15.jsonl.gz'
        self.grid_2020_03_15_transform_crc = '77bc8585'

    def test_list_grid_releases(self):
        # Check that list grid releases returns a list of dictionaries with keys that we use
        with vcr.use_cassette(self.list_grid_releases_path):
            releases = list_grid_releases()
            self.assertIsInstance(releases, List)
            for release in releases:
                self.assertIsInstance(release, Dict)
                self.assertIn('id', release)
                self.assertIn('title', release)

    def test_download_grid_release(self):
        # Download a specific GRID release and check that it has expected md5 sum
        with self.runner.isolated_filesystem():
            with vcr.use_cassette(self.grid_2020_03_15_path):
                files = download_grid_release(self.work_dir, self.grid_2020_03_15_article_id,
                                              self.grid_2020_03_15_title)
                # Check that returned downloads has correct length
                self.assertEqual(1, len(files))

                # Check that file has expected hash
                file_path = files[0]
                self.assertTrue(os.path.exists(file_path))
                self.assertEqual(self.grid_2020_03_15_download_expected_hash, _hash_file(file_path, algorithm='md5'))

    def test_extract_grid_release(self):
        # Test with a GRID release that is an unzipped JSON file (some of the earlier releases)
        with self.runner.isolated_filesystem():
            with vcr.use_cassette(self.grid_2015_09_22_path):
                files = download_grid_release(self.work_dir, self.grid_2015_09_22_article_id,
                                              self.grid_2015_09_22_title)
                release_extracted_path = extract_grid_release(files[0], self.work_dir)
                self.assertTrue(os.path.exists(release_extracted_path))
                file_paths = glob.glob(os.path.join(release_extracted_path, '*.json'))
                self.assertTrue(len(file_paths))
                self.assertTrue(os.path.isfile(file_paths[0]))

        # Test with GRID release that is a .zip file (more common)
        with self.runner.isolated_filesystem():
            with vcr.use_cassette(self.grid_2020_03_15_path):
                files = download_grid_release(self.work_dir, self.grid_2020_03_15_article_id,
                                              self.grid_2020_03_15_title)
                release_extracted_path = extract_grid_release(files[0], self.work_dir)
                self.assertTrue(os.path.exists(release_extracted_path))
                self.assertTrue(os.path.isfile(os.path.join(release_extracted_path, 'grid.json')))

    def test_transform_grid_release(self):
        # Test that the GRID release is transformed as expected
        with self.runner.isolated_filesystem():
            with vcr.use_cassette(self.grid_2020_03_15_path):
                # Get data
                files = download_grid_release(self.work_dir, self.grid_2020_03_15_article_id,
                                              self.grid_2020_03_15_title)
                release_extracted_path = extract_grid_release(files[0], self.work_dir)

                # Transform and check data
                release_json_file = os.path.join(release_extracted_path, 'grid.json')
                version, file_name, file_path = transform_grid_release(release_json_file, self.work_dir)
                self.assertEqual(self.grid_2020_03_15_transform_version, version)
                self.assertEqual(self.grid_2020_03_15_transform_file_name, file_name)
                self.assertTrue(os.path.exists(file_path))
                gzip_crc = gzip_file_crc(file_path)
                self.assertEqual(self.grid_2020_03_15_transform_crc, gzip_crc)
