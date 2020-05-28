# Copyright 2019 Curtin University. All Rights Reserved.
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

import os
import random
import unittest
import unittest
from unittest.mock import patch
from unittest.mock import Mock
from academic_observatory.utils import unique_id
from academic_observatory.utils.config_utils import observatory_home, telescope_path, SubFolder, ObservatoryConfig


class TestConfigUtils(unittest.TestCase):

    def test_observatory_home(self):
        # Create subdir
        path = observatory_home(unique_id(str(random.random())))
        self.assertTrue(os.path.exists(path))

        # Make sure we don't remove the home directory!
        if path != observatory_home():
            os.removedirs(path)

    @patch('academic_observatory.utils.config_utils.pathlib.Path.home')
    def test_telescope_path(self, mock_pathlib_home):
        # Mock getting home path
        home_path = '/tmp/'
        mock_pathlib_home.return_value = home_path

        # The name of the telescope to create and expected root folder
        telescope_name = 'grid'
        root_path = os.path.join(home_path, '.observatory', 'data', 'telescopes', telescope_name)

        # Create subdir
        path_downloaded = telescope_path(telescope_name, SubFolder.downloaded)
        expected = os.path.join(root_path, SubFolder.downloaded.value)
        self.assertEqual(expected, path_downloaded)
        self.assertTrue(os.path.exists(path_downloaded))

        # Create subdir
        path_extracted = telescope_path(telescope_name, SubFolder.extracted)
        expected = os.path.join(root_path, SubFolder.extracted.value)
        self.assertEqual(expected, path_extracted)
        self.assertTrue(os.path.exists(path_extracted))

        # Create subdir
        path_transformed = telescope_path(telescope_name, SubFolder.transformed)
        expected = os.path.join(root_path, SubFolder.transformed.value)
        self.assertEqual(expected, path_transformed)
        self.assertTrue(os.path.exists(path_transformed))

        for path in [path_downloaded, path_extracted, path_transformed]:
            os.removedirs(path)
