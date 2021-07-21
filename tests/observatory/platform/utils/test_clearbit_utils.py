# Copyright 2021 Curtin University. All Rights Reserved.
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
import unittest

import vcr
from click.testing import CliRunner
from observatory.platform.utils.clearbit_utils import clearbit_download_logo
from observatory.platform.utils.file_utils import _hash_file
from observatory.platform.utils.test_utils import test_fixtures_path


class TestClearbitUtils(unittest.TestCase):
    def test_clearbit_download_logo(self):
        """ Test clearbit_download_logo """

        with CliRunner().isolated_filesystem() as t:
            with vcr.use_cassette(test_fixtures_path("vcr_cassettes", "clearbit_download_logo.yaml")):
                # Company that exists
                file_path = os.path.join(t, "blueorigin.jpg")
                success = clearbit_download_logo(company_url="blueorigin.com", file_path=file_path)
                self.assertTrue(success)
                self.assertTrue(os.path.exists(file_path))
                self.assertEqual("9e9619a1a504d55509b226ad2c29f173", _hash_file(file_path, algorithm="md5"))

                # Company that doesn't exist
                file_path = os.path.join(t, "bsmmu.jpg")
                success = clearbit_download_logo(company_url="bsmmu.edu.bd", file_path=file_path)
                self.assertFalse(success)
