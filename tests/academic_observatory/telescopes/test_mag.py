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

import unittest


class TestMag(unittest.TestCase):
    """ Tests for the functions used by the MAG telescope """

    def __init__(self, *args, **kwargs):
        """ Constructor which sets up variables used by tests.

        :param args: arguments.
        :param kwargs: keyword arguments.
        """

        super(TestMag, self).__init__(*args, **kwargs)

    def test_list_mag_release_files(self):
        self.fail()

    def test_transform_mag_file(self):
        self.fail()

    def test_transform_mag_release(self):
        self.fail()

    def test_db_load_mag_release(self):
        self.fail()
