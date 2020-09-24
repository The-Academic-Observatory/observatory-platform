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

# Author: Tuan Chien


import glob
import logging
import os
import pendulum
import unittest
import vcr

from typing import List, Dict
from click.testing import CliRunner
from unittest.mock import patch

from observatory_platform.telescopes.scopus import (
    ScopusRelease
)


class TestScopusRelease(unittest.TestCase):
    """ Test the ScopusRelease class. """

    # def __init__(self, *args, **kwargs):
    #     """ Constructor which sets up variables used by tests.
    #
    #     :param args: arguments.
    #     :param kwargs: keyword arguments.
    #     """
    #
    #     super(TestScopusRelease, self).__init__(*args, **kwargs)


    @patch('observatory_platform.utils.config_utils.airflow.models.Variable.get', return_value='teletest')
    def test_init(self, tele):
        """ Test initialisation. """

        obj = ScopusRelease(inst_id='inst_id', scopus_inst_id=['scopus_inst_id'],
                            release_date=pendulum.date(1984, 1, 1),
                            dag_start=pendulum.date(2000, 1, 1), project_id='project_id',
                            download_bucket_name='download_bucket', transform_bucket_name='transform_bucket',
                            data_location='data_location', schema_ver='schema_ver')


        self.assertEqual(obj.inst_id, 'inst_id')
        self.assertEqual(obj.scopus_inst_id[0], 'scopus_inst_id')
        self.assertEqual(obj.release_date, pendulum.date(1984, 1, 1))
        self.assertEqual(obj.dag_start, pendulum.date(2000, 1, 1))
        self.assertEqual(obj.project_id, 'project_id')
        self.assertEqual(obj.download_path, 'teletest/telescopes/download/scopus')
        self.assertEqual(obj.transform_path, 'teletest/telescopes/transform/scopus')
        self.assertEqual(obj.telescope_path, 'telescopes/scopus/1984-01-01')
        self.assertEqual(obj.download_bucket_name, 'download_bucket')
        self.assertEqual(obj.transform_bucket_name, 'transform_bucket')
        self.assertEqual(obj.data_location, 'data_location')
        self.assertEqual(obj.schema_ver, 'schema_ver')
        self.assertEqual(tele.call_count, 1)

# class TestScopus(unittest.TestCase):
#     """ Tests for the functions used by the SCOPUS telescope """
#
#     def __init__(self, *args, **kwargs):
#         """ Constructor which sets up variables used by tests.
#
#         :param args: arguments.
#         :param kwargs: keyword arguments.
#         """
#
#         super(TestScopus, self).__init__(*args, **kwargs)
#
#         # Paths
#         self.work_dir = '.'
#
#         # Release <date>
#
#
#         # Turn logging to warning because vcr prints too much at info level
#         logging.basicConfig()
#         logging.getLogger().setLevel(logging.WARNING)
#
#
#     def test_build_query(self):
#         """ Test query building code. """
#
