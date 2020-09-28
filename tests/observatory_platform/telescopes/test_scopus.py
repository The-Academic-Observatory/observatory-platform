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

from elsapy.elsclient import ElsClient
from queue import Queue
from typing import List, Dict
from click.testing import CliRunner
from unittest.mock import patch

from observatory_platform.telescopes.scopus import (
    ScopusRelease,
    ScopusUtility,
    ScopusUtilWorker,
)

from observatory_platform.utils.telescope_utils import (
    build_schedule,
)


class TestScopusRelease(unittest.TestCase):
    """ Test the ScopusRelease class. """

    @patch('observatory_platform.utils.config_utils.airflow.models.Variable.get', return_value='teletest')
    def test_init(self, tele):
        """ Test initialisation. """

        obj = ScopusRelease(inst_id='inst_id', scopus_inst_id=['scopus_inst_id'],
                            release_date=pendulum.date(1984, 1, 1),
                            start_date=pendulum.date(2000, 5, 1),
                            end_date=pendulum.date(2000, 1, 1), project_id='project_id',
                            download_bucket_name='download_bucket', transform_bucket_name='transform_bucket',
                            data_location='data_location', schema_ver='schema_ver')

        self.assertEqual(obj.inst_id, 'inst_id')
        self.assertEqual(obj.scopus_inst_id[0], 'scopus_inst_id')
        self.assertEqual(obj.release_date, pendulum.date(1984, 1, 1))
        self.assertEqual(obj.start_date, pendulum.date(2000, 5, 1))
        self.assertEqual(obj.end_date, pendulum.date(2000, 1, 1))
        self.assertEqual(obj.project_id, 'project_id')
        self.assertEqual(obj.download_path, 'teletest/telescopes/download/scopus')
        self.assertEqual(obj.transform_path, 'teletest/telescopes/transform/scopus')
        self.assertEqual(obj.telescope_path, 'telescopes/scopus/1984-01-01')
        self.assertEqual(obj.download_bucket_name, 'download_bucket')
        self.assertEqual(obj.transform_bucket_name, 'transform_bucket')
        self.assertEqual(obj.data_location, 'data_location')
        self.assertEqual(obj.schema_ver, 'schema_ver')
        self.assertEqual(tele.call_count, 1)


class TestScopusUtilWorker(unittest.TestCase):
    """ Test the ScopusUtilWorker class. """

    def test_init(self):
        """ Test the constructor. """

        now = pendulum.now('UTC')
        client = ElsClient('api_key')
        client_id = 1
        worker = ScopusUtilWorker(client_id, client, now)
        self.assertEqual(now, worker.quota_reset_date)
        self.assertEqual(client_id, worker.client_id)
        self.assertTrue(isinstance(worker.client, ElsClient))


class TestScopusUtility(unittest.TestCase):
    """ Test the SCOPUS utility class. """

    def __init__(self, *args, **kwargs):
        super(TestScopusUtility, self).__init__(*args, **kwargs)
        self.scopus_2019_09_01_path = '/tmp/scopus_curtin_2019_09_01.yml'

    def test_build_query(self):
        """ Test query builder. """

        scopus_inst_id = ["test1", "test2"]
        period = (pendulum.date(2018, 10, 1), pendulum.date(2019, 2, 1))
        query = ScopusUtility.build_query(scopus_inst_id, period)
        query_truth = '(AF-ID(test1) OR AF-ID(test2)) AND PUBDATETXT("October 2018" or "November 2018" or "December 2018" or "January 2019" or "February 2019")'
        self.assertEqual(query, query_truth)

    # def test_download_scopus_period(self):
    #     """ Test downloading of a period. Can help record cassettes too. """
    #
    #     with vcr.use_cassette(self.scopus_2019_09_01_path):
    #         scopus_inst_id = ["60031226"]  # Curtin University
    #         api_key = "test_key"
    #         client = ElsClient(api_key)
    #         period = (pendulum.date(2020, 9, 1), pendulum.date(2020,9,30))
    #         save_file = ScopusUtility.download_scopus_period(client, "scopus_curtin", period, scopus_inst_id, "/tmp")
    #         self.assertTrue(save_file != '')

    # def test_sequential(self):
    #     """ Sequential download test. """
    #
    #     now = pendulum.now('UTC')
    #     workers = list()
    #
    #     workers.append(ScopusUtilWorker(1, ElsClient('api_key1'), now))
    #     workers.append(ScopusUtilWorker(2, ElsClient('api_key2'), now))
    #
    #     scopus_inst_id = ["60031226"]
    #     schedule = build_schedule(pendulum.date(1990, 5, 1), pendulum.date(1990, 9, 1))
    #     taskq = Queue()
    #     for period in schedule:
    #         taskq.put(period)
    #     download_path = '/tmp'
    #
    #     saved_files = ScopusUtility.download_sequential(workers, taskq, 'scopus_curtin', scopus_inst_id, download_path)
    #     # self.assertEqual(len(saved_files), 5)

    # def test_parallel(self):
    #     """ Parallel download test. """
    #
    #     now = pendulum.now('UTC')
    #     workers = list()
    #
    #     workers.append(ScopusUtilWorker(1, ElsClient('api_key1'), now))
    #     workers.append(ScopusUtilWorker(2, ElsClient('api_key2'), now))
    #
    #     scopus_inst_id = ["60031226"]
    #     schedule = build_schedule(pendulum.date(1990, 5, 1), pendulum.date(1990, 9, 1))
    #     taskq = Queue()
    #     for period in schedule:
    #         taskq.put(period)
    #     download_path = '/tmp'
    #
    #     saved_files = ScopusUtility.download_parallel(workers, taskq, 'scopus_curtin', scopus_inst_id, download_path)
    #     self.assertEqual(len(saved_files), 5)

    def test_download_snapshot(self):

        release = ScopusRelease(inst_id='curtin', scopus_inst_id=['60031226'],
                            release_date=pendulum.date(2020, 1, 1),
                            start_date=pendulum.date(1990, 5, 1),
                            end_date=pendulum.date(1990, 9, 1), project_id='project_id',
                            download_bucket_name='download_bucket', transform_bucket_name='transform_bucket',
                            data_location='data_location', schema_ver='schema_ver')

        api_keys = []

        ScopusUtility.download_snapshot(api_keys, release, 'sequential')

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
