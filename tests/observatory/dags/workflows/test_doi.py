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

import os
import unittest

import pendulum
from airflow.exceptions import AirflowException
from google.cloud import bigquery

from observatory.dags.workflows.doi import (select_table_shard_dates,
                                            set_task_state)
from observatory.platform.utils.gc_utils import (create_bigquery_dataset, bigquery_sharded_table_id,
                                                 create_bigquery_table_from_query)
from tests.observatory.test_utils import random_id


class TestDoi(unittest.TestCase):
    """ Tests for the functions used by the Doi workflow """

    def __init__(self, *args, **kwargs):
        super(TestDoi, self).__init__(*args, **kwargs)
        self.gc_project_id: str = os.getenv('TEST_GCP_PROJECT_ID')
        self.gc_bucket_name: str = os.getenv('TEST_GCP_BUCKET_NAME')
        self.gc_bucket_location: str = os.getenv('TEST_GCP_DATA_LOCATION')

    def test_set_task_state(self):
        set_task_state(True, 'my-task-id')
        with self.assertRaises(AirflowException):
            set_task_state(False, 'my-task-id')

    def test_select_table_shard_dates(self):
        client = bigquery.Client()
        dataset_id = random_id()
        table_id = 'fundref'
        # end_date = pendulum.date(year=2019, month=5, day=1)
        release_1 = pendulum.datetime(year=2019, month=5, day=1)
        release_2 = pendulum.datetime(year=2019, month=6, day=1)
        release_3 = pendulum.datetime(year=2019, month=7, day=1)
        query = "SELECT * FROM `bigquery-public-data.labeled_patents.figures` LIMIT 1"

        try:
            create_bigquery_dataset(self.gc_project_id, dataset_id, self.gc_bucket_location)
            create_bigquery_table_from_query(query, self.gc_project_id, dataset_id,
                                             bigquery_sharded_table_id(table_id, release_1),
                                             self.gc_bucket_location)
            create_bigquery_table_from_query(query, self.gc_project_id, dataset_id,
                                             bigquery_sharded_table_id(table_id, release_2),
                                             self.gc_bucket_location)
            create_bigquery_table_from_query(query, self.gc_project_id, dataset_id,
                                             bigquery_sharded_table_id(table_id, release_3),
                                             self.gc_bucket_location)

            suffixes = select_table_shard_dates(self.gc_project_id, dataset_id, table_id, release_1)
            self.assertTrue(len(suffixes), 1)
            self.assertEqual(release_1, suffixes[0])

            suffixes = select_table_shard_dates(self.gc_project_id, dataset_id, table_id, release_2)
            self.assertTrue(len(suffixes), 1)
            self.assertEqual(release_2, suffixes[0])

            suffixes = select_table_shard_dates(self.gc_project_id, dataset_id, table_id, release_3)
            self.assertTrue(len(suffixes), 1)
            self.assertEqual(release_3, suffixes[0])

        finally:
            client.delete_dataset(dataset_id, delete_contents=True, not_found_ok=True)
