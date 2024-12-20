# Copyright 2020-2024 Curtin University
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


import os

import pendulum

from observatory_platform.dataset_api import build_schedule, DatasetAPI, DatasetRelease
from observatory_platform.google.bigquery import bq_run_query
from observatory_platform.sandbox.sandbox_environment import SandboxEnvironment
from observatory_platform.sandbox.test_utils import SandboxTestCase


class TestDatasetAPI(SandboxTestCase):
    def __init__(self, *args, **kwargs):
        super(TestDatasetAPI, self).__init__(*args, **kwargs)
        self.project_id = os.getenv("TEST_GCP_PROJECT_ID")
        self.data_location = os.getenv("TEST_GCP_DATA_LOCATION")

    def test_add_dataset_release(self):
        env = SandboxEnvironment(project_id=self.project_id, data_location=self.data_location)
        bq_dataset_id = env.add_dataset(prefix="dataset_api")
        api = DatasetAPI(bq_project_id=self.project_id, bq_dataset_id=bq_dataset_id, location=self.data_location)
        with env.create():
            # Add dataset release
            dag_id = "doi_workflow"
            entity_id = "doi"

            dt = pendulum.now()
            expected = DatasetRelease(
                dag_id=dag_id,
                entity_id=entity_id,
                dag_run_id="test",
                created=dt,
                modified=dt,
                data_interval_start=dt,
                data_interval_end=dt,
                snapshot_date=dt,
                partition_date=dt,
                changefile_start_date=dt,
                changefile_end_date=dt,
                sequence_start=1,
                sequence_end=20,
                extra={"hello": "world"},
            )
            api.add_dataset_release(expected)

            # Check if dataset release added
            rows = bq_run_query(f"SELECT * FROM `{api.full_table_id}`")
            self.assertEqual(1, len(rows))
            actual = DatasetRelease.from_dict(dict(rows[0]))
            self.assertEqual(expected, actual)

    def test_get_dataset_releases(self):
        env = SandboxEnvironment(project_id=self.project_id, data_location=self.data_location)
        bq_dataset_id = env.add_dataset(prefix="dataset_api")
        api = DatasetAPI(bq_project_id=self.project_id, bq_dataset_id=bq_dataset_id, location=self.data_location)
        expected = []
        with env.create():
            # Create dataset releases
            dag_id = "doi_workflow"
            entity_id = "doi"
            for i in range(10):
                dt = pendulum.now()
                release = DatasetRelease(
                    dag_id=dag_id,
                    entity_id=entity_id,
                    dag_run_id="test",
                    created=dt,
                    modified=dt,
                )
                api.add_dataset_release(release)
                expected.append(release)

            # Sort descending order
            expected.sort(key=lambda r: r.created, reverse=True)

            # Get releases
            actual = api.get_dataset_releases(dag_id=dag_id, entity_id=entity_id)
            self.assertListEqual(expected, actual)

    def test_is_first_release(self):
        env = SandboxEnvironment(project_id=self.project_id, data_location=self.data_location)
        bq_dataset_id = env.add_dataset(prefix="dataset_api")
        api = DatasetAPI(bq_project_id=self.project_id, bq_dataset_id=bq_dataset_id, location=self.data_location)
        with env.create():
            dag_id = "doi_workflow"
            entity_id = "doi"

            # Is first release
            is_first = api.is_first_release(dag_id=dag_id, entity_id=entity_id)
            self.assertTrue(is_first)

            # Not first release
            dt = pendulum.now()
            release = DatasetRelease(
                dag_id=dag_id,
                entity_id=entity_id,
                dag_run_id="test",
                created=dt,
                modified=dt,
            )
            api.add_dataset_release(release)
            is_first = api.is_first_release(dag_id=dag_id, entity_id=entity_id)
            self.assertFalse(is_first)

    def test_get_latest_dataset_release(self):
        dag_id = "doi_workflow"
        entity_id = "doi"
        dt = pendulum.now()
        releases = [
            DatasetRelease(
                dag_id=dag_id,
                entity_id=entity_id,
                dag_run_id="test",
                created=dt,
                modified=dt,
                snapshot_date=pendulum.datetime(2022, 1, 1),
            ),
            DatasetRelease(
                dag_id=dag_id,
                entity_id=entity_id,
                dag_run_id="test",
                created=dt,
                modified=dt,
                snapshot_date=pendulum.datetime(2023, 1, 1),
            ),
            DatasetRelease(
                dag_id=dag_id,
                entity_id=entity_id,
                dag_run_id="test",
                created=dt,
                modified=dt,
                snapshot_date=pendulum.datetime(2024, 1, 1),
            ),
        ]
        env = SandboxEnvironment(project_id=self.project_id, data_location=self.data_location)
        bq_dataset_id = env.add_dataset(prefix="dataset_api")
        api = DatasetAPI(bq_project_id=self.project_id, bq_dataset_id=bq_dataset_id, location=self.data_location)

        with env.create():
            # Create dataset releases
            for release in releases:
                api.add_dataset_release(release)

            latest = api.get_latest_dataset_release(dag_id=dag_id, entity_id=entity_id, date_key="snapshot_date")
            self.assertEqual(releases[-1], latest)

    def test_build_schedule(self):
        start_date = pendulum.datetime(2021, 1, 1)
        end_date = pendulum.datetime(2021, 2, 1)
        schedule = build_schedule(start_date, end_date)
        self.assertEqual([pendulum.Period(pendulum.date(2021, 1, 1), pendulum.date(2021, 1, 31))], schedule)

        start_date = pendulum.datetime(2021, 1, 1)
        end_date = pendulum.datetime(2021, 3, 1)
        schedule = build_schedule(start_date, end_date)
        self.assertEqual(
            [
                pendulum.Period(pendulum.date(2021, 1, 1), pendulum.date(2021, 1, 31)),
                pendulum.Period(pendulum.date(2021, 2, 1), pendulum.date(2021, 2, 28)),
            ],
            schedule,
        )

        start_date = pendulum.datetime(2021, 1, 7)
        end_date = pendulum.datetime(2021, 2, 7)
        schedule = build_schedule(start_date, end_date)
        self.assertEqual([pendulum.Period(pendulum.date(2021, 1, 7), pendulum.date(2021, 2, 6))], schedule)
