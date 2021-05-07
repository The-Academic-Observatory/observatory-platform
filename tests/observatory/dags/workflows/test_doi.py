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
from datetime import datetime

import pendulum
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

from observatory.dags.workflows.doi import DoiWorkflow
from observatory.platform.utils.test_utils import (
    ObservatoryEnvironment,
    ObservatoryTestCase,
    module_file_path,
)


def make_dummy_dag(dag_id: str, execution_date: datetime):
    with DAG(
            dag_id=dag_id,
            schedule_interval="@weekly",
            default_args={"owner": "airflow", "start_date": execution_date},
            catchup=False,
    ) as dag:
        task1 = DummyOperator(task_id="dummy_task")

    return dag


class TestDoi(ObservatoryTestCase):
    """ Tests for the functions used by the Doi workflow """

    def __init__(self, *args, **kwargs):
        super(TestDoi, self).__init__(*args, **kwargs)
        self.gcp_project_id: str = os.getenv("TEST_GCP_PROJECT_ID")
        self.gcp_bucket_name: str = os.getenv("TEST_GCP_BUCKET_NAME")
        self.gcp_data_location: str = os.getenv("TEST_GCP_DATA_LOCATION")

    def test_set_task_state(self):
        set_task_state(True, 'my-task-id')
        with self.assertRaises(AirflowException):
            set_task_state(False, 'my-task-id')

    def test_dag_structure(self):
        """Test that the DOI DAG has the correct structure.

        :return: None
        """

        dag = DoiWorkflow().make_dag()
        self.assert_dag_structure(
            {
                "crossref_metadata_sensor": ["check_dependencies"],
                "fundref_sensor": ["check_dependencies"],
                "geonames_sensor": ["check_dependencies"],
                "grid_sensor": ["check_dependencies"],
                "mag_sensor": ["check_dependencies"],
                "open_citations_sensor": ["check_dependencies"],
                "unpaywall_sensor": ["check_dependencies"],
                "check_dependencies": ["create_datasets"],
                "create_datasets": [
                    "extend_grid",
                    "aggregate_crossref_events",
                    "aggregate_orcid",
                    "aggregate_mag",
                    "aggregate_unpaywall",
                    "extend_crossref_funders",
                    "aggregate_open_citations",
                    "aggregate_wos",
                    "aggregate_scopus",
                ],
                "extend_grid": ["create_doi"],
                "aggregate_crossref_events": ["create_doi"],
                "aggregate_orcid": ["create_doi"],
                "aggregate_mag": ["create_doi"],
                "aggregate_unpaywall": ["create_doi"],
                "extend_crossref_funders": ["create_doi"],
                "aggregate_open_citations": ["create_doi"],
                "aggregate_wos": ["create_doi"],
                "aggregate_scopus": ["create_doi"],
                "create_doi": [
                    "create_country",
                    "create_funder",
                    "create_group",
                    "create_institution",
                    "create_author",
                    "create_journal",
                    "create_publisher",
                    "create_region",
                    "create_subregion",
                ],
                "create_country": ["copy_to_dashboards"],
                "create_funder": ["copy_to_dashboards"],
                "create_group": ["copy_to_dashboards"],
                "create_institution": ["copy_to_dashboards"],
                "create_author": ["copy_to_dashboards"],
                "create_journal": ["copy_to_dashboards"],
                "create_publisher": ["copy_to_dashboards"],
                "create_region": ["copy_to_dashboards"],
                "create_subregion": ["copy_to_dashboards"],
                "copy_to_dashboards": ["create_dashboard_views"],
                "create_dashboard_views": [],
            },
            dag,
        )

    def test_dag_load(self):
        """Test that the DOI can be loaded from a DAG bag.

        :return: None
        """

        env = ObservatoryEnvironment(self.gcp_project_id, self.gcp_data_location)
        with env.create():
            dag_file = os.path.join(module_file_path("observatory.dags.dags"), "doi.py")
            self.assert_dag_load("doi", dag_file)

    def test_telescope(self):
        """Test the DOI telescope end to end.

        :return: None.
        """

        env = ObservatoryEnvironment(self.gcp_project_id, self.gcp_data_location)

        with env.create():
            # Make dag
            doi_dag = DoiWorkflow().make_dag()

            # Test that sensors do go into the 'up_for_reschedule' state as the DAGs that they wait for haven't run
            execution_date = pendulum.datetime(year=2020, month=11, day=1)
            expected_state = 'up_for_reschedule'
            with env.create_dag_run(doi_dag, execution_date):
                for task_id in DoiWorkflow.SENSOR_DAG_IDS:
                    ti = env.run_task(f'{task_id}_sensor', doi_dag, execution_date=execution_date)
                    self.assertEqual(expected_state, ti.state)

            # Run Dummy Dags
            execution_date = pendulum.datetime(year=2020, month=11, day=2)
            expected_state = 'success'
            for dag_id in DoiWorkflow.SENSOR_DAG_IDS:
                dag = make_dummy_dag(dag_id, execution_date)
                with env.create_dag_run(dag, execution_date):
                    # Running all of a DAGs tasks sets the DAG to finished
                    ti = env.run_task('dummy_task', dag, execution_date=execution_date)
                    self.assertEqual(expected_state, ti.state)

            # Test that sensors go into 'success' state as the DAGs that they are waiting for have finished
            with env.create_dag_run(doi_dag, execution_date):
                for task_id in DoiWorkflow.SENSOR_DAG_IDS:
                    ti = env.run_task(f'{task_id}_sensor', doi_dag, execution_date=execution_date)
                    self.assertEqual(expected_state, ti.state)
