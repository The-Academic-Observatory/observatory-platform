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

# Author: Richard Hosking


import os
from unittest.mock import MagicMock, patch

import pendulum
from click.testing import CliRunner

from observatory.dags.workflows.oapen_workflow import OapenWorkflow, OapenWorkflowRelease

from observatory.platform.utils.gc_utils import (
    run_bigquery_query,
)
from observatory.platform.utils.workflow_utils import make_dag_id
from observatory.platform.utils.test_utils import (
    ObservatoryEnvironment,
    ObservatoryTestCase,
    make_dummy_dag,
)


class TestOapenWorkflow(ObservatoryTestCase):
    """
    Test the OapenWorkflow class.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.org_name = "OAPEN"
        self.gcp_project_id = "project_id"
        self.data_location = os.getenv("TESTS_DATA_LOCATION")

        # Release Object Defaults for reference
        self.ao_gcp_project_id = "academic-observatory"
        self.oapen_metadata_dataset_id = "oapen"
        self.oapen_metadata_table_id = "metadata"


    @patch("observatory.dags.workflows.oapen_workflow.OapenWorkflow.make_release")
    @patch("observatory.dags.workflows.oapen_workflow.select_table_shard_dates")
    def test_cleanup(self, mock_sel_table_suffixes, mock_mr):
        mock_sel_table_suffixes.return_value = [pendulum.datetime(2021, 1, 1)]
        with CliRunner().isolated_filesystem():
            wf = OapenWorkflow()

            mock_mr.return_value = OapenWorkflowRelease(
                dag_id="oapen_workflow_test",
                release_date=pendulum.datetime(2021, 1, 1),
                gcp_project_id=self.gcp_project_id,
                oapen_metadata_dataset_id=self.oapen_metadata_dataset_id,
                oapen_metadata_table_id=self.oapen_metadata_table_id,
            )

            release = wf.make_release(execution_date=pendulum.datetime(2021, 1, 1))
            wf.cleanup(release)

    def test_dag_structure(self):

        with CliRunner().isolated_filesystem():
            wf = OapenWorkflow()
            dag = wf.make_dag()
            self.assert_dag_structure(
                {
                    "oapen_irus_uk_oapen_sensor": ["create_onix_formatted_metadata_output_tasks"],
                    "create_onix_formatted_metadata_output_tasks": ["copy_irus_uk_release"],
                    "copy_irus_uk_release": ["create_oaebu_book_product_table"],
                    "create_oaebu_book_product_table": ["export_oaebu_table.book_product_list"],
                    "export_oaebu_table.book_product_list": ["export_oaebu_table.book_product_metrics"],
                    "export_oaebu_table.book_product_metrics": ["export_oaebu_table.book_product_metrics_country"],
                    "export_oaebu_table.book_product_metrics_country": [
                        "export_oaebu_table.book_product_metrics_institution"
                    ],
                    "export_oaebu_table.book_product_metrics_institution": [
                        "export_oaebu_table.book_product_metrics_city"
                    ],
                    "export_oaebu_table.book_product_metrics_city": [
                        "export_oaebu_table.book_product_metrics_referrer"
                    ],
                    "export_oaebu_table.book_product_metrics_referrer": [
                        "export_oaebu_table.book_product_metrics_events"
                    ],
                    "export_oaebu_table.book_product_metrics_events": [
                        "export_oaebu_table.book_product_publisher_metrics"
                    ],
                    "export_oaebu_table.book_product_publisher_metrics": [
                        "export_oaebu_table.book_product_subject_metrics"
                    ],
                    "export_oaebu_table.book_product_subject_metrics": ["export_oaebu_table.book_product_year_metrics"],
                    "export_oaebu_table.book_product_year_metrics": [
                        "export_oaebu_table.book_product_subject_year_metrics"
                    ],
                    "export_oaebu_table.book_product_subject_year_metrics": [
                        "export_oaebu_table.book_product_author_metrics"
                    ],
                    "export_oaebu_table.book_product_author_metrics": ["cleanup"],
                    "cleanup": [],
                },
                dag,
            )


class TestOapenWorkflowFunctional(ObservatoryTestCase):
    """Functionally test the workflow. """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.timestamp = pendulum.now()
        self.oapen_table_id = "oapen"

        self.data_location = os.getenv("TEST_GCP_DATA_LOCATION")

        self.ao_gcp_project_id = "academic-observatory"
        self.oapen_metadata_dataset_id = "oapen"
        self.oapen_metadata_table_id = "metadata"
        self.public_book_metadata_dataset_id = "observatory"
        self.public_book_metadata_table_id = "book"

        self.org_name = "OAPEN"
        self.gcp_project_id = os.getenv("TEST_GCP_PROJECT_ID")

        self.gcp_dataset_id = "oaebu"
        self.irus_uk_dag_id_prefix = "oapen_irus_uk"
        self.irus_uk_table_id = "oapen_irus_uk"

        self.irus_uk_dataset_id = "fixtures"


    def test_run_workflow_tests(self):
        """Functional test of the OAPEN workflow"""

        # Setup Observatory environment
        env = ObservatoryEnvironment(self.gcp_project_id, self.data_location, enable_api=False)
        org_name = self.org_name

        # Create datasets
        partner_release_date = pendulum.datetime(2021, 1, 1)
        oaebu_intermediate_dataset_id = env.add_dataset(prefix="oaebu_intermediate")
        oaebu_output_dataset_id = env.add_dataset(prefix="oaebu")
        oaebu_onix_dataset_id = env.add_dataset(prefix="oaebu_onix_dataset")
        oaebu_elastic_dataset_id = env.add_dataset(prefix="data_export")


        # Create the Observatory environment and run tests
        with env.create(task_logging=True):
            self.gcp_bucket_name = env.transform_bucket

            # Pull info from Observatory API
            gcp_project_id = (self.gcp_project_id,)

            # Setup workflow
            start_date = pendulum.datetime(year=2021, month=5, day=9)
            workflow = OapenWorkflow(start_date=start_date)

            # Make DAG
            workflow_dag = workflow.make_dag()

            # Test that sensors go into the 'up_for_reschedule' state as the DAGs that they wait for haven't run
            expected_state = "up_for_reschedule"
            with env.create_dag_run(workflow_dag, start_date):
                ti = env.run_task(
                    f"{make_dag_id(self.irus_uk_dag_id_prefix, org_name)}_sensor", workflow_dag, execution_date=start_date
                )
                self.assertEqual(expected_state, ti.state)

            # Run Dummy Dags
            expected_state = "success"
            execution_date = pendulum.datetime(year=2021, month=5, day=16)
            release_date = pendulum.datetime(year=2021, month=5, day=22)

            dag = make_dummy_dag(make_dag_id(self.irus_uk_dag_id_prefix, org_name), execution_date)
            with env.create_dag_run(dag, execution_date):
                # Running all of a DAGs tasks sets the DAG to finished
                ti = env.run_task("dummy_task", dag, execution_date=execution_date)
                self.assertEqual(expected_state, ti.state)

            # Run end to end tests for the DAG
            with env.create_dag_run(workflow_dag, execution_date):
                # Test that sensors go into 'success' state as the DAGs that they are waiting for have finished
                ti = env.run_task(
                    f"{make_dag_id(self.irus_uk_dag_id_prefix, org_name)}_sensor", workflow_dag, execution_date=execution_date
                )
                self.assertEqual(expected_state, ti.state)

                # Mock make_release
                workflow.make_release = MagicMock(
                    return_value=OapenWorkflowRelease(
                        dag_id=make_dag_id(self.irus_uk_dag_id_prefix, org_name),
                        release_date=release_date,
                        gcp_project_id=self.gcp_project_id,
                        oaebu_onix_dataset=oaebu_onix_dataset_id,
                        oaebu_dataset=oaebu_output_dataset_id,
                        oaebu_intermediate_dataset=oaebu_intermediate_dataset_id,
                        oaebu_elastic_dataset=oaebu_elastic_dataset_id,
                        irus_uk_dataset_id=self.irus_uk_dataset_id,
                    )
                )

                # Format OAPEN Metadata like ONIX to enable the next steps
                ti = env.run_task(
                    workflow.create_onix_formatted_metadata_output_tasks.__name__,
                    workflow_dag,
                    execution_date,
                )
                self.assertEqual(expected_state, ti.state)

                # Copy IRUS-UK data and add release date
                ti = env.run_task(
                    workflow.copy_irus_uk_release.__name__,
                    workflow_dag,
                    execution_date,
                )
                self.assertEqual(expected_state, ti.state)

                # Create oaebu output tables
                ti = env.run_task(
                    workflow.create_oaebu_book_product_table.__name__,
                    workflow_dag,
                    execution_date,
                )
                self.assertEqual(expected_state, ti.state)

                # Export oaebu elastic tables
                export_tables = [
                    "book_product_list",
                    "book_product_metrics",
                    "book_product_metrics_country",
                    "book_product_metrics_institution",
                    "book_product_metrics_city",
                    "book_product_metrics_referrer",
                    "book_product_metrics_events",
                    "book_product_publisher_metrics",
                    "book_product_subject_metrics",
                    "book_product_year_metrics",
                    "book_product_subject_year_metrics",
                    "book_product_author_metrics",
                ]

                for table in export_tables:
                    ti = env.run_task(
                        f"{workflow.export_oaebu_table.__name__}.{table}",
                        workflow_dag,
                        execution_date,
                    )
                    self.assertEqual(expected_state, ti.state)


                # Test conditions
                release_suffix = release_date.strftime("%Y%m%d")

                # Check records in book_product and book_product_list match
                sql = f"SELECT COUNT(*) from {self.gcp_project_id}.{oaebu_output_dataset_id}.book_product{release_suffix}"
                records = run_bigquery_query(sql)
                count_book_product = len(records)

                sql = f"SELECT COUNT(*) from {self.gcp_project_id}.{oaebu_elastic_dataset_id}.{self.gcp_project_id.replace('-', '_')}_book_product_list{release_suffix}"
                records = run_bigquery_query(sql)
                count_book_product_list = len(records)

                self.assertEqual(count_book_product, count_book_product_list)

                # Ensure there are no duplicates
                sql = f"""  SELECT
                                count
                            FROM(SELECT 
                                COUNT(*) as count
                                FROM {self.gcp_project_id}.{oaebu_elastic_dataset_id}.{self.gcp_project_id.replace('-', '_')}_book_product_metrics{release_suffix} 
                                GROUP BY product_id, month)
                            WHERE count > 1"""
                records = run_bigquery_query(sql)
                self.assertEqual(len(records), 0)

                # Cleanup
                env.run_task(workflow.cleanup.__name__, workflow_dag, execution_date)
