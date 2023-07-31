# Copyright 2021 Curtin University
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

from __future__ import annotations

import copy
import os
import random
from typing import Dict, List
from unittest.mock import patch

import pendulum
from airflow import DAG
from airflow.models import Connection
from airflow.operators.dummy import DummyOperator
from faker import Faker

from observatory.platform.bigquery import bq_find_schema, bq_sharded_table_id
from observatory.platform.elastic.elastic import Elastic
from observatory.platform.elastic.kibana import Kibana, TimeField
from observatory.platform.files import load_jsonl
from observatory.platform.gcs import gcs_blob_name_from_path
from observatory.platform.observatory_config import Workflow, CloudWorkspace
from observatory.platform.observatory_environment import (
    ObservatoryEnvironment,
    ObservatoryTestCase,
    Table,
    bq_load_tables,
    test_fixtures_path,
    find_free_port,
)
from observatory.platform.workflows.elastic_import_workflow import (
    ElasticImportWorkflow,
    KeepInfo,
    KeepOrder,
    load_elastic_mappings_simple,
    get_keep_info,
    ELASTIC_CONN_ID,
    KIBANA_CONN_ID,
    ElasticImportConfig,
    ElasticImportRelease,
)

TEST_FIXTURES_PATH = test_fixtures_path("schemas")


def make_dummy_dag(dag_id: str, execution_date: pendulum.DateTime) -> DAG:
    """A Dummy DAG for testing purposes.

    :param dag_id: the DAG id.
    :param execution_date: the DAGs execution date.
    :return: the DAG.
    """

    with DAG(
        dag_id=dag_id,
        schedule="@weekly",
        default_args={"owner": "airflow", "start_date": execution_date},
        catchup=False,
    ) as dag:
        DummyOperator(task_id="dummy_task")

    return dag


def generate_authors_table(num_rows: int = 1000, min_age: int = 1, max_age: int = 100) -> List[Dict]:
    """Generate records for the test authors table.

    :param num_rows: number of rows to generate.
    :param min_age: the minimum age of an author.
    :param max_age: the maximum age of an author.
    :return: the author table rows.
    """

    faker = Faker()
    rows = []
    for _ in range(num_rows):
        name = faker.name()
        age = random.randint(min_age, max_age)
        dob = pendulum.now().subtract(years=age)
        rows.append({"name": name, "age": age, "dob": dob.strftime("%Y-%m-%d")})

    return rows


def author_records_to_bq(author_records: List[Dict]):
    """Convert author records into BigQuery records, i.e. convert the age field from a long into a string.

    :param author_records: the author records.
    :return: the converted author records.
    """

    records = []

    for record in author_records:
        tmp = copy.copy(record)
        tmp["age"] = str(tmp["age"])
        records.append(tmp)

    return records


class TestElasticImportWorkflow(ObservatoryTestCase):
    """Tests for the Elastic Import Workflow"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.dag_id = "elastic_import"
        self.elastic_port = find_free_port()
        self.kibana_port = find_free_port()
        self.project_id = os.getenv("TEST_GCP_PROJECT_ID")
        self.data_location = os.getenv("TEST_GCP_DATA_LOCATION")
        self.table_name = "ao_author"
        self.kibana_time_fields = [TimeField("^.*$", "dob")]

    def test_get_keep_info(self):
        index_keep_info = {}
        keep_info = get_keep_info(index="something", index_keep_info=index_keep_info)
        self.assertEqual(keep_info, KeepInfo(ordering=KeepOrder.newest, num=2))

        index_keep_info = {"": KeepInfo(ordering=KeepOrder.newest, num=1)}
        keep_info = get_keep_info(index="something", index_keep_info=index_keep_info)
        self.assertEqual(keep_info, KeepInfo(ordering=KeepOrder.newest, num=1))

        index_keep_info = {
            "test-very-specific": KeepInfo(ordering=KeepOrder.newest, num=5),
            "test": KeepInfo(ordering=KeepOrder.newest, num=3),
        }

        keep_info = get_keep_info(index="test-very-different", index_keep_info=index_keep_info)
        self.assertEqual(keep_info, KeepInfo(ordering=KeepOrder.newest, num=3))

        keep_info = get_keep_info(index="test-very-specific", index_keep_info=index_keep_info)
        self.assertEqual(keep_info, KeepInfo(ordering=KeepOrder.newest, num=5))

        keep_info = get_keep_info(index="nomatch", index_keep_info=index_keep_info)
        self.assertEqual(keep_info, KeepInfo(ordering=KeepOrder.newest, num=2))

    def test_load_elastic_mappings_simple(self):
        """Test load_elastic_mappings_simple"""

        mappings = load_elastic_mappings_simple(TEST_FIXTURES_PATH, self.table_name)
        self.assertIsInstance(mappings, Dict)

    def test_dag_load(self):
        """Test that vm_create can be loaded from a DAG bag.
        :return: None
        """

        env = ObservatoryEnvironment(
            workflows=[
                Workflow(
                    dag_id=self.dag_id,
                    name="My Elastic Import Workflow",
                    class_name="observatory.platform.workflows.elastic_import_workflow.ElasticImportWorkflow",
                    cloud_workspace=self.fake_cloud_workspace,
                    kwargs=dict(
                        sensor_dag_ids=["doi"],
                        kibana_spaces=["test-space"],
                        elastic_import_config="tests.observatory.platform.workflows.elastic_config.ELASTIC_IMPORT_CONFIG",
                        tags=["academic-observatory"],
                    ),
                )
            ]
        )

        with env.create():
            self.assert_dag_load_from_config(self.dag_id)

        # Failure to load caused by missing kwargs
        env = ObservatoryEnvironment(
            workflows=[
                Workflow(
                    dag_id=self.dag_id,
                    name="My Elastic Import Workflow",
                    class_name="observatory.platform.workflows.elastic_import_workflow.ElasticImportWorkflow",
                    cloud_workspace=self.fake_cloud_workspace,
                )
            ]
        )

        with env.create():
            with self.assertRaises(AssertionError) as cm:
                self.assert_dag_load_from_config(self.dag_id)
            msg = cm.exception.args[0]
            self.assertTrue("missing 3 required keyword-only arguments" in msg)
            self.assertTrue("sensor_dag_ids" in msg)
            self.assertTrue("kibana_spaces" in msg)
            self.assertTrue("elastic_import_config" in msg)

    def test_constructor(self):
        config = self.config = ElasticImportConfig(
            elastic_mappings_path=TEST_FIXTURES_PATH,
            elastic_mappings_func=load_elastic_mappings_simple,
            kibana_time_fields=self.kibana_time_fields,
        )

        # Can create ElasticImportWorkflow
        workflow = ElasticImportWorkflow(
            dag_id="elastic_import",
            cloud_workspace=CloudWorkspace(
                project_id="project-id",
                download_bucket="download_bucket",
                transform_bucket="transform_bucket",
                data_location="us",
            ),
            sensor_dag_ids=["doi"],
            kibana_spaces=[],
            elastic_import_config=config,
        )
        self.assertTrue(workflow.index_keep_info is not None)
        self.assertIsInstance(workflow.elastic_import_config, ElasticImportConfig)
        self.assertEqual(config, workflow.elastic_import_config)

        # Can create elastic_import_config from string
        workflow = ElasticImportWorkflow(
            dag_id="elastic_import",
            cloud_workspace=CloudWorkspace(
                project_id="project-id",
                download_bucket="download_bucket",
                transform_bucket="transform_bucket",
                data_location="us",
            ),
            sensor_dag_ids=["doi"],
            kibana_spaces=[],
            elastic_import_config="tests.observatory.platform.workflows.elastic_config.ELASTIC_IMPORT_CONFIG",
        )
        self.assertIsInstance(workflow.elastic_import_config, ElasticImportConfig)
        self.assertEqual(config, workflow.elastic_import_config)

    def test_dag_structure(self):
        """Test that the DAG has the correct structure.

        :return: None
        """

        config = ElasticImportConfig(
            elastic_mappings_path=TEST_FIXTURES_PATH,
            elastic_mappings_func=load_elastic_mappings_simple,
            kibana_time_fields=self.kibana_time_fields,
        )
        workflow = ElasticImportWorkflow(
            dag_id="elastic_import",
            cloud_workspace=CloudWorkspace(
                project_id="project-id",
                download_bucket="download_bucket",
                transform_bucket="transform_bucket",
                data_location="us",
            ),
            sensor_dag_ids=["doi"],
            kibana_spaces=[],
            elastic_import_config=config,
        )
        self.assertTrue(workflow.index_keep_info is not None)

        dag = workflow.make_dag()
        self.assert_dag_structure(
            {
                "doi_sensor": ["check_dependencies"],
                "check_dependencies": ["list_release_info"],
                "list_release_info": ["export_bigquery_tables"],
                "export_bigquery_tables": ["download_exported_data"],
                "download_exported_data": ["import_to_elastic"],
                "import_to_elastic": ["update_elastic_aliases"],
                "update_elastic_aliases": ["delete_stale_indices"],
                "delete_stale_indices": ["create_kibana_index_patterns"],
                "create_kibana_index_patterns": ["cleanup"],
                "cleanup": [],
            },
            dag,
        )

    def setup_data_export(
        self,
        table_name: str,
        author_records: List[Dict],
        dataset_id: str,
        bucket_name: str,
        snapshot_date: pendulum.DateTime,
        schema_file_path: str,
    ):
        """Setup the fake dataset in BigQuery.

        :param table_name: the table name to load.
        :param author_records: the author records.
        :param dataset_id: the BigQuery dataset id.
        :param bucket_name: the Google Cloud Storage bucket name.
        :param snapshot_date: the dataset release date.
        :paran schema_file_path: The location of the schema file to use
        :return: None.
        """

        tables = [
            Table(
                table_name=table_name,
                is_sharded=True,
                dataset_id=dataset_id,
                records=author_records,
                schema_file_path=schema_file_path,
            )
        ]

        bq_load_tables(
            tables=tables,
            bucket_name=bucket_name,
            snapshot_date=snapshot_date,
            project_id=self.project_id,
        )

    @patch("observatory.platform.workflows.elastic_import_workflow.Kibana")
    def test_telescope(self, mock_kibana):
        """Test the DAG end to end.

        :return: None.
        """
        # Create ground truth author records
        author_records = generate_authors_table(num_rows=10)
        sort_key = lambda x: x["name"]
        author_records.sort(key=sort_key)

        env = ObservatoryEnvironment(
            self.project_id,
            self.data_location,
            enable_api=False,
            enable_elastic=True,
            elastic_port=self.elastic_port,
            kibana_port=self.kibana_port,
        )

        bq_dataset_id = env.add_dataset(prefix="data_export")
        with env.create() as t:
            # Create settings
            start_date = pendulum.datetime(year=2021, month=5, day=9)
            dag_id_sensor = "doi"
            space_id = "spce"

            config = self.config = ElasticImportConfig(
                elastic_mappings_path=TEST_FIXTURES_PATH,
                elastic_mappings_func=load_elastic_mappings_simple,
                kibana_time_fields=self.kibana_time_fields,
            )
            workflow = ElasticImportWorkflow(
                dag_id="elastic_import",
                cloud_workspace=CloudWorkspace(
                    project_id=self.project_id,
                    download_bucket=env.download_bucket,
                    transform_bucket=env.download_bucket,
                    data_location=self.data_location,
                ),
                sensor_dag_ids=[dag_id_sensor],
                kibana_spaces=[space_id],
                elastic_import_config=config,
                bq_dataset_id=bq_dataset_id,  # Need to set the custom dataset ID that is used for tests
                start_date=start_date,
            )
            es_dag = workflow.make_dag()

            # Create Elastic/Kibana clients
            elastic = Elastic(host=env.elastic_env.elastic_uri)
            kibana = Kibana(host=env.elastic_env.kibana_uri, username="elastic", password="observatory")

            # Create connections
            env.add_connection(Connection(conn_id=ELASTIC_CONN_ID, uri=env.elastic_env.elastic_uri))
            env.add_connection(Connection(conn_id=KIBANA_CONN_ID, uri=env.elastic_env.kibana_uri))

            # Mock kibana inside workflow, using username/password instead of api key
            mock_kibana.return_value = kibana

            # Create Kibana space
            kibana.create_space(space_id, "SPCE")

            # Test that DAG waits for sensor
            # Test that sensors do go into the 'up_for_reschedule' state as the DAGs that they wait for haven't run
            expected_state = "up_for_reschedule"
            task_id_sensor = "doi_sensor"
            with env.create_dag_run(es_dag, start_date):
                ti = env.run_task(task_id_sensor)
                self.assertEqual(expected_state, ti.state)

            # Run Dummy Dags
            execution_date = pendulum.datetime(year=2021, month=5, day=16)
            snapshot_date = pendulum.datetime(year=2021, month=5, day=23)
            expected_state = "success"
            doi_dag = make_dummy_dag(dag_id_sensor, execution_date)
            with env.create_dag_run(doi_dag, execution_date):
                # Running all of a DAGs tasks sets the DAG to finished
                ti = env.run_task("dummy_task")
                self.assertEqual(expected_state, ti.state)

            # Make dataset with a small number of tables
            schema_file_path = bq_find_schema(
                path=TEST_FIXTURES_PATH, table_name=self.table_name, release_date=snapshot_date
            )
            self.setup_data_export(
                self.table_name, author_records, bq_dataset_id, env.transform_bucket, snapshot_date, schema_file_path
            )

            # Run end to end tests for DOI DAG
            expected_state = "success"
            with env.create_dag_run(es_dag, execution_date) as dag_run:
                # Make release object for testing paths
                run_id = dag_run.run_id
                release = ElasticImportRelease(
                    dag_id=workflow.dag_id, run_id=run_id, snapshot_date=snapshot_date, table_ids=[]
                )

                # Test that sensor goes into 'success' state as the DAGs that they are waiting for have finished
                ti = env.run_task(task_id_sensor)
                self.assertEqual(expected_state, ti.state)

                # Test that all dependencies are specified: no error should be thrown
                ti = env.run_task(workflow.check_dependencies.__name__)
                self.assertEqual(expected_state, ti.state)

                # Test list_release_info task
                ti = env.run_task(workflow.list_release_info.__name__)
                self.assertEqual(expected_state, ti.state)
                table_id = bq_sharded_table_id(self.project_id, bq_dataset_id, self.table_name, snapshot_date)
                expected_msg = {"snapshot_date": snapshot_date.format("YYYYMMDD"), "table_ids": [table_id]}
                actual_msg = ti.xcom_pull(
                    key="releases", task_ids=workflow.list_release_info.__name__, include_prior_dates=False
                )
                self.assertEqual(expected_msg, actual_msg)

                # Test export_bigquery_tables info task
                ti = env.run_task(workflow.export_bigquery_tables.__name__)
                self.assertEqual(expected_state, ti.state)
                file_name = f"{table_id}_000000000000.jsonl.gz"
                blob_name = gcs_blob_name_from_path(os.path.join(release.download_folder, file_name))
                self.assert_blob_exists(env.download_bucket, blob_name)

                # Test list download_exported_data info task
                ti = env.run_task(workflow.download_exported_data.__name__)
                self.assertEqual(expected_state, ti.state)
                file_path = os.path.join(release.download_folder, file_name)
                self.assertTrue(os.path.isfile(file_path))  # Check that file exists
                expected_rows = author_records_to_bq(author_records)
                actual_rows = load_jsonl(file_path)
                expected_rows.sort(key=sort_key)
                actual_rows.sort(key=sort_key)
                self.assertEqual(len(expected_rows), len(actual_rows))
                self.assertListEqual(expected_rows, actual_rows)  # Check that exported rows match

                # Test list import_to_elastic info task
                index_id = f"ao-author-{snapshot_date.strftime('%Y%m%d')}"
                ti = env.run_task(workflow.import_to_elastic.__name__)
                self.assertEqual(expected_state, ti.state)
                self.assertTrue(elastic.es.indices.exists(index=index_id))  # Check that index exists
                actual_rows = elastic.query(index_id)
                actual_rows.sort(key=sort_key)
                self.assertEqual(len(expected_rows), len(actual_rows))
                self.assertListEqual(expected_rows, actual_rows)  # check that author records matches es index

                # Test list update_elastic_aliases info task
                expected_alias_id = "ao-author"
                ti = env.run_task(workflow.update_elastic_aliases.__name__)
                self.assertEqual(expected_state, ti.state)
                expected_indexes = [index_id]
                actual_indexes = elastic.get_alias_indexes(expected_alias_id)
                self.assertListEqual(expected_indexes, actual_indexes)  # Check that aliases updated

                # Test delete_stale_indices task
                # Artificially load extra indices for ao-author
                elastic.create_index("ao-author-20210524")
                elastic.create_index("ao-author-20210525")
                elastic.create_index("ao-author-20210526")
                ti = env.run_task(workflow.delete_stale_indices.__name__)
                self.assertEqual(expected_state, ti.state)
                indices_after_cleanup = set(elastic.list_indices("ao-author-*"))
                self.assertEqual(len(indices_after_cleanup), 2)
                self.assertTrue("ao-author-20210526" in indices_after_cleanup)
                self.assertTrue("ao-author-20210525" in indices_after_cleanup)

                # Test list create_kibana_index_patterns info task
                expected_index_pattern_id = expected_alias_id
                ti = env.run_task(workflow.create_kibana_index_patterns.__name__)
                self.assertEqual(expected_state, ti.state)
                self.assertTrue(
                    kibana.get_index_pattern(expected_index_pattern_id, space_id=space_id)
                )  # Check that index pattern created

                # Test list cleanup info task
                ti = env.run_task(workflow.cleanup.__name__)
                self.assertEqual(expected_state, ti.state)
                self.assertFalse(os.path.exists(release.workflow_folder))
