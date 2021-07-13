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
import json
import os
import random
from datetime import datetime
from typing import Dict, List

import pendulum
from airflow import DAG
from airflow.models import Connection
from airflow.operators.dummy_operator import DummyOperator
from faker import Faker

from observatory.dags.model import Table, bq_load_tables
from observatory.dags.workflows.elastic_import_workflow import ElasticImportWorkflow
from observatory.platform.elastic.elastic import Elastic, elastic_mappings_path
from observatory.platform.elastic.kibana import Kibana
from observatory.platform.utils.airflow_utils import AirflowConns
from observatory.platform.utils.file_utils import load_jsonl
from observatory.platform.utils.gc_utils import bigquery_sharded_table_id
from observatory.platform.utils.jinja2_utils import render_template
from observatory.platform.utils.telescope_utils import make_dag_id
from observatory.platform.utils.test_utils import (
    ObservatoryEnvironment,
    ObservatoryTestCase,
    module_file_path,
)


def generate_authors_table(num_rows: int = 1000, min_age: int = 1, max_age: int = 100) -> List[Dict]:
    faker = Faker()
    rows = []
    for _ in range(num_rows):
        name = faker.name()
        age = random.randint(min_age, max_age)
        dob = pendulum.now().subtract(years=age)
        rows.append({"name": name, "age": age, "dob": dob.strftime("%Y-%m-%d")})

    return rows


def make_dummy_dag(dag_id: str, execution_date: datetime) -> DAG:
    """ A Dummy DAG for testing purposes.

    :param dag_id: the DAG id.
    :param execution_date: the DAGs execution date.
    :return: the DAG.
    """

    with DAG(
        dag_id=dag_id,
        schedule_interval="@weekly",
        default_args={"owner": "airflow", "start_date": execution_date},
        catchup=False,
    ) as dag:
        task1 = DummyOperator(task_id="dummy_task")

    return dag


def author_records_to_bq(author_records: List[Dict]):
    records = []

    for record in author_records:
        tmp = copy.copy(record)
        tmp["age"] = str(tmp["age"])
        records.append(tmp)

    return records


class TestElasticImportWorkflow(ObservatoryTestCase):
    """ Tests for the Elastic Import Workflow """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.elastic_port = 9201
        self.kibana_port = 5602
        self.elastic_uri = f"http://localhost:{self.elastic_port}"
        self.kibana_uri = f"http://localhost:{self.kibana_port}"
        self.elastic = Elastic(host=self.elastic_uri)
        self.kibana = Kibana(host=self.kibana_uri)
        self.project_id = os.getenv("TEST_GCP_PROJECT_ID")
        self.data_location = os.getenv("TEST_GCP_DATA_LOCATION")
        self.cwd = os.path.dirname(os.path.abspath(__file__))

    def test_load_mappings(self):
        """ Test that all mappings files can be parsed """

        file_names = [
            "book-author-metrics-mappings.json.jinja2",
            "book-list-mappings.json.jinja2",
            "book-metrics-city-mappings.json.jinja2",
            "book-metrics-country-mappings.json.jinja2",
            "book-metrics-events-mappings.json.jinja2",
            "book-metrics-institution-mappings.json.jinja2",
            "book-metrics-mappings.json.jinja2",
            "book-metrics-referrer-mappings.json.jinja2",
            "book-publisher-metrics-mappings.json.jinja2",
            "book-subject-metrics-mappings.json.jinja2",
            "book-subject-year-metrics-mappings.json.jinja2",
            "book-year-metrics-mappings.json.jinja2",
        ]

        for file_name in file_names:
            path = elastic_mappings_path(file_name)
            render = render_template(path, aggregration_level="hello")
            mapping = json.loads(render)
            self.assertIsInstance(mapping, Dict)

    def test_dag_structure(self):
        """Test that the DAG has the correct structure.

        :return: None
        """

        dag = ElasticImportWorkflow(
            dag_id="elastic_import",
            project_id="project-id",
            dataset_id="dataset-id",
            bucket_name="bucket-name",
            data_location="us",
            file_type="jsonl.gz",
            sensor_dag_ids=["doi"],
            kibana_spaces=[],
        ).make_dag()
        self.assert_dag_structure(
            {
                "doi_sensor": ["check_dependencies"],
                "check_dependencies": ["list_release_info"],
                "list_release_info": ["export_bigquery_tables"],
                "export_bigquery_tables": ["download_exported_data"],
                "download_exported_data": ["import_to_elastic"],
                "import_to_elastic": ["update_elastic_aliases"],
                "update_elastic_aliases": ["create_kibana_index_patterns"],
                "create_kibana_index_patterns": ["cleanup"],
                "cleanup": [],
            },
            dag,
        )

    def test_dag_load(self):
        """Test that the DAG can be loaded from a DAG bag.

        :return: None
        """

        env = ObservatoryEnvironment(self.project_id, self.data_location, enable_api=False)
        with env.create():
            expected_dag_ids = [
                make_dag_id("elastic_import", suffix)
                for suffix in ["observatory", "anu_press", "ucl_press", "wits_press", "umich_press"]
            ]

            dag_file = os.path.join(module_file_path("observatory.dags.dags"), "elastic_import.py")
            for dag_id in expected_dag_ids:
                self.assert_dag_load(dag_id, dag_file)

    def setup_data_export(
        self, author_records: List[Dict], dataset_id: str, bucket_name: str, release_date: pendulum.Pendulum
    ):
        tables = [
            Table(
                table_name="author",
                is_sharded=True,
                dataset_id=dataset_id,
                records=author_records,
                schema_prefix="author",
                schema_path=self.cwd,
            )
        ]

        bq_load_tables(
            tables=tables, bucket_name=bucket_name, release_date=release_date, data_location=self.data_location
        )

    def test_telescope(self):
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
        dataset_id = env.add_dataset(prefix="data_export")
        with env.create() as t:
            # Create connections
            env.add_connection(Connection(conn_id=AirflowConns.ELASTIC, uri=self.elastic_uri))
            env.add_connection(Connection(conn_id=AirflowConns.KIBANA, uri=self.kibana_uri))

            # Create settings
            start_date = pendulum.datetime(year=2021, month=5, day=9)
            dag_id_sensor = "doi"
            space_id = "spce"
            workflow = ElasticImportWorkflow(
                dag_id="elastic_import",
                project_id=self.project_id,
                dataset_id=dataset_id,
                bucket_name=env.download_bucket,
                data_location=self.data_location,
                file_type="jsonl.gz",
                sensor_dag_ids=[dag_id_sensor],
                kibana_spaces=[space_id],
                start_date=start_date,
                mappings_path=self.cwd,
            )
            es_dag = workflow.make_dag()

            # Create Kibana space
            self.kibana.create_space(space_id, "SPCE")

            # Test that DAG waits for sensor
            # Test that sensors do go into the 'up_for_reschedule' state as the DAGs that they wait for haven't run
            expected_state = "up_for_reschedule"
            task_id_sensor = "doi_sensor"
            with env.create_dag_run(es_dag, start_date):
                ti = env.run_task(task_id_sensor, es_dag, execution_date=start_date)
                self.assertEqual(expected_state, ti.state)

            # Run Dummy Dags
            execution_date = pendulum.datetime(year=2021, month=5, day=16)
            release_date = pendulum.datetime(year=2021, month=5, day=22)
            expected_state = "success"
            doi_dag = make_dummy_dag(dag_id_sensor, execution_date)
            with env.create_dag_run(doi_dag, execution_date):
                # Running all of a DAGs tasks sets the DAG to finished
                ti = env.run_task("dummy_task", doi_dag, execution_date=execution_date)
                self.assertEqual(expected_state, ti.state)

            # Make dataset with a small number of tables
            self.setup_data_export(author_records, dataset_id, env.transform_bucket, release_date)

            # Run end to end tests for DOI DAG
            expected_state = "success"
            with env.create_dag_run(es_dag, execution_date):
                # Data folders
                release_id = f"{workflow.dag_id}_{release_date.strftime('%Y_%m_%d')}"
                download_folder = os.path.join(t, "data", "telescopes", "download", workflow.dag_id, release_id)
                extract_folder = os.path.join(t, "data", "telescopes", "extract", workflow.dag_id, release_id)
                transform_folder = os.path.join(t, "data", "telescopes", "transform", workflow.dag_id, release_id)

                # Test that sensor goes into 'success' state as the DAGs that they are waiting for have finished
                ti = env.run_task(task_id_sensor, es_dag, execution_date=execution_date)
                self.assertEqual(expected_state, ti.state)

                # Test that all dependencies are specified: no error should be thrown
                ti = env.run_task(workflow.check_dependencies.__name__, es_dag, execution_date)
                self.assertEqual(expected_state, ti.state)

                # Test list_release_info task
                ti = env.run_task(workflow.list_release_info.__name__, es_dag, execution_date=execution_date)
                self.assertEqual(expected_state, ti.state)
                table_id = bigquery_sharded_table_id("author", release_date)
                expected_msg = {
                    "release_date": release_date.date(),
                    "table_ids": [table_id],
                }
                actual_msg = ti.xcom_pull(
                    key="releases", task_ids=workflow.list_release_info.__name__, include_prior_dates=False
                )
                self.assertEqual(expected_msg, actual_msg)

                # Test export_bigquery_tables info task
                ti = env.run_task(workflow.export_bigquery_tables.__name__, es_dag, execution_date=execution_date)
                self.assertEqual(expected_state, ti.state)
                blob_suffix = f"{table_id}_000000000000.jsonl.gz"
                blob_name = f"telescopes/{es_dag.dag_id}/{release_id}/{blob_suffix}"
                self.assert_blob_exists(env.download_bucket, blob_name)

                # Test list download_exported_data info task
                ti = env.run_task(workflow.download_exported_data.__name__, es_dag, execution_date=execution_date)
                self.assertEqual(expected_state, ti.state)
                file_path = os.path.join(download_folder, blob_suffix)
                self.assertTrue(os.path.isfile(file_path))  # Check that file exists
                expected_rows = author_records_to_bq(author_records)
                actual_rows = load_jsonl(file_path)
                expected_rows.sort(key=sort_key)
                actual_rows.sort(key=sort_key)
                self.assertEqual(len(expected_rows), len(actual_rows))
                self.assertListEqual(expected_rows, actual_rows)  # Check that exported rows match

                # Test list import_to_elastic info task
                index_id = f"author-{release_date.strftime('%Y%m%d')}"
                ti = env.run_task(workflow.import_to_elastic.__name__, es_dag, execution_date=execution_date)
                self.assertEqual(expected_state, ti.state)
                self.assertTrue(self.elastic.es.indices.exists(index=index_id))  # Check that index exists
                actual_rows = self.elastic.query(index_id)
                actual_rows.sort(key=sort_key)
                self.assertEqual(len(expected_rows), len(actual_rows))
                self.assertListEqual(expected_rows, actual_rows)  # check that author records matches es index

                # Test list update_elastic_aliases info task
                expected_alias_id = "author"
                ti = env.run_task(workflow.update_elastic_aliases.__name__, es_dag, execution_date=execution_date)
                self.assertEqual(expected_state, ti.state)
                expected_indexes = [index_id]
                actual_indexes = self.elastic.get_alias_indexes(expected_alias_id)
                self.assertListEqual(expected_indexes, actual_indexes)  # Check that aliases updated

                # Test list create_kibana_index_patterns info task
                expected_index_pattern_id = "author"
                ti = env.run_task(workflow.create_kibana_index_patterns.__name__, es_dag, execution_date=execution_date)
                self.assertEqual(expected_state, ti.state)
                self.assertTrue(
                    self.kibana.get_index_pattern(expected_index_pattern_id, space_id=space_id)
                )  # Check that index pattern created

                # Test list cleanup info task
                ti = env.run_task(workflow.cleanup.__name__, es_dag, execution_date=execution_date)
                self.assertEqual(expected_state, ti.state)
                self.assert_cleanup(download_folder, extract_folder, transform_folder)
