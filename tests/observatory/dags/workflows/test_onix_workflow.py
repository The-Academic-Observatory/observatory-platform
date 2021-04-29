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

import os
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

import observatory.api.server.orm as orm
import pandas as pd
import pendulum
from airflow import DAG
from airflow.models.connection import Connection
from airflow.operators.dummy_operator import DummyOperator
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.utils.decorators import apply_defaults
from airflow.utils.state import State
from click.testing import CliRunner
from google.cloud.bigquery import SourceFormat
from observatory.api.client.identifiers import TelescopeTypes
from observatory.api.server.orm import Organisation
from observatory.dags.telescopes.onix import OnixTelescope
from observatory.dags.workflows.oaebu_partners import OaebuPartners
from observatory.dags.workflows.onix_workflow import OnixWorkflow, OnixWorkflowRelease
from observatory.platform.utils.airflow_utils import AirflowConns
from observatory.platform.utils.gc_utils import (
    delete_bigquery_dataset,
    delete_bucket_dir,
    run_bigquery_query,
    upload_files_to_cloud_storage,
)
from observatory.platform.utils.telescope_utils import (
    make_observatory_api,
    make_telescope_sensor,
)
from observatory.platform.utils.template_utils import (
    blob_name,
    bq_load_shard_v2,
    table_ids_from_path,
)
from observatory.platform.utils.test_utils import (
    ObservatoryEnvironment,
    ObservatoryTestCase,
    module_file_path,
    random_id,
    test_fixtures_path,
)


class TestOnixWorkflowRelease(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        with patch("observatory.dags.workflows.onix_workflow.select_table_suffixes") as mock_sel_table_suffixes:
            mock_sel_table_suffixes.return_value = [pendulum.Pendulum(2021, 1, 1)]
            self.release = OnixWorkflowRelease(
                dag_id="did",
                release_date=pendulum.Pendulum(2021, 4, 20),
                gcp_project_id="pid",
                gcp_bucket_name="bucket",
            )

    def test_transform_bucket(self):
        self.assertEqual(self.release.transform_bucket, "bucket")

    def test_transform_folder(self):
        self.assertEqual(self.release.transform_folder, "did/20210420")

    def test_transform_files(self):
        self.assertEqual(
            self.release.transform_files,
            [
                self.release.workslookup_filename,
                self.release.workslookup_errors_filename,
                self.release.worksfamilylookup_filename,
            ],
        )

    def test_download_bucket(self):
        self.assertEqual(self.release.download_bucket, str())

    def test_download_files(self):
        self.assertEqual(self.release.download_files, list())

    def test_extract_files(self):
        self.assertEqual(self.release.extract_files, list())

    def test_download_folder(self):
        self.assertEqual(self.release.download_folder, str())

    def test_extract_folder(self):
        self.assertEqual(self.release.extract_folder, str())


class TestOnixWorkflow(ObservatoryTestCase):
    """
    Test the OnixWorkflow class.
    """

    onix_data = [
        {
            "ISBN13": "111",
            "RelatedWorks": [
                {
                    "WorkRelationCode": "Manifestation of",
                    "WorkIdentifiers": [
                        {"WorkIDType": "ISBN-13", "IDValue": "112"},
                    ],
                },
                {
                    "WorkRelationCode": "Manifestation of",
                    "WorkIdentifiers": [
                        {"WorkIDType": "ISBN-13", "IDValue": "113"},
                    ],
                },
            ],
            "RelatedProducts": [
                {"ProductRelationCodes": ["Replaces", "something random"], "ISBN13": "211"},
            ],
        },
        {
            "ISBN13": "112",
            "RelatedWorks": [
                {
                    "WorkRelationCode": "Manifestation of",
                    "WorkIdentifiers": [
                        {"WorkIDType": "ISBN-13", "IDValue": "112"},
                    ],
                },
            ],
            "RelatedProducts": [],
        },
        {
            "ISBN13": "211",
            "RelatedWorks": [
                {
                    "WorkRelationCode": "Manifestation of",
                    "WorkIdentifiers": [
                        {"WorkIDType": "ISBN-13", "IDValue": "211"},
                    ],
                },
            ],
            "RelatedProducts": [],
        },
    ]

    class MockTelescopeResponse:
        def __init__(self):
            self.organisation = Organisation(
                name="test",
                gcp_project_id="project_id",
            )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.telescope = TestOnixWorkflow.MockTelescopeResponse()
        self.host = "localhost"
        self.api_port = 5000
        self.project_id = os.getenv("TESTS_GOOGLE_CLOUD_PROJECT_ID")
        self.data_location = os.getenv("TESTS_DATA_LOCATION")
        self.bucket_name = "bucket_name"

    def setup_observatory_env(self, env):
        # Add Observatory API connection
        conn = Connection(conn_id=AirflowConns.OBSERVATORY_API, uri=f"http://:password@{self.host}:{self.api_port}")
        env.add_connection(conn)

        # Add an ONIX telescope
        dt = pendulum.utcnow()
        telescope_type = orm.TelescopeType(name="ONIX Telescope", type_id=TelescopeTypes.onix, created=dt, modified=dt)
        env.api_session.add(telescope_type)
        organisation = orm.Organisation(name="Curtin Press", created=dt, modified=dt)
        env.api_session.add(organisation)
        telescope = orm.Telescope(
            name="Curtin Press ONIX Telescope",
            telescope_type=telescope_type,
            organisation=organisation,
            modified=dt,
            created=dt,
        )
        env.api_session.add(telescope)

        # Add an ONIX workflow
        dt = pendulum.utcnow()
        telescope_type = orm.TelescopeType(
            name="ONIX Telescope Workflow", type_id=TelescopeTypes.onix_workflow, created=dt, modified=dt
        )
        env.api_session.add(telescope_type)
        organisation = orm.Organisation(name="Curtin Press", created=dt, modified=dt)
        env.api_session.add(organisation)
        telescope = orm.Telescope(
            name="Curtin Press ONIX Workflow",
            telescope_type=telescope_type,
            organisation=organisation,
            modified=dt,
            created=dt,
        )
        env.api_session.add(telescope)
        env.api_session.commit()

    @patch("observatory.dags.workflows.onix_workflow.select_table_suffixes")
    def test_ctor_gen_dag_id(self, mock_sel_table_suffixes):
        mock_sel_table_suffixes.return_value = [pendulum.Pendulum(2021, 1, 1)]
        with CliRunner().isolated_filesystem():
            telescope_sensor = make_telescope_sensor(self.telescope.organisation.name, OnixTelescope.DAG_ID_PREFIX)
            wf = OnixWorkflow(
                org_name=self.telescope.organisation.name,
                gcp_project_id=self.telescope.organisation.gcp_project_id,
                gcp_bucket_name=self.bucket_name,
                telescope_sensor=telescope_sensor,
            )

            release = wf.make_release(execution_date=pendulum.Pendulum(2021, 1, 1))
            self.assertEqual(wf.dag_id, "onix_workflow_test")
            self.assertEqual(release.workslookup_filename, "onix_workflow_test/20210101/onix_workid_isbn.jsonl.gz")
            self.assertEqual(
                release.workslookup_errors_filename, "onix_workflow_test/20210101/onix_workid_isbn_errors.jsonl.gz"
            )
            self.assertEqual(
                release.worksfamilylookup_filename, "onix_workflow_test/20210101/onix_workfamilyid_isbn.jsonl.gz"
            )

            self.assertEqual(release.onix_table_id, "onix")
            self.assertEqual(release.release_date, pendulum.Pendulum(2021, 1, 1, 0, 0, 0, 0))
            self.assertEqual(release.project_id, "project_id")
            self.assertEqual(release.transform_bucket, "bucket_name")
            self.assertTrue(wf.sensors[0] != None)

    @patch("observatory.dags.workflows.onix_workflow.select_table_suffixes")
    def test_ctor_gen_assign_dag_id(self, mock_sel_table_suffixes):
        mock_sel_table_suffixes.return_value = [pendulum.Pendulum(2021, 1, 1)]
        with CliRunner().isolated_filesystem():
            telescope_sensor = make_telescope_sensor(self.telescope.organisation.name, OnixTelescope.DAG_ID_PREFIX)
            wf = OnixWorkflow(
                org_name=self.telescope.organisation.name,
                gcp_project_id=self.telescope.organisation.gcp_project_id,
                gcp_bucket_name=self.bucket_name,
                telescope_sensor=telescope_sensor,
                dag_id="dagid",
            )

            release = wf.make_release(execution_date=pendulum.Pendulum(2021, 1, 1))

            self.assertEqual(release.dag_id, "dagid")
            self.assertEqual(release.workslookup_filename, "dagid/20210101/onix_workid_isbn.jsonl.gz")
            self.assertEqual(release.workslookup_errors_filename, "dagid/20210101/onix_workid_isbn_errors.jsonl.gz")
            self.assertEqual(release.worksfamilylookup_filename, "dagid/20210101/onix_workfamilyid_isbn.jsonl.gz")

            self.assertEqual(release.onix_table_id, "onix")
            self.assertEqual(release.release_date, pendulum.Pendulum(2021, 1, 1, 0, 0, 0, 0))
            self.assertEqual(release.project_id, "project_id")
            self.assertEqual(release.transform_bucket, "bucket_name")
            self.assertTrue(wf.sensors[0] != None)

    @patch("observatory.dags.workflows.onix_workflow.select_table_suffixes")
    def test_ctor(self, mock_sel_table_suffixes):
        mock_sel_table_suffixes.return_value = [pendulum.Pendulum(2021, 1, 1)]
        with CliRunner().isolated_filesystem():
            telescope_sensor = make_telescope_sensor(self.telescope.organisation.name, OnixTelescope.DAG_ID_PREFIX)
            wf = OnixWorkflow(
                org_name=self.telescope.organisation.name,
                gcp_project_id=self.telescope.organisation.gcp_project_id,
                gcp_bucket_name=self.bucket_name,
                telescope_sensor=telescope_sensor,
            )

            release = wf.make_release(execution_date=pendulum.Pendulum(2021, 1, 1))

            self.assertEqual(wf.dag_id, "onix_workflow_test")
            self.assertEqual(wf.org_name, "test")
            self.assertEqual(wf.gcp_bucket_name, "bucket_name")

            self.assertEqual(release.dag_id, "onix_workflow_test")
            self.assertEqual(release.release_date, pendulum.Pendulum(2021, 1, 1))
            self.assertEqual(release.transform_folder, "onix_workflow_test/20210101")
            self.assertEqual(release.worksid_table, "onix_workid_isbn")
            self.assertEqual(release.worksid_error_table, "onix_workid_isbn_errors")
            self.assertEqual(release.workfamilyid_table, "onix_workfamilyid_isbn")

            self.assertEqual(release.workslookup_filename, "onix_workflow_test/20210101/onix_workid_isbn.jsonl.gz")
            self.assertEqual(
                release.workslookup_errors_filename, "onix_workflow_test/20210101/onix_workid_isbn_errors.jsonl.gz"
            )
            self.assertEqual(
                release.worksfamilylookup_filename, "onix_workflow_test/20210101/onix_workfamilyid_isbn.jsonl.gz"
            )

            self.assertEqual(release.workflow_dataset_id, "onix_workflow")
            self.assertEqual(release.project_id, "project_id")
            self.assertEqual(release.onix_dataset_id, "onix")
            self.assertEqual(release.dataset_location, "us")
            self.assertEqual(release.dataset_description, "ONIX workflow tables")

            self.assertEqual(release.onix_table_id, "onix")

    @patch("observatory.dags.workflows.onix_workflow.run_bigquery_query")
    @patch("observatory.dags.workflows.onix_workflow.select_table_suffixes")
    def test_get_onix_records(self, mock_sel_table_suffixes, mock_bq_query):
        mock_sel_table_suffixes.return_value = [pendulum.Pendulum(2021, 1, 1)]
        mock_bq_query.return_value = TestOnixWorkflow.onix_data
        with CliRunner().isolated_filesystem():
            telescope_sensor = make_telescope_sensor(self.telescope.organisation.name, OnixTelescope.DAG_ID_PREFIX)
            wf = OnixWorkflow(
                org_name=self.telescope.organisation.name,
                gcp_project_id=self.telescope.organisation.gcp_project_id,
                gcp_bucket_name=self.bucket_name,
                telescope_sensor=telescope_sensor,
            )
            records = wf.get_onix_records("project_id", "ds_id", "table_id")
            self.assertEqual(len(records), 3)
            self.assertEqual(records[0]["ISBN13"], "111")

    @patch("observatory.dags.workflows.onix_workflow.select_table_suffixes")
    def test_cleanup(self, mock_sel_table_suffixes):
        mock_sel_table_suffixes.return_value = [pendulum.Pendulum(2021, 1, 1)]
        with CliRunner().isolated_filesystem():
            telescope_sensor = make_telescope_sensor(self.telescope.organisation.name, OnixTelescope.DAG_ID_PREFIX)
            wf = OnixWorkflow(
                org_name=self.telescope.organisation.name,
                gcp_project_id=self.telescope.organisation.gcp_project_id,
                gcp_bucket_name=self.bucket_name,
                telescope_sensor=telescope_sensor,
            )

            release = wf.make_release(execution_date=pendulum.Pendulum(2021, 1, 1))
            self.assertTrue(os.path.isdir(release.transform_folder))
            wf.cleanup(release)
            self.assertFalse(os.path.isdir(release.transform_folder))

    @patch("observatory.dags.workflows.onix_workflow.select_table_suffixes")
    def test_dag_structure(self, mock_sel_table_suffixes):
        mock_sel_table_suffixes.return_value = [pendulum.Pendulum(2021, 1, 1)]
        with CliRunner().isolated_filesystem():
            telescope_sensor = make_telescope_sensor(self.telescope.organisation.name, OnixTelescope.DAG_ID_PREFIX)
            wf = OnixWorkflow(
                org_name=self.telescope.organisation.name,
                gcp_project_id=self.telescope.organisation.gcp_project_id,
                gcp_bucket_name=self.bucket_name,
                telescope_sensor=telescope_sensor,
            )
            dag = wf.make_dag()
            self.assert_dag_structure(
                {
                    "onix_test_sensor": ["aggregate_works"],
                    "aggregate_works": ["upload_aggregation_tables"],
                    "upload_aggregation_tables": ["bq_load_workid_lookup"],
                    "bq_load_workid_lookup": ["bq_load_workid_lookup_errors"],
                    "bq_load_workid_lookup_errors": ["bq_load_workfamilyid_lookup"],
                    "bq_load_workfamilyid_lookup": ["cleanup"],
                    "cleanup": [],
                },
                dag,
            )

    @patch("observatory.dags.workflows.onix_workflow.select_table_suffixes")
    def test_dag_load(self, mock_sel_table_suffixes):
        mock_sel_table_suffixes.return_value = [pendulum.Pendulum(2021, 1, 1)]
        with CliRunner().isolated_filesystem():
            env = ObservatoryEnvironment(
                self.project_id, self.data_location, api_host=self.host, api_port=self.api_port
            )
            with env.create():
                self.setup_observatory_env(env)
                dag_file = os.path.join(module_file_path("observatory.dags.dags"), "onix_workflow.py")
                self.assert_dag_load("onix_workflow_curtin_press", dag_file)

    @patch("observatory.dags.workflows.onix_workflow.run_bigquery_query")
    @patch("observatory.dags.workflows.onix_workflow.bq_load_shard_v2")
    @patch("observatory.dags.workflows.onix_workflow.upload_files_to_cloud_storage")
    @patch("observatory.dags.workflows.onix_workflow.list_to_jsonl_gz")
    @patch("observatory.dags.workflows.onix_workflow.select_table_suffixes")
    def test_create_and_upload_bq_isbn13_workid_lookup_table(
        self,
        mock_sel_table_suffixes,
        mock_write_to_file,
        mock_upload_files_from_list,
        mock_bq_load_lookup,
        mock_bq_query,
    ):
        mock_sel_table_suffixes.return_value = [pendulum.Pendulum(2021, 1, 1)]
        mock_bq_query.return_value = TestOnixWorkflow.onix_data
        env = ObservatoryEnvironment(self.project_id, self.data_location)
        with env.create():
            with CliRunner().isolated_filesystem():
                telescope_sensor = make_telescope_sensor(self.telescope.organisation.name, OnixTelescope.DAG_ID_PREFIX)
                wf = OnixWorkflow(
                    org_name=self.telescope.organisation.name,
                    gcp_project_id=self.telescope.organisation.gcp_project_id,
                    gcp_bucket_name=self.bucket_name,
                    telescope_sensor=telescope_sensor,
                )

                release = wf.make_release(execution_date=pendulum.Pendulum(2021, 1, 1))

                # Test works aggregation
                wf.aggregate_works(release)
                self.assertEqual(mock_write_to_file.call_count, 3)
                call_args = mock_write_to_file.call_args_list
                self.assertEqual(len(call_args[0][0][1]), 3)
                lookup_table = {arg["isbn13"]: arg["work_id"] for arg in call_args[0][0][1]}

                self.assertTrue(
                    lookup_table["111"] == lookup_table["112"] and lookup_table["112"] != lookup_table["211"]
                )

                self.assertEqual(call_args[0][0][0], "onix_workflow_test/20210101/onix_workid_isbn.jsonl.gz")

                self.assertEqual(
                    call_args[1][0][1],
                    [
                        {
                            "Error": "Product record 111 is a manifestation of 113, but we cannot find a product record with identifier ISBN13:113."
                        },
                    ],
                )
                self.assertEqual(call_args[1][0][0], "onix_workflow_test/20210101/onix_workid_isbn_errors.jsonl.gz")

                # Test upload_aggregation_tables
                wf.upload_aggregation_tables(release)
                self.assertEqual(mock_upload_files_from_list.call_count, 1)
                _, call_args = mock_upload_files_from_list.call_args
                self.assertEqual(call_args["bucket_name"], "bucket_name")
                self.assertEqual(
                    call_args["blob_names"],
                    [
                        "onix_workflow_test/20210101/onix_workid_isbn.jsonl.gz",
                        "onix_workflow_test/20210101/onix_workid_isbn_errors.jsonl.gz",
                        "onix_workflow_test/20210101/onix_workfamilyid_isbn.jsonl.gz",
                    ],
                )
                self.assertEqual(
                    call_args["file_paths"],
                    [
                        "onix_workflow_test/20210101/onix_workid_isbn.jsonl.gz",
                        "onix_workflow_test/20210101/onix_workid_isbn_errors.jsonl.gz",
                        "onix_workflow_test/20210101/onix_workfamilyid_isbn.jsonl.gz",
                    ],
                )

                # Test bq_load_workid_lookup
                wf.bq_load_workid_lookup(release)
                self.assertEqual(mock_bq_load_lookup.call_count, 1)
                _, call_args = mock_bq_load_lookup.call_args
                self.assertEqual(call_args["project_id"], "project_id")
                self.assertEqual(call_args["transform_bucket"], "bucket_name")
                self.assertEqual(call_args["transform_blob"], "onix_workflow_test/20210101/onix_workid_isbn.jsonl.gz")
                self.assertEqual(call_args["dataset_id"], "onix_workflow")
                self.assertEqual(call_args["dataset_location"], "us")
                self.assertEqual(call_args["table_id"], "onix_workid_isbn")
                self.assertEqual(call_args["release_date"], pendulum.Pendulum(2021, 1, 1))
                self.assertEqual(call_args["source_format"], "NEWLINE_DELIMITED_JSON")
                self.assertEqual(call_args["dataset_description"], "ONIX workflow tables")

                # Test bq_load_workid_lookup_errors
                wf.bq_load_workid_lookup_errors(release)
                self.assertEqual(mock_bq_load_lookup.call_count, 2)
                _, call_args = mock_bq_load_lookup.call_args
                self.assertEqual(call_args["project_id"], "project_id")
                self.assertEqual(call_args["transform_bucket"], "bucket_name")
                self.assertEqual(
                    call_args["transform_blob"], "onix_workflow_test/20210101/onix_workid_isbn_errors.jsonl.gz"
                )
                self.assertEqual(call_args["dataset_id"], "onix_workflow")
                self.assertEqual(call_args["dataset_location"], "us")
                self.assertEqual(call_args["table_id"], "onix_workid_isbn_errors")
                self.assertEqual(call_args["release_date"], pendulum.Pendulum(2021, 1, 1))
                self.assertEqual(call_args["source_format"], "NEWLINE_DELIMITED_JSON")
                self.assertEqual(call_args["prefix"], "")
                self.assertEqual(call_args["schema_version"], "")
                self.assertEqual(call_args["dataset_description"], "ONIX workflow tables")

    @patch("observatory.dags.workflows.onix_workflow.run_bigquery_query")
    @patch("observatory.dags.workflows.onix_workflow.bq_load_shard_v2")
    @patch("observatory.dags.workflows.onix_workflow.upload_files_to_cloud_storage")
    @patch("observatory.dags.workflows.onix_workflow.list_to_jsonl_gz")
    @patch("observatory.dags.workflows.onix_workflow.select_table_suffixes")
    def test_create_and_upload_bq_isbn13_workfamilyid_lookup_table(
        self,
        mock_sel_table_suffixes,
        mock_write_to_file,
        mock_upload_files_from_list,
        mock_bq_load_lookup,
        mock_bq_query,
    ):
        mock_sel_table_suffixes.return_value = [pendulum.Pendulum(2021, 1, 1)]
        mock_bq_query.return_value = TestOnixWorkflow.onix_data

        env = ObservatoryEnvironment(self.project_id, self.data_location)
        with env.create():
            with CliRunner().isolated_filesystem():
                telescope_sensor = make_telescope_sensor(self.telescope.organisation.name, OnixTelescope.DAG_ID_PREFIX)
                wf = OnixWorkflow(
                    org_name=self.telescope.organisation.name,
                    gcp_project_id=self.telescope.organisation.gcp_project_id,
                    gcp_bucket_name=self.bucket_name,
                    telescope_sensor=telescope_sensor,
                )

                release = wf.make_release(execution_date=pendulum.Pendulum(2021, 1, 1))

                # Test works family aggregation
                wf.aggregate_works(release)
                self.assertEqual(mock_write_to_file.call_count, 3)
                call_args = mock_write_to_file.call_args_list
                lookup_table = {arg["isbn13"]: arg["work_family_id"] for arg in call_args[2][0][1]}
                self.assertTrue(
                    lookup_table["112"] == lookup_table["111"] and lookup_table["111"] == lookup_table["211"]
                )

                self.assertEqual(call_args[0][0][0], "onix_workflow_test/20210101/onix_workid_isbn.jsonl.gz")

                # Test bq_load_workfamilyid_lookup
                wf.upload_aggregation_tables(release)
                wf.bq_load_workfamilyid_lookup(release)
                self.assertEqual(mock_bq_load_lookup.call_count, 1)
                _, call_args = mock_bq_load_lookup.call_args
                self.assertEqual(call_args["project_id"], "project_id")
                self.assertEqual(call_args["transform_bucket"], "bucket_name")
                self.assertEqual(
                    call_args["transform_blob"], "onix_workflow_test/20210101/onix_workfamilyid_isbn.jsonl.gz"
                )

    @patch("observatory.dags.workflows.onix_workflow.create_bigquery_table_from_query")
    @patch("observatory.dags.workflows.onix_workflow.create_bigquery_dataset")
    @patch("observatory.dags.workflows.onix_workflow.select_table_suffixes")
    def test_create_oaebu_intermediate_table_tasks(
        self, mock_sel_table_suffixes, mock_create_bq_ds, mock_create_bq_table
    ):
        data_partners = [
            OaebuPartners(
                name="Test Partner",
                gcp_project_id="test_project",
                gcp_dataset_id="test_dataset",
                gcp_table_id="test_table",
                isbn_field_name="isbn",
            ),
            OaebuPartners(
                name="Test Partner",
                gcp_project_id="test_project",
                gcp_dataset_id="test_dataset",
                gcp_table_id="test_table2",
                gcp_table_date=pendulum.Pendulum(2021, 1, 1),
                isbn_field_name="isbn",
            ),
        ]

        mock_sel_table_suffixes.return_value = [pendulum.Pendulum(2021, 1, 1)]

        self.assertEqual(data_partners[0].gcp_table_date, None)

        with CliRunner().isolated_filesystem():
            telescope_sensor = make_telescope_sensor(self.telescope.organisation.name, OnixTelescope.DAG_ID_PREFIX)
            wf = OnixWorkflow(
                org_name=self.telescope.organisation.name,
                gcp_project_id=self.telescope.organisation.gcp_project_id,
                gcp_bucket_name=self.bucket_name,
                telescope_sensor=telescope_sensor,
                dag_id="dagid",
                data_partners=data_partners,
            )

            release = wf.make_release(execution_date=pendulum.Pendulum(2021, 1, 1))

            # Spin up tasks
            oaebu_task1, _ = wf.task_funcs[-3]
            self.assertEqual(oaebu_task1.__name__, "create_oaebu_intermediate_table.test_dataset.test_table")

            oaebu_task2, _ = wf.task_funcs[-2]
            self.assertEqual(oaebu_task2.__name__, "create_oaebu_intermediate_table.test_dataset.test_table2")

            # Run tasks
            oaebu_task1(release)
            _, call_args = mock_create_bq_ds.call_args
            self.assertEqual(call_args["project_id"], "project_id")
            self.assertEqual(call_args["dataset_id"], "oaebu_intermediate")
            self.assertEqual(call_args["location"], "us")

            _, call_args = mock_create_bq_table.call_args
            self.assertEqual(call_args["project_id"], "project_id")
            self.assertEqual(call_args["dataset_id"], "oaebu_intermediate")
            self.assertEqual(call_args["table_id"], "test_table_matched20210101")
            self.assertEqual(call_args["location"], "us")

            expected_sql = "\n\n\nWITH orig_wid AS (\nSELECT\n    orig.*, wid.work_id\nFROM\n    `test_project.test_dataset.test_table20210101` orig\nLEFT JOIN\n    `test_project.onix_workflow.onix_workid_isbn20210101` wid\nON\n    orig.isbn = wid.isbn13\n)\n\nSELECT\n    orig_wid.*, wfam.work_family_id\nFROM\n    orig_wid\nLEFT JOIN\n    `test_project.onix_workflow.onix_workfamilyid_isbn20210101` wfam\nON\n    orig_wid.isbn = wfam.isbn13"
            self.assertEqual(call_args["sql"], expected_sql)

            oaebu_task2(release)
            _, call_args = mock_create_bq_ds.call_args
            self.assertEqual(call_args["project_id"], "project_id")
            self.assertEqual(call_args["dataset_id"], "oaebu_intermediate")
            self.assertEqual(call_args["location"], "us")

            _, call_args = mock_create_bq_table.call_args
            self.assertEqual(call_args["project_id"], "project_id")
            self.assertEqual(call_args["dataset_id"], "oaebu_intermediate")
            self.assertEqual(call_args["table_id"], "test_table2_matched20210101")
            self.assertEqual(call_args["location"], "us")

            expected_sql = "\n\n\nWITH orig_wid AS (\nSELECT\n    orig.*, wid.work_id\nFROM\n    `test_project.test_dataset.test_table220210101` orig\nLEFT JOIN\n    `test_project.onix_workflow.onix_workid_isbn20210101` wid\nON\n    orig.isbn = wid.isbn13\n)\n\nSELECT\n    orig_wid.*, wfam.work_family_id\nFROM\n    orig_wid\nLEFT JOIN\n    `test_project.onix_workflow.onix_workfamilyid_isbn20210101` wfam\nON\n    orig_wid.isbn = wfam.isbn13"
            self.assertEqual(call_args["sql"], expected_sql)


class TestOnixWorkflowFunctional(ObservatoryTestCase):
    """ Functionally test the workflow"""

    class DummySensor(BaseSensorOperator):
        @apply_defaults
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)

        def poke(self, context):
            return True

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.host = "localhost"
        self.api_port = 5000
        self.gcp_project_id = os.getenv("TEST_GCP_PROJECT_ID")
        self.data_location = os.getenv("TEST_GCP_DATA_LOCATION")
        self.gcp_bucket_name = os.getenv("TEST_GCP_BUCKET_NAME")
        self.timestamp = pendulum.now()

        self.onix_table_id = "onix"
        self.test_onix_folder = "onix_workflow_test_onix_table"
        self.onix_release_date = pendulum.Pendulum(2021, 4, 1)
        self.onix_dataset_id = ""
        self.fake_partner_dataset = ""

    def setup_observatory_env(self, env):
        # Add Observatory API connection
        conn = Connection(conn_id=AirflowConns.OBSERVATORY_API, uri=f"http://:password@{self.host}:{self.api_port}")
        env.add_connection(conn)

        # Add an ONIX telescope
        dt = pendulum.utcnow()
        telescope_type = orm.TelescopeType(name="ONIX Telescope", type_id=TelescopeTypes.onix, created=dt, modified=dt)
        env.api_session.add(telescope_type)
        organisation = orm.Organisation(
            name="Curtin Press",
            created=dt,
            modified=dt,
            gcp_transform_bucket=self.gcp_bucket_name,
            gcp_project_id=self.gcp_project_id,
        )
        env.api_session.add(organisation)
        telescope = orm.Telescope(
            name="Curtin Press ONIX Telescope",
            telescope_type=telescope_type,
            organisation=organisation,
            modified=dt,
            created=dt,
        )
        env.api_session.add(telescope)

        # Add an ONIX workflow
        telescope_type = orm.TelescopeType(
            name="ONIX Telescope Workflow", type_id=TelescopeTypes.onix_workflow, created=dt, modified=dt
        )
        env.api_session.add(telescope_type)
        organisation = orm.Organisation(name="Curtin Press", created=dt, modified=dt)
        env.api_session.add(organisation)
        telescope = orm.Telescope(
            name="Curtin Press ONIX Workflow",
            telescope_type=telescope_type,
            organisation=organisation,
            modified=dt,
            created=dt,
        )
        env.api_session.add(telescope)
        env.api_session.commit()

        # Add a dataset for onix telescope
        self.onix_dataset_id = env.add_dataset()

    def delete_bucket_blobs(self):
        """ Delete test blob files"""

        delete_bucket_dir(bucket_name=self.gcp_bucket_name, prefix="onix_workflow")

    def setup_fake_onix_data_table(self):
        """Create a new onix data table with its own dataset id and table id, and populate it with some fake data."""

        # Upload fixture to bucket
        files = [os.path.join(test_fixtures_path("telescopes", "onix_workflow"), "onix.json")]
        blobs = [os.path.join(self.test_onix_folder, os.path.basename(file)) for file in files]
        upload_files_to_cloud_storage(bucket_name=self.gcp_bucket_name, blob_names=blobs, file_paths=files)

        # Load into bigquery
        table_id, _ = table_ids_from_path("onix.json")
        bq_load_shard_v2(
            project_id=self.gcp_project_id,
            transform_bucket=self.gcp_bucket_name,
            transform_blob=blobs[0],
            dataset_id=self.onix_dataset_id,
            dataset_location=self.data_location,
            table_id=table_id,
            release_date=self.onix_release_date,
            source_format=SourceFormat.NEWLINE_DELIMITED_JSON,
            dataset_description="Test Onix data for the workflow",
            **{},
        )

    def setup_fake_partner_data(self, env):
        self.fake_partner_dataset = env.add_dataset()

        # Upload fixture to bucket
        files = [os.path.join(test_fixtures_path("telescopes", "onix_workflow"), "jstor_country.json")]
        blobs = [os.path.join(self.test_onix_folder, os.path.basename(file)) for file in files]
        upload_files_to_cloud_storage(bucket_name=self.gcp_bucket_name, blob_names=blobs, file_paths=files)

        # Load into bigquery
        table_id, _ = table_ids_from_path("jstor_country.json")
        bq_load_shard_v2(
            project_id=self.gcp_project_id,
            transform_bucket=self.gcp_bucket_name,
            transform_blob=blobs[0],
            dataset_id=self.fake_partner_dataset,
            dataset_location=self.data_location,
            table_id=table_id,
            release_date=self.onix_release_date,
            source_format=SourceFormat.NEWLINE_DELIMITED_JSON,
            dataset_description="Test Onix data for the workflow",
            **{},
        )

        return [
            OaebuPartners(
                name="Test partner",
                gcp_project_id=self.gcp_project_id,
                gcp_dataset_id=self.fake_partner_dataset,
                gcp_table_id=table_id,
                isbn_field_name="ISBN",
            )
        ]

    def teardown_workflow(self):
        """Delete the testing onix data table, and delete the dataset."""

        delete_bigquery_dataset(self.gcp_project_id, "onix_workflow")
        print("========================================================== Deleting onix_workflow dataset")

    def teardown_intermediate_tables(self):
        delete_bigquery_dataset(self.gcp_project_id, "oaebu_intermediate")

    def test_telescope(self):
        """ Functional test of the ONIX workflow"""

        # Setup Observatory environment
        env = ObservatoryEnvironment(self.gcp_project_id, self.data_location)

        # Create the Observatory environment and run tests
        with env.create():
            telescope_sensor = TestOnixWorkflowFunctional.DummySensor(task_id="dummy_sensor", start_date=self.timestamp)

            # Set up environment
            self.setup_observatory_env(env)
            data_partners = self.setup_fake_partner_data(env)

            # Create fake data table. There's no guarantee the data was deleted so clean it again just in case.
            self.delete_bucket_blobs()
            self.teardown_workflow()
            self.teardown_intermediate_tables()
            self.setup_fake_onix_data_table()

            # Pull info from Observatory API
            api = make_observatory_api()
            telescope_type = api.get_telescope_type(type_id=TelescopeTypes.onix)
            telescopes = api.get_telescopes(telescope_type_id=telescope_type.id, limit=1000)

            # Get the parameters
            self.assertEqual(len(telescopes), 1)
            org_name = telescopes[0].organisation.name
            gcp_bucket_name = telescopes[0].organisation.gcp_transform_bucket
            gcp_project_id = telescopes[0].organisation.gcp_project_id

            # Setup telescope
            telescope = OnixWorkflow(
                org_name=org_name,
                gcp_project_id=gcp_project_id,
                gcp_bucket_name=gcp_bucket_name,
                telescope_sensor=telescope_sensor,
                onix_dataset_id=self.onix_dataset_id,
                onix_table_id=self.onix_table_id,
                data_partners=data_partners,
            )
            workflow_dag = telescope.make_dag()

            # Check sensors ready
            env.run_task(telescope_sensor.task_id, workflow_dag, self.timestamp)

            # Aggregate works
            env.run_task(telescope.aggregate_works.__name__, workflow_dag, self.timestamp)

            # Upload aggregation tables
            env.run_task(telescope.upload_aggregation_tables.__name__, workflow_dag, self.timestamp)

            # Load work id table into bigquery
            env.run_task(telescope.bq_load_workid_lookup.__name__, workflow_dag, self.timestamp)

            # Load work id errors table into bigquery
            env.run_task(telescope.bq_load_workid_lookup_errors.__name__, workflow_dag, self.timestamp)

            # Load work family id table into bigquery
            env.run_task(telescope.bq_load_workfamilyid_lookup.__name__, workflow_dag, self.timestamp)

            # Create oaebu intermediate tables
            oaebu_dataset = data_partners[0].gcp_dataset_id
            oaebu_table = data_partners[0].gcp_table_id
            env.run_task(
                f"{telescope.create_oaebu_intermediate_table.__name__}.{oaebu_dataset}.{oaebu_table}",
                workflow_dag,
                self.timestamp,
            )

            # Test conditions
            release_suffix = self.timestamp.strftime("%Y%m%d")

            transform_path = os.path.join("onix_workflow_curtin_press", release_suffix, "onix_workid_isbn.jsonl.gz")
            self.assert_blob_integrity(self.gcp_bucket_name, transform_path, transform_path)

            transform_path = os.path.join(
                "onix_workflow_curtin_press", release_suffix, "onix_workfamilyid_isbn.jsonl.gz"
            )
            self.assert_blob_integrity(self.gcp_bucket_name, transform_path, transform_path)

            transform_path = os.path.join(
                "onix_workflow_curtin_press", release_suffix, "onix_workid_isbn_errors.jsonl.gz"
            )
            self.assert_blob_integrity(self.gcp_bucket_name, transform_path, transform_path)

            table_id = f"{self.gcp_project_id}.onix_workflow.onix_workid_isbn{release_suffix}"

            self.assert_table_integrity(table_id, 3)

            table_id = f"{self.gcp_project_id}.onix_workflow.onix_workfamilyid_isbn{release_suffix}"
            self.assert_table_integrity(table_id, 3)

            table_id = f"{self.gcp_project_id}.onix_workflow.onix_workid_isbn_errors{release_suffix}"
            self.assert_table_integrity(table_id, 1)

            # Validate the joins worked
            sql = f"SELECT ISBN, work_id, work_family_id from {self.gcp_project_id}.oaebu_intermediate.jstor_country_matched{release_suffix}"
            records = run_bigquery_query(sql)
            oaebu_works = {record["ISBN"]: record["work_id"] for record in records}
            oaebu_wfam = {record["ISBN"]: record["work_family_id"] for record in records}

            self.assertTrue(
                oaebu_works["111"] == oaebu_works["112"]
                and oaebu_works["111"] != oaebu_works["211"]
                and oaebu_works["113"] is None
            )

            self.assertTrue(
                oaebu_wfam["111"] == oaebu_wfam["112"]
                and oaebu_wfam["112"] == oaebu_wfam["211"]
                and oaebu_wfam["113"] is None
            )

            # Cleanup
            env.run_task(telescope.cleanup.__name__, workflow_dag, self.timestamp)

            # Test data teardown
            self.teardown_workflow()
            self.teardown_intermediate_tables()
            self.delete_bucket_blobs()
