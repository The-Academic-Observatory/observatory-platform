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

import hashlib
import os
import unittest
from unittest.mock import MagicMock, Mock, patch

import observatory.api.server.orm as orm
import pendulum
from airflow.exceptions import AirflowException
from airflow.models.connection import Connection
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
from click.testing import CliRunner
from google.cloud import bigquery
from google.cloud.bigquery import SourceFormat
from observatory.api.client.identifiers import TelescopeTypes
from observatory.api.server.orm import Organisation
from observatory.dags.workflows.oaebu_partners import OaebuPartnerName, OaebuPartners
from observatory.dags.workflows.onix_workflow import OnixWorkflow, OnixWorkflowRelease
from observatory.platform.utils.airflow_utils import AirflowConns
from observatory.platform.utils.gc_utils import (
    delete_bigquery_dataset,
    delete_bucket_dir,
    run_bigquery_query,
    upload_files_to_cloud_storage,
)
from observatory.platform.utils.telescope_utils import make_observatory_api
from observatory.platform.utils.template_utils import (
    bq_load_partition,
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

        with patch("observatory.dags.workflows.onix_workflow.select_table_shard_dates") as mock_sel_table_suffixes:
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

    @patch("observatory.dags.workflows.onix_workflow.OnixWorkflow.make_release")
    @patch("observatory.dags.workflows.onix_workflow.select_table_shard_dates")
    def test_ctor_gen_dag_id(self, mock_sel_table_suffixes, mock_mr):
        mock_sel_table_suffixes.return_value = [pendulum.Pendulum(2021, 1, 1)]
        with CliRunner().isolated_filesystem():
            mock_mr.return_value = [
                OnixWorkflowRelease(
                    dag_id="onix_workflow_test",
                    release_date=pendulum.Pendulum(2021, 1, 1),
                    gcp_project_id=self.telescope.organisation.gcp_project_id,
                    gcp_bucket_name=self.bucket_name,
                    onix_dataset_id="",
                    onix_table_id="onix",
                )
            ]
            wf = OnixWorkflow(
                org_name=self.telescope.organisation.name,
                gcp_project_id=self.telescope.organisation.gcp_project_id,
                gcp_bucket_name=self.bucket_name,
            )

            releases = wf.make_release(execution_date=pendulum.Pendulum(2021, 1, 1))
            release = releases[0]
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

    @patch("observatory.dags.workflows.onix_workflow.OnixWorkflow.make_release")
    @patch("observatory.dags.workflows.onix_workflow.select_table_shard_dates")
    def test_ctor_gen_assign_dag_id(self, mock_sel_table_suffixes, mock_mr):
        mock_sel_table_suffixes.return_value = [pendulum.Pendulum(2021, 1, 1)]
        with CliRunner().isolated_filesystem():
            wf = OnixWorkflow(
                org_name=self.telescope.organisation.name,
                gcp_project_id=self.telescope.organisation.gcp_project_id,
                gcp_bucket_name=self.bucket_name,
                dag_id="dagid",
            )

            mock_mr.return_value = [
                OnixWorkflowRelease(
                    dag_id="dagid",
                    release_date=pendulum.Pendulum(2021, 1, 1),
                    gcp_project_id=self.telescope.organisation.gcp_project_id,
                    gcp_bucket_name=self.bucket_name,
                    onix_dataset_id="",
                    onix_table_id="onix",
                )
            ]

            releases = wf.make_release(execution_date=pendulum.Pendulum(2021, 1, 1))
            release = releases[0]

            self.assertEqual(release.dag_id, "dagid")
            self.assertEqual(release.workslookup_filename, "dagid/20210101/onix_workid_isbn.jsonl.gz")
            self.assertEqual(release.workslookup_errors_filename, "dagid/20210101/onix_workid_isbn_errors.jsonl.gz")
            self.assertEqual(release.worksfamilylookup_filename, "dagid/20210101/onix_workfamilyid_isbn.jsonl.gz")

            self.assertEqual(release.onix_table_id, "onix")
            self.assertEqual(release.release_date, pendulum.Pendulum(2021, 1, 1, 0, 0, 0, 0))
            self.assertEqual(release.project_id, "project_id")
            self.assertEqual(release.transform_bucket, "bucket_name")
            self.assertTrue(wf.sensors[0] != None)

    @patch("observatory.dags.workflows.onix_workflow.OnixWorkflow.make_release")
    @patch("observatory.dags.workflows.onix_workflow.select_table_shard_dates")
    def test_ctor(self, mock_sel_table_suffixes, mock_mr):
        mock_sel_table_suffixes.return_value = [pendulum.Pendulum(2021, 1, 1)]
        with CliRunner().isolated_filesystem():
            wf = OnixWorkflow(
                org_name=self.telescope.organisation.name,
                gcp_project_id=self.telescope.organisation.gcp_project_id,
                gcp_bucket_name=self.bucket_name,
            )

            mock_mr.return_value = [
                OnixWorkflowRelease(
                    dag_id="onix_workflow_test",
                    release_date=pendulum.Pendulum(2021, 1, 1),
                    gcp_project_id=self.telescope.organisation.gcp_project_id,
                    gcp_bucket_name=self.bucket_name,
                    onix_dataset_id="onix",
                    onix_table_id="onix",
                )
            ]

            releases = wf.make_release(execution_date=pendulum.Pendulum(2021, 1, 1))
            release = releases[0]

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
    @patch("observatory.dags.workflows.onix_workflow.select_table_shard_dates")
    def test_get_onix_records(self, mock_sel_table_suffixes, mock_bq_query):
        mock_sel_table_suffixes.return_value = [pendulum.Pendulum(2021, 1, 1)]
        mock_bq_query.return_value = TestOnixWorkflow.onix_data
        with CliRunner().isolated_filesystem():
            wf = OnixWorkflow(
                org_name=self.telescope.organisation.name,
                gcp_project_id=self.telescope.organisation.gcp_project_id,
                gcp_bucket_name=self.bucket_name,
            )
            records = wf.get_onix_records("project_id", "ds_id", "table_id")
            self.assertEqual(len(records), 3)
            self.assertEqual(records[0]["ISBN13"], "111")

    def test_make_release(self):
        ti_mock = Mock()
        ti_mock.xcom_pull = MagicMock(return_value=[{"release_date": pendulum.Pendulum(2021, 1, 1)}])

        with CliRunner().isolated_filesystem():
            wf = OnixWorkflow(
                org_name=self.telescope.organisation.name,
                gcp_project_id=self.telescope.organisation.gcp_project_id,
                gcp_bucket_name=self.bucket_name,
            )

            kwargs = {"ti": ti_mock}
            releases = wf.make_release(**kwargs)
            self.assertEqual(len(releases), 1)
            self.assertEqual(releases[0].dag_id, "onix_workflow_test")
            self.assertEqual(releases[0].release_date, pendulum.Pendulum(2021, 1, 1))
            self.assertEqual(releases[0].project_id, "project_id")
            self.assertEqual(releases[0].onix_dataset_id, "onix")
            self.assertEqual(releases[0].onix_table_id, "onix")
            self.assertEqual(releases[0].bucket_name, "bucket_name")

    @patch("observatory.dags.workflows.onix_workflow.OnixWorkflow.make_release")
    @patch("observatory.dags.workflows.onix_workflow.select_table_shard_dates")
    def test_cleanup(self, mock_sel_table_suffixes, mock_mr):
        mock_sel_table_suffixes.return_value = [pendulum.Pendulum(2021, 1, 1)]
        with CliRunner().isolated_filesystem():
            wf = OnixWorkflow(
                org_name=self.telescope.organisation.name,
                gcp_project_id=self.telescope.organisation.gcp_project_id,
                gcp_bucket_name=self.bucket_name,
            )

            mock_mr.return_value = [
                OnixWorkflowRelease(
                    dag_id="onix_workflow_test",
                    release_date=pendulum.Pendulum(2021, 1, 1),
                    gcp_project_id=self.telescope.organisation.gcp_project_id,
                    gcp_bucket_name=self.bucket_name,
                    onix_dataset_id="onix",
                    onix_table_id="onix",
                )
            ]

            releases = wf.make_release(execution_date=pendulum.Pendulum(2021, 1, 1))
            release = releases[0]
            self.assertTrue(os.path.isdir(release.transform_folder))
            wf.cleanup(releases)
            self.assertFalse(os.path.isdir(release.transform_folder))

    def test_dag_structure(self):
        data_partners = [
            OaebuPartners(
                name=OaebuPartnerName.jstor_country,
                gcp_project_id="project",
                gcp_dataset_id="dataset",
                gcp_table_id="jstor_country",
                isbn_field_name="isbn",
                sharded=False,
            ),
            OaebuPartners(
                name=OaebuPartnerName.oapen_irus_uk,
                gcp_project_id="project",
                gcp_dataset_id="dataset",
                gcp_table_id="oapen_irus_uk",
                isbn_field_name="ISBN",
                sharded=False,
            ),
            OaebuPartners(
                name=OaebuPartnerName.google_books_sales,
                gcp_project_id="project",
                gcp_dataset_id="dataset",
                gcp_table_id="google_books_sales",
                isbn_field_name="Primary_ISBN",
                sharded=False,
            ),
            OaebuPartners(
                name=OaebuPartnerName.google_books_traffic,
                gcp_project_id="project",
                gcp_dataset_id="dataset",
                gcp_table_id="google_books_traffic",
                isbn_field_name="Primary_ISBN",
                sharded=False,
            ),
        ]

        with CliRunner().isolated_filesystem():
            wf = OnixWorkflow(
                org_name=self.telescope.organisation.name,
                gcp_project_id=self.telescope.organisation.gcp_project_id,
                gcp_bucket_name=self.bucket_name,
                data_partners=data_partners,
            )
            dag = wf.make_dag()
            # print("------------------------------")
            # print(dag.__dict__)
            # print("------------------------------")
            self.assert_dag_structure(
                {
                    "onix_test_sensor": ["continue_workflow"],
                    "continue_workflow": ["aggregate_works"],
                    "aggregate_works": ["upload_aggregation_tables"],
                    "upload_aggregation_tables": ["bq_load_workid_lookup"],
                    "bq_load_workid_lookup": ["bq_load_workid_lookup_errors"],
                    "bq_load_workid_lookup_errors": ["bq_load_workfamilyid_lookup"],
                    "bq_load_workfamilyid_lookup": ["create_oaebu_intermediate_table.dataset.jstor_country"],
                    "create_oaebu_intermediate_table.dataset.jstor_country": [
                        "create_oaebu_intermediate_table.dataset.oapen_irus_uk"
                    ],
                    "create_oaebu_intermediate_table.dataset.oapen_irus_uk": [
                        "create_oaebu_intermediate_table.dataset.google_books_sales"
                    ],
                    "create_oaebu_intermediate_table.dataset.google_books_sales": [
                        "create_oaebu_intermediate_table.dataset.google_books_traffic"
                    ],
                    "create_oaebu_intermediate_table.dataset.google_books_traffic": ["create_oaebu_data_qa_onix_isbn"],
                    "create_oaebu_data_qa_onix_isbn": ["create_oaebu_data_qa_onix_aggregate"],
                    "create_oaebu_data_qa_onix_aggregate": ["create_oaebu_data_qa_jstor_isbn.dataset.jstor_country"],
                    "create_oaebu_data_qa_jstor_isbn.dataset.jstor_country": [
                        "create_oaebu_data_qa_intermediate_unmatched_workid.dataset.jstor_country"
                    ],
                    "create_oaebu_data_qa_intermediate_unmatched_workid.dataset.jstor_country": [
                        "create_oaebu_data_qa_oapen_irus_uk_isbn"
                    ],
                    "create_oaebu_data_qa_oapen_irus_uk_isbn": [
                        "create_oaebu_data_qa_intermediate_unmatched_workid.dataset.oapen_irus_uk"
                    ],
                    "create_oaebu_data_qa_intermediate_unmatched_workid.dataset.oapen_irus_uk": [
                        "create_oaebu_data_qa_google_books_sales_isbn"
                    ],
                    "create_oaebu_data_qa_google_books_sales_isbn": [
                        "create_oaebu_data_qa_intermediate_unmatched_workid.dataset.google_books_sales"
                    ],
                    "create_oaebu_data_qa_intermediate_unmatched_workid.dataset.google_books_sales": [
                        "create_oaebu_data_qa_google_books_traffic_isbn"
                    ],
                    "create_oaebu_data_qa_google_books_traffic_isbn": [
                        "create_oaebu_data_qa_intermediate_unmatched_workid.dataset.google_books_traffic"
                    ],
                    "create_oaebu_data_qa_intermediate_unmatched_workid.dataset.google_books_traffic": ["cleanup"],
                    "cleanup": [],
                },
                dag,
            )

    @patch("observatory.dags.workflows.onix_workflow.select_table_shard_dates")
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

    @patch("observatory.dags.workflows.onix_workflow.OnixWorkflow.make_release")
    @patch("observatory.dags.workflows.onix_workflow.run_bigquery_query")
    @patch("observatory.dags.workflows.onix_workflow.bq_load_shard_v2")
    @patch("observatory.dags.workflows.onix_workflow.upload_files_to_cloud_storage")
    @patch("observatory.dags.workflows.onix_workflow.list_to_jsonl_gz")
    @patch("observatory.dags.workflows.onix_workflow.select_table_shard_dates")
    def test_create_and_upload_bq_isbn13_workid_lookup_table(
        self,
        mock_sel_table_suffixes,
        mock_write_to_file,
        mock_upload_files_from_list,
        mock_bq_load_lookup,
        mock_bq_query,
        mock_mr,
    ):
        mock_sel_table_suffixes.return_value = [pendulum.Pendulum(2021, 1, 1)]
        mock_bq_query.return_value = TestOnixWorkflow.onix_data
        env = ObservatoryEnvironment(self.project_id, self.data_location)
        with env.create():
            with CliRunner().isolated_filesystem():
                wf = OnixWorkflow(
                    org_name=self.telescope.organisation.name,
                    gcp_project_id=self.telescope.organisation.gcp_project_id,
                    gcp_bucket_name=self.bucket_name,
                )

                mock_mr.return_value = [
                    OnixWorkflowRelease(
                        dag_id="onix_workflow_test",
                        release_date=pendulum.Pendulum(2021, 1, 1),
                        gcp_project_id=self.telescope.organisation.gcp_project_id,
                        gcp_bucket_name=self.bucket_name,
                        onix_dataset_id="onix",
                        onix_table_id="onix",
                    )
                ]

                releases = wf.make_release()
                release = releases[0]

                # Test works aggregation
                wf.aggregate_works(releases)
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
                            "Error": "Product ISBN13:111 is a manifestation of ISBN13:113, which is not given as a product identifier in any ONIX product record."
                        },
                    ],
                )
                self.assertEqual(call_args[1][0][0], "onix_workflow_test/20210101/onix_workid_isbn_errors.jsonl.gz")

                # Test upload_aggregation_tables
                wf.upload_aggregation_tables(releases)
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
                wf.bq_load_workid_lookup(releases)
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
                wf.bq_load_workid_lookup_errors(releases)
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

    @patch("observatory.dags.workflows.onix_workflow.OnixWorkflow.make_release")
    @patch("observatory.dags.workflows.onix_workflow.run_bigquery_query")
    @patch("observatory.dags.workflows.onix_workflow.bq_load_shard_v2")
    @patch("observatory.dags.workflows.onix_workflow.upload_files_to_cloud_storage")
    @patch("observatory.dags.workflows.onix_workflow.list_to_jsonl_gz")
    @patch("observatory.dags.workflows.onix_workflow.select_table_shard_dates")
    def test_create_and_upload_bq_isbn13_workfamilyid_lookup_table(
        self,
        mock_sel_table_suffixes,
        mock_write_to_file,
        mock_upload_files_from_list,
        mock_bq_load_lookup,
        mock_bq_query,
        mock_mr,
    ):
        mock_sel_table_suffixes.return_value = [pendulum.Pendulum(2021, 1, 1)]
        mock_bq_query.return_value = TestOnixWorkflow.onix_data

        env = ObservatoryEnvironment(self.project_id, self.data_location)
        with env.create():
            with CliRunner().isolated_filesystem():
                wf = OnixWorkflow(
                    org_name=self.telescope.organisation.name,
                    gcp_project_id=self.telescope.organisation.gcp_project_id,
                    gcp_bucket_name=self.bucket_name,
                )

                mock_mr.return_value = [
                    OnixWorkflowRelease(
                        dag_id="onix_workflow_test",
                        release_date=pendulum.Pendulum(2021, 1, 1),
                        gcp_project_id=self.telescope.organisation.gcp_project_id,
                        gcp_bucket_name=self.bucket_name,
                        onix_dataset_id="onix",
                        onix_table_id="onix",
                    )
                ]
                releases = wf.make_release()
                release = releases[0]

                # Test works family aggregation
                wf.aggregate_works(releases)
                self.assertEqual(mock_write_to_file.call_count, 3)
                call_args = mock_write_to_file.call_args_list
                lookup_table = {arg["isbn13"]: arg["work_family_id"] for arg in call_args[2][0][1]}
                self.assertTrue(
                    lookup_table["112"] == lookup_table["111"] and lookup_table["111"] == lookup_table["211"]
                )

                self.assertEqual(call_args[0][0][0], "onix_workflow_test/20210101/onix_workid_isbn.jsonl.gz")

                # Test bq_load_workfamilyid_lookup
                wf.upload_aggregation_tables(releases)
                wf.bq_load_workfamilyid_lookup(releases)
                self.assertEqual(mock_bq_load_lookup.call_count, 1)
                _, call_args = mock_bq_load_lookup.call_args
                self.assertEqual(call_args["project_id"], "project_id")
                self.assertEqual(call_args["transform_bucket"], "bucket_name")
                self.assertEqual(
                    call_args["transform_blob"], "onix_workflow_test/20210101/onix_workfamilyid_isbn.jsonl.gz"
                )

    def test_create_oaebu_intermediate_table(self):
        with CliRunner().isolated_filesystem():
            wf = OnixWorkflow(
                org_name=self.telescope.organisation.name,
                gcp_project_id=self.telescope.organisation.gcp_project_id,
                gcp_bucket_name=self.bucket_name,
            )
            with patch(
                "observatory.dags.workflows.onix_workflow.create_bigquery_table_from_query", return_value=False
            ) as _:
                with patch("observatory.dags.workflows.onix_workflow.create_bigquery_dataset") as _:
                    self.assertRaises(
                        AirflowException,
                        wf.create_oaebu_intermediate_table,
                        releases=[
                            OnixWorkflowRelease(
                                dag_id="onix_workflow_test",
                                release_date=pendulum.Pendulum(2021, 1, 1),
                                gcp_project_id=self.telescope.organisation.gcp_project_id,
                                gcp_bucket_name=self.bucket_name,
                                onix_dataset_id="onix",
                                onix_table_id="onix",
                            )
                        ],
                        orig_project_id="project",
                        orig_dataset="dataset",
                        orig_table="table",
                        orig_isbn="isbn",
                        sharded=False,
                    )

    @patch("observatory.dags.workflows.onix_workflow.OnixWorkflow.make_release")
    @patch("observatory.dags.workflows.onix_workflow.create_bigquery_table_from_query")
    @patch("observatory.dags.workflows.onix_workflow.create_bigquery_dataset")
    @patch("observatory.dags.workflows.onix_workflow.select_table_shard_dates")
    def test_create_oaebu_intermediate_table_tasks(
        self, mock_sel_table_suffixes, mock_create_bq_ds, mock_create_bq_table, mock_mr
    ):
        data_partners = [
            OaebuPartners(
                name="Test Partner",
                gcp_project_id="test_project",
                gcp_dataset_id="test_dataset",
                gcp_table_id="test_table",
                isbn_field_name="isbn",
                sharded=True,
            ),
            OaebuPartners(
                name="Test Partner",
                gcp_project_id="test_project",
                gcp_dataset_id="test_dataset",
                gcp_table_id="test_table2",
                isbn_field_name="isbn",
                sharded=True,
            ),
        ]

        mock_sel_table_suffixes.return_value = [pendulum.Pendulum(2021, 1, 1)]
        mock_create_bq_table.return_value = True

        with CliRunner().isolated_filesystem():
            wf = OnixWorkflow(
                org_name=self.telescope.organisation.name,
                gcp_project_id=self.telescope.organisation.gcp_project_id,
                gcp_bucket_name=self.bucket_name,
                dag_id="dagid",
                data_partners=data_partners,
            )

            mock_mr.return_value = [
                OnixWorkflowRelease(
                    dag_id="onix_workflow_test",
                    release_date=pendulum.Pendulum(2021, 1, 1),
                    gcp_project_id=self.telescope.organisation.gcp_project_id,
                    gcp_bucket_name=self.bucket_name,
                    onix_dataset_id="onix",
                    onix_table_id="onix",
                )
            ]

            releases = wf.make_release()
            release = releases[0]

            # Spin up tasks
            oaebu_task1, _ = wf.task_funcs[5]
            self.assertEqual(oaebu_task1.__name__, "create_oaebu_intermediate_table.test_dataset.test_table")

            oaebu_task2, _ = wf.task_funcs[6]
            self.assertEqual(oaebu_task2.__name__, "create_oaebu_intermediate_table.test_dataset.test_table2")

            # Run tasks
            oaebu_task1(releases)
            _, call_args = mock_create_bq_ds.call_args
            self.assertEqual(call_args["project_id"], "project_id")
            self.assertEqual(call_args["dataset_id"], "oaebu_intermediate")
            self.assertEqual(call_args["location"], "us")

            _, call_args = mock_create_bq_table.call_args
            self.assertEqual(call_args["project_id"], "project_id")
            self.assertEqual(call_args["dataset_id"], "oaebu_intermediate")
            self.assertEqual(call_args["table_id"], "test_dataset_test_table_matched20210101")
            self.assertEqual(call_args["location"], "us")
            sql_hash = hashlib.md5(call_args["sql"].encode("utf-8"))
            sql_hash = sql_hash.hexdigest()
            expected_hash = "1dabe0b435be583fdbae6ad4421f579b"
            self.assertEqual(sql_hash, expected_hash)

            oaebu_task2(releases)
            _, call_args = mock_create_bq_ds.call_args
            self.assertEqual(call_args["project_id"], "project_id")
            self.assertEqual(call_args["dataset_id"], "oaebu_intermediate")
            self.assertEqual(call_args["location"], "us")

            _, call_args = mock_create_bq_table.call_args
            self.assertEqual(call_args["project_id"], "project_id")
            self.assertEqual(call_args["dataset_id"], "oaebu_intermediate")
            self.assertEqual(call_args["table_id"], "test_dataset_test_table2_matched20210101")
            self.assertEqual(call_args["location"], "us")

            sql_hash = hashlib.md5(call_args["sql"].encode("utf-8"))
            sql_hash = sql_hash.hexdigest()
            expected_hash = "200d285a02bdc14b3229874f50d8a30d"
            self.assertEqual(sql_hash, expected_hash)

    @patch("observatory.dags.workflows.onix_workflow.create_bigquery_table_from_query")
    @patch("observatory.dags.workflows.onix_workflow.create_bigquery_dataset")
    def test_create_oaebu_data_qa_onix_isbn(self, mock_bq_ds, mock_bq_table_query):
        with CliRunner().isolated_filesystem():
            wf = OnixWorkflow(
                org_name=self.telescope.organisation.name,
                gcp_project_id=self.telescope.organisation.gcp_project_id,
                gcp_bucket_name=self.bucket_name,
            )
            mock_bq_table_query.return_value = True
            wf.create_oaebu_data_qa_onix_isbn(
                [
                    OnixWorkflowRelease(
                        dag_id="onix_workflow_test",
                        release_date=pendulum.Pendulum(2021, 1, 1),
                        gcp_project_id=self.telescope.organisation.gcp_project_id,
                        gcp_bucket_name=self.bucket_name,
                        onix_dataset_id="onix",
                        onix_table_id="onix",
                    )
                ]
            )

            _, call_args = mock_bq_ds.call_args
            self.assertEqual(call_args["dataset_id"], "oaebu_data_qa")

            _, call_args = mock_bq_table_query.call_args
            self.assertEqual(call_args["project_id"], "project_id")
            self.assertEqual(call_args["dataset_id"], "oaebu_data_qa")
            self.assertEqual(call_args["table_id"], "onix_invalid_isbn20210101")
            self.assertEqual(call_args["location"], "us")

            sql_hash = hashlib.md5(call_args["sql"].encode("utf-8"))
            sql_hash = sql_hash.hexdigest()
            expected_hash = "98d0a2b99961e9c63c3ade6faf9997f9"
            self.assertEqual(sql_hash, expected_hash)

            mock_bq_table_query.return_value = False
            self.assertRaises(
                AirflowException,
                wf.create_oaebu_data_qa_onix_isbn,
                [
                    OnixWorkflowRelease(
                        dag_id="onix_workflow_test",
                        release_date=pendulum.Pendulum(2021, 1, 1),
                        gcp_project_id=self.telescope.organisation.gcp_project_id,
                        gcp_bucket_name=self.bucket_name,
                        onix_dataset_id="onix",
                        onix_table_id="onix",
                    )
                ],
            )

    @patch("observatory.dags.workflows.onix_workflow.create_bigquery_table_from_query")
    @patch("observatory.dags.workflows.onix_workflow.create_bigquery_dataset")
    def test_create_oaebu_data_qa_onix_aggregate(self, mock_bq_ds, mock_bq_table_query):
        with CliRunner().isolated_filesystem():
            wf = OnixWorkflow(
                org_name=self.telescope.organisation.name,
                gcp_project_id=self.telescope.organisation.gcp_project_id,
                gcp_bucket_name=self.bucket_name,
            )
            mock_bq_table_query.return_value = True
            wf.create_oaebu_data_qa_onix_aggregate(
                [
                    OnixWorkflowRelease(
                        dag_id="onix_workflow_test",
                        release_date=pendulum.Pendulum(2021, 1, 1),
                        gcp_project_id=self.telescope.organisation.gcp_project_id,
                        gcp_bucket_name=self.bucket_name,
                        onix_dataset_id="onix",
                        onix_table_id="onix",
                    )
                ]
            )

            _, call_args = mock_bq_ds.call_args
            self.assertEqual(call_args["dataset_id"], "oaebu_data_qa")

            _, call_args = mock_bq_table_query.call_args
            self.assertEqual(call_args["project_id"], "project_id")
            self.assertEqual(call_args["dataset_id"], "oaebu_data_qa")
            self.assertEqual(call_args["table_id"], "onix_aggregate_metrics20210101")
            self.assertEqual(call_args["location"], "us")

            sql_hash = hashlib.md5(call_args["sql"].encode("utf-8"))
            sql_hash = sql_hash.hexdigest()
            expected_hash = "e457bc4d32a3bbb75ef215009da917b3"
            self.assertEqual(sql_hash, expected_hash)

            mock_bq_table_query.return_value = False
            self.assertRaises(
                AirflowException,
                wf.create_oaebu_data_qa_onix_aggregate,
                [
                    OnixWorkflowRelease(
                        dag_id="onix_workflow_test",
                        release_date=pendulum.Pendulum(2021, 1, 1),
                        gcp_project_id=self.telescope.organisation.gcp_project_id,
                        gcp_bucket_name=self.bucket_name,
                        onix_dataset_id="onix",
                        onix_table_id="onix",
                    )
                ],
            )

    @patch("observatory.dags.workflows.onix_workflow.select_table_shard_dates")
    @patch("observatory.dags.workflows.onix_workflow.create_bigquery_table_from_query")
    @patch("observatory.dags.workflows.onix_workflow.create_bigquery_dataset")
    def test_create_oaebu_data_qa_jstor_isbn(self, mock_bq_ds, mock_bq_table_query, mock_sel_table_suffixes):
        mock_sel_table_suffixes.return_value = [pendulum.Pendulum(2021, 1, 1)]

        data_partners = [
            OaebuPartners(
                name=OaebuPartnerName.jstor_country,
                gcp_project_id="project",
                gcp_dataset_id="jstor",
                gcp_table_id="country",
                isbn_field_name="ISBN",
                sharded=False,
            )
        ]
        with CliRunner().isolated_filesystem():
            wf = OnixWorkflow(
                org_name=self.telescope.organisation.name,
                gcp_project_id=self.telescope.organisation.gcp_project_id,
                gcp_bucket_name=self.bucket_name,
                data_partners=data_partners,
            )
            mock_bq_table_query.return_value = True
            wf.create_oaebu_data_qa_jstor_isbn(
                releases=[
                    OnixWorkflowRelease(
                        dag_id="onix_workflow_test",
                        release_date=pendulum.Pendulum(2021, 1, 1),
                        gcp_project_id=self.telescope.organisation.gcp_project_id,
                        gcp_bucket_name=self.bucket_name,
                        onix_dataset_id="onix",
                        onix_table_id="onix",
                    )
                ],
                project_id="project",
                orig_dataset_id="jstor",
                orig_table="country",
                sharded=True,
            )

            self.assertEqual(mock_sel_table_suffixes.call_count, 1)

            wf.create_oaebu_data_qa_jstor_isbn(
                releases=[
                    OnixWorkflowRelease(
                        dag_id="onix_workflow_test",
                        release_date=pendulum.Pendulum(2021, 1, 1),
                        gcp_project_id=self.telescope.organisation.gcp_project_id,
                        gcp_bucket_name=self.bucket_name,
                        onix_dataset_id="onix",
                        onix_table_id="onix",
                    )
                ],
                project_id="project",
                orig_dataset_id="jstor",
                orig_table="country",
                sharded=False,
            )
            self.assertEqual(mock_sel_table_suffixes.call_count, 1)

            _, call_args = mock_bq_table_query.call_args
            self.assertEqual(call_args["project_id"], "project")
            self.assertEqual(call_args["dataset_id"], "oaebu_data_qa")
            self.assertEqual(call_args["table_id"], "jstor_invalid_eisbn20210101")
            self.assertEqual(call_args["location"], "us")

            sql_hash = hashlib.md5(call_args["sql"].encode("utf-8"))
            sql_hash = sql_hash.hexdigest()
            expected_hash = "7a9a66b5a0295ecdd53d245e659f3e85"
            self.assertEqual(sql_hash, expected_hash)

    @patch("observatory.dags.workflows.onix_workflow.select_table_shard_dates")
    @patch("observatory.dags.workflows.onix_workflow.create_bigquery_table_from_query")
    @patch("observatory.dags.workflows.onix_workflow.create_bigquery_dataset")
    def test_create_oaebu_data_qa_google_books_sales_isbn(
        self, mock_bq_ds, mock_bq_table_query, mock_sel_table_suffixes
    ):
        mock_sel_table_suffixes.return_value = [pendulum.Pendulum(2021, 1, 1)]

        data_partners = [
            OaebuPartners(
                name=OaebuPartnerName.google_books_sales,
                gcp_project_id="project",
                gcp_dataset_id="google_books",
                gcp_table_id="sales",
                isbn_field_name="Primary_ISBN",
                sharded=False,
            )
        ]
        with CliRunner().isolated_filesystem():
            wf = OnixWorkflow(
                org_name=self.telescope.organisation.name,
                gcp_project_id=self.telescope.organisation.gcp_project_id,
                gcp_bucket_name=self.bucket_name,
                data_partners=data_partners,
            )
            mock_bq_table_query.return_value = True
            wf.create_oaebu_data_qa_google_books_sales_isbn(
                releases=[
                    OnixWorkflowRelease(
                        dag_id="onix_workflow_test",
                        release_date=pendulum.Pendulum(2021, 1, 1),
                        gcp_project_id=self.telescope.organisation.gcp_project_id,
                        gcp_bucket_name=self.bucket_name,
                        onix_dataset_id="onix",
                        onix_table_id="onix",
                    )
                ],
                project_id="project",
                orig_dataset_id="google_books",
                orig_table="sales",
                sharded=True,
            )

            self.assertEqual(mock_sel_table_suffixes.call_count, 1)

            wf.create_oaebu_data_qa_google_books_sales_isbn(
                releases=[
                    OnixWorkflowRelease(
                        dag_id="onix_workflow_test",
                        release_date=pendulum.Pendulum(2021, 1, 1),
                        gcp_project_id=self.telescope.organisation.gcp_project_id,
                        gcp_bucket_name=self.bucket_name,
                        onix_dataset_id="onix",
                        onix_table_id="onix",
                    )
                ],
                project_id="project",
                orig_dataset_id="google_books",
                orig_table="sales",
                sharded=False,
            )
            self.assertEqual(mock_sel_table_suffixes.call_count, 1)

            _, call_args = mock_bq_table_query.call_args
            self.assertEqual(call_args["project_id"], "project")
            self.assertEqual(call_args["dataset_id"], "oaebu_data_qa")
            self.assertEqual(call_args["table_id"], "google_books_sales_invalid_isbn20210101")
            self.assertEqual(call_args["location"], "us")

            sql_hash = hashlib.md5(call_args["sql"].encode("utf-8"))
            sql_hash = sql_hash.hexdigest()
            expected_hash = "90bbe5c7fb00173d1a85f6ab13ab99b2"
            self.assertEqual(sql_hash, expected_hash)

    @patch("observatory.dags.workflows.onix_workflow.select_table_shard_dates")
    @patch("observatory.dags.workflows.onix_workflow.create_bigquery_table_from_query")
    @patch("observatory.dags.workflows.onix_workflow.create_bigquery_dataset")
    def test_create_oaebu_data_qa_google_books_traffic_isbn(
        self, mock_bq_ds, mock_bq_table_query, mock_sel_table_suffixes
    ):
        mock_sel_table_suffixes.return_value = [pendulum.Pendulum(2021, 1, 1)]

        data_partners = [
            OaebuPartners(
                name=OaebuPartnerName.google_books_traffic,
                gcp_project_id="project",
                gcp_dataset_id="google_books",
                gcp_table_id="traffic",
                isbn_field_name="Primary_ISBN",
                sharded=False,
            )
        ]
        with CliRunner().isolated_filesystem():
            wf = OnixWorkflow(
                org_name=self.telescope.organisation.name,
                gcp_project_id=self.telescope.organisation.gcp_project_id,
                gcp_bucket_name=self.bucket_name,
                data_partners=data_partners,
            )
            mock_bq_table_query.return_value = True
            wf.create_oaebu_data_qa_google_books_traffic_isbn(
                releases=[
                    OnixWorkflowRelease(
                        dag_id="onix_workflow_test",
                        release_date=pendulum.Pendulum(2021, 1, 1),
                        gcp_project_id=self.telescope.organisation.gcp_project_id,
                        gcp_bucket_name=self.bucket_name,
                        onix_dataset_id="onix",
                        onix_table_id="onix",
                    )
                ],
                project_id="project",
                orig_dataset_id="google_books",
                orig_table="traffic",
                sharded=True,
            )

            self.assertEqual(mock_sel_table_suffixes.call_count, 1)

            wf.create_oaebu_data_qa_google_books_traffic_isbn(
                releases=[
                    OnixWorkflowRelease(
                        dag_id="onix_workflow_test",
                        release_date=pendulum.Pendulum(2021, 1, 1),
                        gcp_project_id=self.telescope.organisation.gcp_project_id,
                        gcp_bucket_name=self.bucket_name,
                        onix_dataset_id="onix",
                        onix_table_id="onix",
                    )
                ],
                project_id="project",
                orig_dataset_id="google_books",
                orig_table="sales",
                sharded=False,
            )
            self.assertEqual(mock_sel_table_suffixes.call_count, 1)

            _, call_args = mock_bq_table_query.call_args
            self.assertEqual(call_args["project_id"], "project")
            self.assertEqual(call_args["dataset_id"], "oaebu_data_qa")
            self.assertEqual(call_args["table_id"], "google_books_traffic_invalid_isbn20210101")
            self.assertEqual(call_args["location"], "us")

            sql_hash = hashlib.md5(call_args["sql"].encode("utf-8"))
            sql_hash = sql_hash.hexdigest()
            expected_hash = "90bbe5c7fb00173d1a85f6ab13ab99b2"
            self.assertEqual(sql_hash, expected_hash)

    @patch("observatory.dags.workflows.onix_workflow.select_table_shard_dates")
    @patch("observatory.dags.workflows.onix_workflow.create_bigquery_table_from_query")
    @patch("observatory.dags.workflows.onix_workflow.create_bigquery_dataset")
    def test_create_oaebu_data_qa_oapen_irus_uk_isbn(self, mock_bq_ds, mock_bq_table_query, mock_sel_table_suffixes):
        mock_sel_table_suffixes.return_value = [pendulum.Pendulum(2021, 1, 1)]

        data_partners = [
            OaebuPartners(
                name=OaebuPartnerName.oapen_irus_uk,
                gcp_project_id="project",
                gcp_dataset_id="irus_uk",
                gcp_table_id="oapen_irus_uk",
                isbn_field_name="ISBN",
                sharded=False,
            )
        ]
        with CliRunner().isolated_filesystem():
            wf = OnixWorkflow(
                org_name=self.telescope.organisation.name,
                gcp_project_id=self.telescope.organisation.gcp_project_id,
                gcp_bucket_name=self.bucket_name,
                data_partners=data_partners,
            )
            mock_bq_table_query.return_value = True
            wf.create_oaebu_data_qa_oapen_irus_uk_isbn(
                releases=[
                    OnixWorkflowRelease(
                        dag_id="onix_workflow_test",
                        release_date=pendulum.Pendulum(2021, 1, 1),
                        gcp_project_id=self.telescope.organisation.gcp_project_id,
                        gcp_bucket_name=self.bucket_name,
                        onix_dataset_id="onix",
                        onix_table_id="onix",
                    )
                ],
                project_id="project",
                orig_dataset_id="irus_uk",
                orig_table="oapen_irus_uk",
                sharded=True,
            )

            self.assertEqual(mock_sel_table_suffixes.call_count, 1)

            wf.create_oaebu_data_qa_oapen_irus_uk_isbn(
                releases=[
                    OnixWorkflowRelease(
                        dag_id="onix_workflow_test",
                        release_date=pendulum.Pendulum(2021, 1, 1),
                        gcp_project_id=self.telescope.organisation.gcp_project_id,
                        gcp_bucket_name=self.bucket_name,
                        onix_dataset_id="onix",
                        onix_table_id="onix",
                    )
                ],
                project_id="project",
                orig_dataset_id="irus_uk",
                orig_table="oapen_irus_uk",
                sharded=False,
            )
            self.assertEqual(mock_sel_table_suffixes.call_count, 1)

            _, call_args = mock_bq_table_query.call_args
            self.assertEqual(call_args["project_id"], "project")
            self.assertEqual(call_args["dataset_id"], "oaebu_data_qa")
            self.assertEqual(call_args["table_id"], "oapen_irus_uk_invalid_isbn20210101")
            self.assertEqual(call_args["location"], "us")

            sql_hash = hashlib.md5(call_args["sql"].encode("utf-8"))
            sql_hash = sql_hash.hexdigest()
            expected_hash = "ae842fbf661d3a0c50b748dec8e1cd24"
            self.assertEqual(sql_hash, expected_hash)

    @patch("observatory.dags.workflows.onix_workflow.select_table_shard_dates")
    @patch("observatory.dags.workflows.onix_workflow.create_bigquery_table_from_query")
    @patch("observatory.dags.workflows.onix_workflow.create_bigquery_dataset")
    def test_create_oaebu_data_qa_intermediate_unmatched_workid(
        self, mock_bq_ds, mock_bq_table_query, mock_sel_table_suffixes
    ):
        mock_sel_table_suffixes.return_value = [pendulum.Pendulum(2021, 1, 1)]

        data_partners = [
            OaebuPartners(
                name=OaebuPartnerName.jstor_country,
                gcp_project_id="project",
                gcp_dataset_id="jstor",
                gcp_table_id="country",
                isbn_field_name="ISBN",
                sharded=False,
            )
        ]
        with CliRunner().isolated_filesystem():
            wf = OnixWorkflow(
                org_name=self.telescope.organisation.name,
                gcp_project_id=self.telescope.organisation.gcp_project_id,
                gcp_bucket_name=self.bucket_name,
                data_partners=data_partners,
            )
            mock_bq_table_query.return_value = True
            wf.create_oaebu_data_qa_intermediate_unmatched_workid(
                releases=[
                    OnixWorkflowRelease(
                        dag_id="onix_workflow_test",
                        release_date=pendulum.Pendulum(2021, 1, 1),
                        gcp_project_id=self.telescope.organisation.gcp_project_id,
                        gcp_bucket_name=self.bucket_name,
                        onix_dataset_id="onix",
                        onix_table_id="onix",
                    )
                ],
                project_id="project",
                orig_dataset_id="jstor",
                orig_table="country",
                orig_isbn="isbn",
            )

            _, call_args = mock_bq_ds.call_args
            self.assertEqual(call_args["project_id"], "project_id")
            self.assertEqual(call_args["dataset_id"], "oaebu_data_qa")

            _, call_args = mock_bq_table_query.call_args
            self.assertEqual(call_args["table_id"], "country_unmatched_isbn20210101")
            self.assertEqual(call_args["location"], "us")

            sql_hash = hashlib.md5(call_args["sql"].encode("utf-8"))
            sql_hash = sql_hash.hexdigest()
            expected_hash = "4ae11853bcbb43ec48d77341ba2a0fa8"
            self.assertEqual(sql_hash, expected_hash)

            mock_bq_table_query.return_value = False
            self.assertRaises(
                AirflowException,
                wf.create_oaebu_data_qa_intermediate_unmatched_workid,
                releases=[
                    OnixWorkflowRelease(
                        dag_id="onix_workflow_test",
                        release_date=pendulum.Pendulum(2021, 1, 1),
                        gcp_project_id=self.telescope.organisation.gcp_project_id,
                        gcp_bucket_name=self.bucket_name,
                        onix_dataset_id="onix",
                        onix_table_id="onix",
                    )
                ],
                project_id="project",
                orig_dataset_id="jstor",
                orig_table="country",
                orig_isbn="isbn",
            )


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
        self.timestamp = pendulum.now()

        self.onix_table_id = "onix"
        self.test_onix_folder = random_id()  # "onix_workflow_test_onix_table"
        self.onix_release_date = pendulum.Pendulum(2021, 1, 1)

        # 2020-01-01
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
            gcp_transform_bucket=env.transform_bucket,
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
        files = [
            os.path.join(test_fixtures_path("telescopes", "onix_workflow"), "jstor_country.json"),
            os.path.join(test_fixtures_path("telescopes", "onix_workflow"), "google_books_sales.json"),
            os.path.join(test_fixtures_path("telescopes", "onix_workflow"), "google_books_traffic.json"),
            os.path.join(test_fixtures_path("telescopes", "onix_workflow"), "oapen_irus_uk.json"),
        ]
        blobs = [os.path.join(self.test_onix_folder, os.path.basename(file)) for file in files]
        upload_files_to_cloud_storage(bucket_name=self.gcp_bucket_name, blob_names=blobs, file_paths=files)

        # Load into bigquery
        table_ids = []
        for file_name, blob in zip(files, blobs):
            table_id, _ = table_ids_from_path(file_name)
            bq_load_partition(
                project_id=self.gcp_project_id,
                transform_bucket=self.gcp_bucket_name,
                transform_blob=blob,
                dataset_id=self.fake_partner_dataset,
                dataset_location=self.data_location,
                table_id=table_id,
                release_date=self.onix_release_date,
                source_format=SourceFormat.NEWLINE_DELIMITED_JSON,
                partition_type=bigquery.table.TimePartitioningType.MONTH,
                dataset_description="Test Onix data for the workflow",
                partition_field="release_date",
                **{},
            )
            table_ids.append(table_id)

        # Make partners
        partners = []
        for (name, isbn_field_name), table_id in zip(
            [
                (OaebuPartnerName.jstor_country, "ISBN"),
                (OaebuPartnerName.google_books_sales, "Primary_ISBN"),
                (OaebuPartnerName.google_books_traffic, "Primary_ISBN"),
                (OaebuPartnerName.oapen_irus_uk, "ISBN"),
            ],
            table_ids,
        ):
            partners.append(
                OaebuPartners(
                    name=name,
                    gcp_project_id=self.gcp_project_id,
                    gcp_dataset_id=self.fake_partner_dataset,
                    gcp_table_id=table_id,
                    isbn_field_name=isbn_field_name,
                    sharded=False,
                )
            )

        return partners

    def test_sensors(self):
        # Setup Observatory environment
        env = ObservatoryEnvironment(self.gcp_project_id, self.data_location)
        with env.create():
            # Set up environment
            self.gcp_bucket_name = env.transform_bucket
            self.setup_observatory_env(env)
            data_partners = self.setup_fake_partner_data(env)

            # Create fake data table. There's no guarantee the data was deleted so clean it again just in case.
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
                onix_dataset_id=self.onix_dataset_id,
                onix_table_id=self.onix_table_id,
                data_partners=data_partners,
            )

            telescope.make_release = MagicMock(
                return_value=[
                    OnixWorkflowRelease(
                        dag_id="onix_workflow_curtin_press",
                        release_date=self.onix_release_date,
                        gcp_project_id=self.gcp_project_id,
                        gcp_bucket_name=self.gcp_bucket_name,
                        onix_dataset_id=self.onix_dataset_id,
                        onix_table_id=self.onix_table_id,
                        oaebu_data_qa_dataset=env.add_dataset(),
                    )
                ]
            )
            workflow_dag = telescope.make_dag()

            expected_state = "up_for_reschedule"
            with env.create_dag_run(workflow_dag, self.timestamp):
                ti = env.run_task("onix_curtin_press_sensor", workflow_dag, self.timestamp)
                self.assertEqual(expected_state, ti.state)

    def test_telescope(self):
        """ Functional test of the ONIX workflow"""

        # Setup Observatory environment
        env = ObservatoryEnvironment(self.gcp_project_id, self.data_location)

        # Create the Observatory environment and run tests
        with env.create():
            self.gcp_bucket_name = env.transform_bucket

            # Set up environment
            self.setup_observatory_env(env)
            data_partners = self.setup_fake_partner_data(env)

            # Create fake data table. There's no guarantee the data was deleted so clean it again just in case.
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
                onix_dataset_id=self.onix_dataset_id,
                onix_table_id=self.onix_table_id,
                data_partners=data_partners,
            )

            oaebu_data_qa_dataset = env.add_dataset()
            onix_workflow_dataset = env.add_dataset()
            oaebu_intermediate_dataset = env.add_dataset()
            telescope.make_release = MagicMock(
                return_value=[
                    OnixWorkflowRelease(
                        dag_id="onix_workflow_curtin_press",
                        release_date=self.onix_release_date,
                        gcp_project_id=self.gcp_project_id,
                        gcp_bucket_name=self.gcp_bucket_name,
                        onix_dataset_id=self.onix_dataset_id,
                        onix_table_id=self.onix_table_id,
                        oaebu_data_qa_dataset=oaebu_data_qa_dataset,
                        workflow_dataset=onix_workflow_dataset,
                        oaebu_intermediate_dataset=oaebu_intermediate_dataset,
                    )
                ]
            )

            # Override sensor for testing
            telescope_sensor = TestOnixWorkflowFunctional.DummySensor(task_id="dummy_sensor", start_date=self.timestamp)
            telescope.sensors = [telescope_sensor]
            workflow_dag = telescope.make_dag()

            # Trigger sensor
            env.run_task(telescope_sensor.task_id, workflow_dag, self.timestamp)

            # Continue workflow
            env.run_task(telescope.continue_workflow.__name__, workflow_dag, self.timestamp)

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
            for data_partner in data_partners:
                oaebu_dataset = data_partner.gcp_dataset_id
                oaebu_table = data_partner.gcp_table_id

                env.run_task(
                    f"{telescope.create_oaebu_intermediate_table.__name__}.{oaebu_dataset}.{oaebu_table}",
                    workflow_dag,
                    self.timestamp,
                )

            # ONIX isbn check
            env.run_task(
                telescope.create_oaebu_data_qa_onix_isbn.__name__,
                workflow_dag,
                self.timestamp,
            )

            # ONIX aggregate metrics
            env.run_task(
                telescope.create_oaebu_data_qa_onix_aggregate.__name__,
                workflow_dag,
                self.timestamp,
            )

            # JSTOR isbn check
            env.run_task(
                f"{telescope.create_oaebu_data_qa_jstor_isbn.__name__}.{data_partners[0].gcp_dataset_id}.{data_partners[0].gcp_table_id}",
                workflow_dag,
                self.timestamp,
            )

            # JSTOR intermediate unmatched isbns
            env.run_task(
                f"{telescope.create_oaebu_data_qa_intermediate_unmatched_workid.__name__}.{data_partners[0].gcp_dataset_id}.{data_partners[0].gcp_table_id}",
                workflow_dag,
                self.timestamp,
            )

            # Google Books Sales isbn check
            env.run_task(
                telescope.create_oaebu_data_qa_google_books_sales_isbn.__name__,
                workflow_dag,
                self.timestamp,
            )

            # Google Books Sales intermediate unmatched isbns
            env.run_task(
                f"{telescope.create_oaebu_data_qa_intermediate_unmatched_workid.__name__}.{data_partners[1].gcp_dataset_id}.{data_partners[1].gcp_table_id}",
                workflow_dag,
                self.timestamp,
            )

            # Google Books Traffic isbn check
            env.run_task(
                telescope.create_oaebu_data_qa_google_books_traffic_isbn.__name__,
                workflow_dag,
                self.timestamp,
            )

            # Google Books Traffic intermediate unmatched isbns
            env.run_task(
                f"{telescope.create_oaebu_data_qa_intermediate_unmatched_workid.__name__}.{data_partners[2].gcp_dataset_id}.{data_partners[2].gcp_table_id}",
                workflow_dag,
                self.timestamp,
            )

            # OAPEN IRUS UK isbn check
            env.run_task(
                telescope.create_oaebu_data_qa_oapen_irus_uk_isbn.__name__,
                workflow_dag,
                self.timestamp,
            )

            # OAPEN IRUS UK intermediate unmatched isbns
            env.run_task(
                f"{telescope.create_oaebu_data_qa_intermediate_unmatched_workid.__name__}.{data_partners[3].gcp_dataset_id}.{data_partners[3].gcp_table_id}",
                workflow_dag,
                self.timestamp,
            )

            # Test conditions
            release_suffix = self.onix_release_date.strftime("%Y%m%d")

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

            table_id = f"{self.gcp_project_id}.{onix_workflow_dataset}.onix_workid_isbn{release_suffix}"

            self.assert_table_integrity(table_id, 3)

            table_id = f"{self.gcp_project_id}.{onix_workflow_dataset}.onix_workfamilyid_isbn{release_suffix}"
            self.assert_table_integrity(table_id, 3)

            table_id = f"{self.gcp_project_id}.{onix_workflow_dataset}.onix_workid_isbn_errors{release_suffix}"
            self.assert_table_integrity(table_id, 1)

            # Validate the joins worked
            # JSTOR
            sql = f"SELECT ISBN, work_id, work_family_id from {self.gcp_project_id}.{oaebu_intermediate_dataset}.{self.fake_partner_dataset}_jstor_country_matched{release_suffix}"
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

            # OAPEN IRUS UK
            sql = f"SELECT ISBN, work_id, work_family_id from {self.gcp_project_id}.{oaebu_intermediate_dataset}.{self.fake_partner_dataset}_oapen_irus_uk_matched{release_suffix}"
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

            # Check invalid ISBN13s picked up in ONIX
            sql = f"SELECT ISBN13 from {self.gcp_project_id}.{oaebu_data_qa_dataset}.onix_invalid_isbn{release_suffix}"
            records = run_bigquery_query(sql)
            isbns = set([record["ISBN13"] for record in records])
            self.assertEqual(len(isbns), 3)
            self.assertTrue("211" in isbns)
            self.assertTrue("112" in isbns)
            self.assertTrue("111" in isbns)

            # Check ONIX aggregate metrics are correct
            sql = f"SELECT * from {self.gcp_project_id}.{oaebu_data_qa_dataset}.onix_aggregate_metrics{release_suffix}"
            records = run_bigquery_query(sql)
            self.assertEqual(len(records), 1)
            self.assertEqual(records[0]["table_size"], 3)
            self.assertEqual(records[0]["no_isbns"], 0)
            self.assertEqual(records[0]["no_relatedworks"], 0)
            self.assertEqual(records[0]["no_relatedproducts"], 2)
            self.assertEqual(records[0]["no_doi"], 3)
            self.assertEqual(records[0]["no_productform"], 3)
            self.assertEqual(records[0]["no_contributors"], 3)
            self.assertEqual(records[0]["no_titledetails"], 3)
            self.assertEqual(records[0]["no_publisher_urls"], 3)

            # Check JSTOR ISBN are valid
            sql = f"SELECT * from {self.gcp_project_id}.{oaebu_data_qa_dataset}.jstor_invalid_isbn{release_suffix}"
            records = run_bigquery_query(sql)
            isbns = set([record["ISBN"] for record in records])
            self.assertEqual(len(isbns), 4)
            self.assertTrue("111" in isbns)
            self.assertTrue("211" in isbns)
            self.assertTrue("113" in isbns)
            self.assertTrue("112" in isbns)

            # Check JSTOR eISBN are valid
            sql = f"SELECT * from {self.gcp_project_id}.{oaebu_data_qa_dataset}.jstor_invalid_eisbn{release_suffix}"
            records = run_bigquery_query(sql)
            isbns = set([record["eISBN"] for record in records])
            self.assertEqual(len(isbns), 4)
            self.assertTrue("111" in isbns)
            self.assertTrue("113" in isbns)
            self.assertTrue("112" in isbns)
            self.assertTrue(None in isbns)

            # Check JSTOR unmatched ISBN picked up
            sql = f"SELECT ISBN from {self.gcp_project_id}.{oaebu_data_qa_dataset}.jstor_country_unmatched_ISBN{release_suffix}"
            records = run_bigquery_query(sql)
            isbns = set([record["ISBN"] for record in records])
            self.assertEqual(len(isbns), 2)
            self.assertTrue("9781111111113" in isbns)
            self.assertTrue("113" in isbns)

            # Check OAPEN IRUS UK ISBN are valid
            sql = f"SELECT * from {self.gcp_project_id}.{oaebu_data_qa_dataset}.oapen_irus_uk_invalid_isbn{release_suffix}"
            records = run_bigquery_query(sql)
            isbns = set([record["ISBN"] for record in records])
            self.assertEqual(len(isbns), 4)
            self.assertTrue("111" in isbns)
            self.assertTrue("113" in isbns)
            self.assertTrue("112" in isbns)
            self.assertTrue("211" in isbns)

            # Check OAPEN IRUS UK unmatched ISBN picked up
            sql = f"SELECT ISBN from {self.gcp_project_id}.{oaebu_data_qa_dataset}.oapen_irus_uk_unmatched_ISBN{release_suffix}"
            records = run_bigquery_query(sql)
            isbns = set([record["ISBN"] for record in records])
            self.assertEqual(len(isbns), 2)
            self.assertTrue("9781111111113" in isbns)
            self.assertTrue("113" in isbns)

            # Check Google Books Sales ISBN are valid
            sql = f"SELECT * from {self.gcp_project_id}.{oaebu_data_qa_dataset}.google_books_sales_invalid_isbn{release_suffix}"
            records = run_bigquery_query(sql)
            isbns = set([record["Primary_ISBN"] for record in records])
            self.assertEqual(len(isbns), 4)
            self.assertTrue("111" in isbns)
            self.assertTrue("211" in isbns)
            self.assertTrue("113" in isbns)
            self.assertTrue("112" in isbns)

            # Check Google Books Sales unmatched ISBN picked up
            sql = f"SELECT Primary_ISBN from {self.gcp_project_id}.{oaebu_data_qa_dataset}.google_books_sales_unmatched_Primary_ISBN{release_suffix}"
            records = run_bigquery_query(sql)
            isbns = set([record["Primary_ISBN"] for record in records])
            self.assertEqual(len(isbns), 2)
            self.assertTrue("9781111111113" in isbns)
            self.assertTrue("113" in isbns)

            # Check Google Books Traffic ISBN are valid
            sql = f"SELECT * from {self.gcp_project_id}.{oaebu_data_qa_dataset}.google_books_traffic_invalid_isbn{release_suffix}"
            records = run_bigquery_query(sql)
            isbns = set([record["Primary_ISBN"] for record in records])
            self.assertEqual(len(isbns), 4)
            self.assertTrue("111" in isbns)
            self.assertTrue("211" in isbns)
            self.assertTrue("113" in isbns)
            self.assertTrue("112" in isbns)

            # Check Google Books Traffic unmatched ISBN picked up
            sql = f"SELECT Primary_ISBN from {self.gcp_project_id}.{oaebu_data_qa_dataset}.google_books_traffic_unmatched_Primary_ISBN{release_suffix}"
            records = run_bigquery_query(sql)
            isbns = set([record["Primary_ISBN"] for record in records])
            self.assertEqual(len(isbns), 2)
            self.assertTrue("9781111111113" in isbns)
            self.assertTrue("113" in isbns)

            # Cleanup
            env.run_task(telescope.cleanup.__name__, workflow_dag, self.timestamp)

            # Environment cleanup
            delete_bigquery_dataset(project_id=self.gcp_project_id, dataset_id=oaebu_data_qa_dataset)
            delete_bigquery_dataset(project_id=self.gcp_project_id, dataset_id="oaebu_intermediate")
            delete_bigquery_dataset(project_id=self.gcp_project_id, dataset_id="onix_workflow")
            delete_bucket_dir(bucket_name=self.gcp_bucket_name, prefix=self.test_onix_folder)
