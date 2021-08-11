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

# Author: Aniek Roelofs

import gzip
import json
import os
from datetime import datetime, timedelta
from unittest.mock import patch

import pendulum
from airflow.models.connection import Connection
from croniter import croniter
from googleapiclient.discovery import build
from googleapiclient.http import HttpMockSequence
from observatory.api.client.identifiers import TelescopeTypes
from observatory.api.client.model.organisation import Organisation
from observatory.api.server import orm
from observatory.dags.telescopes.google_analytics import (
    GoogleAnalyticsRelease,
    GoogleAnalyticsTelescope,
)
from observatory.platform.utils.airflow_utils import AirflowConns
from observatory.platform.utils.template_utils import blob_name, table_ids_from_path
from observatory.platform.utils.test_utils import (
    ObservatoryEnvironment,
    ObservatoryTestCase,
    module_file_path,
)


class TestGoogleAnalytics(ObservatoryTestCase):
    """Tests for the Google Analytics telescope"""

    def __init__(self, *args, **kwargs):
        """Constructor which sets up variables used by tests.
        :param args: arguments.
        :param kwargs: keyword arguments.
        """
        super(TestGoogleAnalytics, self).__init__(*args, **kwargs)
        self.project_id = os.getenv("TEST_GCP_PROJECT_ID")
        self.data_location = os.getenv("TEST_GCP_DATA_LOCATION")
        self.host = "localhost"
        self.api_port = 5000

    def test_dag_structure(self):
        """Test that the Google Analytics DAG has the correct structure.
        :return: None
        """
        organisation = Organisation(name="Organisation Name")
        dag = GoogleAnalyticsTelescope(organisation, "1234", r"regex").make_dag()
        self.assert_dag_structure(
            {
                "check_dependencies": ["download_transform"],
                "download_transform": ["upload_transformed"],
                "upload_transformed": ["bq_load_partition"],
                "bq_load_partition": ["cleanup"],
                "cleanup": [],
            },
            dag,
        )

    def test_dag_load(self):
        """Test that the Google Analytics DAG can be loaded from a DAG bag.
        :return: None
        """

        env = ObservatoryEnvironment(self.project_id, self.data_location)
        with env.create():
            # Add Observatory API connection
            conn = Connection(conn_id=AirflowConns.OBSERVATORY_API, uri=f"http://:password@{self.host}:{self.api_port}")
            env.add_connection(conn)

            # Add a telescope
            dt = pendulum.now("UTC")
            telescope_type = orm.TelescopeType(
                name="Google Analytics Telescope", type_id=TelescopeTypes.google_analytics, created=dt, modified=dt
            )
            env.api_session.add(telescope_type)
            organisation = orm.Organisation(name="UCL Press", created=dt, modified=dt)
            env.api_session.add(organisation)
            telescope = orm.Telescope(
                name="UCL Press Google Analytics Telescope",
                telescope_type=telescope_type,
                organisation=organisation,
                modified=dt,
                created=dt,
                extra={"view_id": "11235141", "pagepath_regex": r".*regex$"},
            )
            env.api_session.add(telescope)
            env.api_session.commit()

            dag_file = os.path.join(module_file_path("observatory.dags.dags"), "google_analytics.py")
            self.assert_dag_load("google_analytics_ucl_press", dag_file)

    @patch("observatory.dags.telescopes.google_analytics.build")
    @patch("observatory.dags.telescopes.google_analytics.ServiceAccountCredentials")
    def test_telescope(self, mock_account_credentials, mock_build):
        """Test the Google Analytics telescope end to end.
        :return: None.
        """
        # Set up organisation name and telescope extra values
        self.organisation_name = "UCL Press"
        self.extra = {"view_id": "11235141", "pagepath_regex": r".*regex$"}
        self.view_id = self.extra.get("view_id")
        self.pagepath_regex = self.extra.get("pagepath_regex")

        # Mock the Google Reporting Analytics API service
        mock_account_credentials.from_json_keyfile_dict.return_value = ""

        http = HttpMockSequence(create_http_mock_sequence(self.organisation_name))
        mock_build.return_value = build("analyticsreporting", "v4", http=http)

        # Setup Observatory environment
        env = ObservatoryEnvironment(self.project_id, self.data_location)
        dataset_id = env.add_dataset()

        # Setup Telescope
        execution_date = pendulum.datetime(year=2020, month=4, day=1)
        organisation = Organisation(
            name=self.organisation_name,
            gcp_project_id=self.project_id,
            gcp_download_bucket=env.download_bucket,
            gcp_transform_bucket=env.transform_bucket,
        )
        telescope = GoogleAnalyticsTelescope(
            organisation=organisation, view_id=self.view_id, pagepath_regex=self.pagepath_regex, dataset_id=dataset_id
        )
        dag = telescope.make_dag()

        # Create the Observatory environment and run tests
        with env.create():
            with env.create_dag_run(dag, execution_date):
                # Add OAEBU service account connection connection
                conn = Connection(
                    conn_id=AirflowConns.OAEBU_SERVICE_ACCOUNT,
                    uri=f"google-cloud-platform://?type=service_account&private_key_id=private_key_id"
                    f"&private_key=private_key"
                    f"&client_email=client_email"
                    f"&client_id=client_id",
                )
                env.add_connection(conn)

                # Test that all dependencies are specified: no error should be thrown
                env.run_task(telescope.check_dependencies.__name__, dag, execution_date)

                # Use release to check tasks
                cron_schedule = dag.normalized_schedule_interval
                cron_iter = croniter(cron_schedule, execution_date)
                end_date = pendulum.instance(cron_iter.get_next(datetime)) - timedelta(days=1)
                release = GoogleAnalyticsRelease(telescope.dag_id, execution_date, end_date, organisation)

                # Test download_transform task
                env.run_task(telescope.download_transform.__name__, dag, execution_date)
                self.assertEqual(1, len(release.transform_files))
                for file in release.transform_files:
                    self.assertTrue(os.path.isfile(file))
                    # Use frozenset to test results are as expected, many dict transformations re-order items in dict
                    actual_list = []
                    with gzip.open(file, "rb") as f:
                        for line in f:
                            actual_list.append(json.loads(line))
                    expected_list = [
                        {
                            "url": "/base/path/151420",
                            "title": "Anything public program drive north.",
                            "start_date": "2020-04-01",
                            "end_date": "2020-04-30",
                            "release_date": "2020-04-30",
                            "average_time": 59.5,
                            "unique_views": {
                                "country": [{"name": "country 1", "value": 3}, {"name": "country 2", "value": 3}],
                                "referrer": [{"name": "referrer 1", "value": 3}, {"name": "referrer 2", "value": 3}],
                                "social_network": [
                                    {"name": "social_network 1", "value": 3},
                                    {"name": "social_network 2", "value": 3},
                                ],
                            },
                            "sessions": {
                                "country": [{"name": "country 1", "value": 1}, {"name": "country 2", "value": 1}],
                                "source": [{"name": "source 1", "value": 1}, {"name": "source 2", "value": 1}],
                            },
                        },
                        {
                            "url": "/base/path/833557",
                            "title": "Standard current never no.",
                            "start_date": "2020-04-01",
                            "end_date": "2020-04-30",
                            "release_date": "2020-04-30",
                            "average_time": 44.2,
                            "unique_views": {
                                "country": [{"name": "country 2", "value": 2}, {"name": "country 1", "value": 1}],
                                "referrer": [{"name": "referrer 1", "value": 1}, {"name": "referrer 2", "value": 2}],
                                "social_network": [
                                    {"name": "social_network 2", "value": 2},
                                    {"name": "social_network 1", "value": 1},
                                ],
                            },
                            "sessions": {"country": [], "source": []},
                        },
                        {
                            "url": "/base/path/833557?fbclid=123",
                            "title": "Standard current never no.",
                            "start_date": "2020-04-01",
                            "end_date": "2020-04-30",
                            "average_time": 38.8,
                            "unique_views": {
                                "country": [{"name": "country 2", "value": 2}],
                                "referrer": [{"name": "referrer 2", "value": 2}],
                                "social_network": [{"name": "social_network 2", "value": 2}],
                            },
                            "sessions": {"country": [], "source": []},
                            "release_date": "2020-04-30",
                        },
                    ]
                    self.assertEqual(3, len(actual_list))
                    self.assertEqual(frozenset(expected_list[0]), frozenset(actual_list[0]))
                    self.assertEqual(frozenset(expected_list[1]), frozenset(actual_list[1]))
                    self.assertEqual(frozenset(expected_list[2]), frozenset(actual_list[2]))

                # Test that transformed file uploaded
                env.run_task(telescope.upload_transformed.__name__, dag, execution_date)
                for file in release.transform_files:
                    self.assert_blob_integrity(env.transform_bucket, blob_name(file), file)

                # Test that data loaded into BigQuery
                env.run_task(telescope.bq_load_partition.__name__, dag, execution_date)
                for file in release.transform_files:
                    table_id, _ = table_ids_from_path(file)
                    table_id = f'{self.project_id}.{dataset_id}.{table_id}${release.release_date.strftime("%Y%m")}'
                    expected_rows = 3
                    self.assert_table_integrity(table_id, expected_rows)

                # Test that all telescope data deleted
                download_folder, extract_folder, transform_folder = (
                    release.download_folder,
                    release.extract_folder,
                    release.transform_folder,
                )
                env.run_task(telescope.cleanup.__name__, dag, execution_date)
                self.assert_cleanup(download_folder, extract_folder, transform_folder)

    @patch("observatory.dags.telescopes.google_analytics.build")
    @patch("observatory.dags.telescopes.google_analytics.ServiceAccountCredentials")
    def test_telescope_anu(self, mock_account_credentials, mock_build):
        """Test the Google Analytics telescope end to end specifically for ANU Press, to test custom dimensions.
        :return: None.
        """
        # Set up organisation name and telescope extra values
        self.organisation_name = GoogleAnalyticsTelescope.ANU_ORG_NAME
        self.extra = {"view_id": "12354151", "pagepath_regex": ""}
        self.view_id = self.extra.get("view_id")
        self.pagepath_regex = self.extra.get("pagepath_regex")

        # Mock the Google Reporting Analytics API service
        mock_account_credentials.from_json_keyfile_dict.return_value = ""

        http = HttpMockSequence(create_http_mock_sequence(self.organisation_name))
        mock_build.return_value = build("analyticsreporting", "v4", http=http)

        # Setup Observatory environment
        env = ObservatoryEnvironment(self.project_id, self.data_location)
        dataset_id = env.add_dataset()

        # Setup Telescope
        execution_date = pendulum.datetime(year=2020, month=4, day=1)
        organisation = Organisation(
            name=self.organisation_name,
            gcp_project_id=self.project_id,
            gcp_download_bucket=env.download_bucket,
            gcp_transform_bucket=env.transform_bucket,
        )
        telescope = GoogleAnalyticsTelescope(
            organisation=organisation, view_id=self.view_id, pagepath_regex=self.pagepath_regex, dataset_id=dataset_id
        )
        dag = telescope.make_dag()

        # Create the Observatory environment and run tests
        with env.create():
            with env.create_dag_run(dag, execution_date):
                # Add OAEBU service account connection connection
                conn = Connection(
                    conn_id=AirflowConns.OAEBU_SERVICE_ACCOUNT,
                    uri=f"google-cloud-platform://?type=service_account&private_key_id=private_key_id"
                    f"&private_key=private_key"
                    f"&client_email=client_email"
                    f"&client_id=client_id",
                )
                env.add_connection(conn)

                # Test that all dependencies are specified: no error should be thrown
                env.run_task(telescope.check_dependencies.__name__, dag, execution_date)

                # Use release to check tasks
                cron_schedule = dag.normalized_schedule_interval
                cron_iter = croniter(cron_schedule, execution_date)
                end_date = pendulum.instance(cron_iter.get_next(datetime)) - timedelta(days=1)
                release = GoogleAnalyticsRelease(telescope.dag_id, execution_date, end_date, organisation)

                # Test download_transform task
                env.run_task(telescope.download_transform.__name__, dag, execution_date)
                self.assertEqual(1, len(release.transform_files))
                for file in release.transform_files:
                    self.assertTrue(os.path.isfile(file))
                    # Use frozenset to test results are as expected, many dict transformations re-order items in dict
                    actual_list = []
                    with gzip.open(file, "rb") as f:
                        for line in f:
                            actual_list.append(json.loads(line))
                    expected_list = [
                        {
                            "url": "/base/path/151420",
                            "title": "Anything public program drive north.",
                            "start_date": "2020-04-01",
                            "end_date": "2020-04-30",
                            "average_time": 59.5,
                            "unique_views": {
                                "country": [{"name": "country 1", "value": 3}, {"name": "country 2", "value": 3}],
                                "referrer": [{"name": "referrer 1", "value": 3}, {"name": "referrer 2", "value": 3}],
                                "social_network": [
                                    {"name": "social_network 1", "value": 3},
                                    {"name": "social_network 2", "value": 3},
                                ],
                            },
                            "sessions": {
                                "country": [{"name": "country 1", "value": 1}, {"name": "country 2", "value": 1}],
                                "source": [{"name": "source 1", "value": 1}, {"name": "source 2", "value": 1}],
                            },
                            "publication_id": "1234567890123",
                            "publication_type": "book",
                            "publication_imprint": "imprint",
                            "publication_group": "group",
                            "publication_whole_or_part": "whole",
                            "publication_format": "PDF",
                            "release_date": "2020-04-30",
                        },
                        {
                            "url": "/base/path/833557",
                            "title": "Standard current never no.",
                            "start_date": "2020-04-01",
                            "end_date": "2020-04-30",
                            "average_time": 44.2,
                            "unique_views": {
                                "country": [{"name": "country 2", "value": 2}, {"name": "country 1", "value": 1}],
                                "referrer": [{"name": "referrer 1", "value": 1}, {"name": "referrer 2", "value": 2}],
                                "social_network": [
                                    {"name": "social_network 2", "value": 2},
                                    {"name": "social_network 1", "value": 1},
                                ],
                            },
                            "sessions": {"country": [], "source": []},
                            "publication_id": "1234567891234",
                            "publication_type": "book",
                            "publication_imprint": "imprint",
                            "publication_group": "(none)",
                            "publication_whole_or_part": "part",
                            "publication_format": "HTML",
                            "release_date": "2020-04-30",
                        },
                        {
                            "url": "/base/path/833557?fbclid=123",
                            "title": "Standard current never no.",
                            "start_date": "2020-04-01",
                            "end_date": "2020-04-30",
                            "average_time": 38.8,
                            "unique_views": {
                                "country": [{"name": "country 2", "value": 2}],
                                "referrer": [{"name": "referrer 2", "value": 2}],
                                "social_network": [{"name": "social_network 2", "value": 2}],
                            },
                            "sessions": {"country": [], "source": []},
                            "publication_id": "1234567891234",
                            "publication_type": "book",
                            "publication_imprint": "imprint",
                            "publication_group": "(none)",
                            "publication_whole_or_part": "part",
                            "publication_format": "HTML",
                            "release_date": "2020-04-30",
                        },
                    ]
                    self.assertEqual(3, len(actual_list))
                    self.assertEqual(frozenset(expected_list[0]), frozenset(actual_list[0]))
                    self.assertEqual(frozenset(expected_list[1]), frozenset(actual_list[1]))
                    self.assertEqual(frozenset(expected_list[2]), frozenset(actual_list[2]))

                # Test that transformed file uploaded
                env.run_task(telescope.upload_transformed.__name__, dag, execution_date)
                for file in release.transform_files:
                    self.assert_blob_integrity(env.transform_bucket, blob_name(file), file)

                # Test that data loaded into BigQuery
                env.run_task(telescope.bq_load_partition.__name__, dag, execution_date)
                for file in release.transform_files:
                    table_id, _ = table_ids_from_path(file)
                    table_id = f'{self.project_id}.{dataset_id}.{table_id}${release.release_date.strftime("%Y%m")}'
                    expected_rows = 3
                    self.assert_table_integrity(table_id, expected_rows)

                # Test that all telescope data deleted
                download_folder, extract_folder, transform_folder = (
                    release.download_folder,
                    release.extract_folder,
                    release.transform_folder,
                )
                env.run_task(telescope.cleanup.__name__, dag, execution_date)
                self.assert_cleanup(download_folder, extract_folder, transform_folder)


def create_http_mock_sequence(organisation_name: str) -> list:
    """Create a list of http mock sequences for listing books and getting dimension data

    :param organisation_name: The organisation name (add custom dimensions for ANU)
    :return: A list with HttpMockSequence instances
    """
    http_mock_sequence = []
    list_books = {
        "reports": [
            {
                "columnHeader": {
                    "dimensions": ["ga:pagepath", "ga:pageTitle"],
                    "metricHeader": {"metricHeaderEntries": [{"name": "ga:avgTimeOnPage", "type": "TIME"}]},
                },
                "data": {
                    "rows": [
                        {
                            "dimensions": ["/base/path/151420", "Anything public program drive north."],
                            "metrics": [{"values": ["59.5"]}],
                        },
                        {
                            "dimensions": ["/base/path/833557", "Standard current never no."],
                            "metrics": [{"values": ["49.6"]}],
                        },
                    ],
                    "totals": [{"values": ["109.1"]}],
                    "rowCount": 2,
                    "minimums": [{"values": ["49.6"]}],
                    "maximums": [{"values": ["59.5"]}],
                    "isDataGolden": True,
                },
                "nextPageToken": "200",
            }
        ]
    }
    # Add custom dimensions from ANU Press
    if organisation_name == GoogleAnalyticsTelescope.ANU_ORG_NAME:
        list_books["reports"][0]["columnHeader"]["dimensions"] += [f"ga:dimension{(str(i))}" for i in range(1, 7)]
        list_books["reports"][0]["data"]["rows"][0]["dimensions"] += [
            "1234567890123",
            "book",
            "imprint",
            "group",
            "whole",
            "PDF",
        ]
        list_books["reports"][0]["data"]["rows"][1]["dimensions"] += [
            "1234567891234",
            "book",
            "imprint",
            "(none)",
            "part",
            "HTML",
        ]
    list_books_next_page = {
        "reports": [
            {
                "columnHeader": {
                    "dimensions": ["ga:pagepath", "ga:pageTitle"],
                    "metricHeader": {"metricHeaderEntries": [{"name": "ga:avgTimeOnPage", "type": "TIME"}]},
                },
                "data": {
                    "rows": [
                        {
                            "dimensions": ["/base/path/833557?fbclid=123", "Standard current never no."],
                            "metrics": [{"values": ["38.8"]}],
                        }
                    ],
                    "totals": [{"values": ["38.8"]}],
                    "rowCount": 1,
                    "minimums": [{"values": ["38.8"]}],
                    "maximums": [{"values": ["38.8"]}],
                    "isDataGolden": True,
                },
            }
        ]
    }
    # Add custom dimensions from ANU Press
    if organisation_name == GoogleAnalyticsTelescope.ANU_ORG_NAME:
        list_books_next_page["reports"][0]["columnHeader"]["dimensions"] += [
            f"ga:dimension{(str(i))}" for i in range(1, 7)
        ]
        list_books_next_page["reports"][0]["data"]["rows"][0]["dimensions"] += [
            "1234567891234",
            "book",
            "imprint",
            "(none)",
            "part",
            "HTML",
        ]
    http_mock_sequence.append(({"status": "200"}, json.dumps(list_books)))
    http_mock_sequence.append(({"status": "200"}, json.dumps(list_books_next_page)))
    for dimension in ["country", "referrer", "social_network", "source"]:
        results = {
            "reports": [
                {
                    "columnHeader": {
                        "dimensions": ["ga:pagePath", "ga:country"],
                        "metricHeader": {
                            "metricHeaderEntries": [
                                {"name": "ga:uniquePageviews", "type": "INTEGER"},
                                {"name": "ga:sessions", "type": "INTEGER"},
                            ]
                        },
                    },
                    "data": {
                        "rows": [
                            {
                                "dimensions": ["/base/path/151420", dimension + " 1"],
                                "metrics": [{"values": ["3", "1"]}],
                            },
                            {
                                "dimensions": ["/base/path/151420", dimension + " 2"],
                                "metrics": [{"values": ["3", "1"]}],
                            },
                            {
                                "dimensions": ["/base/path/833557", dimension + " 1"],
                                "metrics": [{"values": ["1", "0"]}],
                            },
                            {
                                "dimensions": ["/base/path/833557?fbclid=123", dimension + " 2"],
                                "metrics": [{"values": ["2", "0"]}],
                            },
                        ],
                        "totals": [{"values": ["6", "1"]}],
                        "rowCount": 3,
                        "minimums": [{"values": ["1", "0"]}],
                        "maximums": [{"values": ["3", "1"]}],
                        "isDataGolden": True,
                    },
                }
            ]
        }
        http_mock_sequence.append(({"status": "200"}, json.dumps(results)))

    return http_mock_sequence
