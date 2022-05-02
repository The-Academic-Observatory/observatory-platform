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


from observatory.platform.utils.test_utils import ObservatoryTestCase, ObservatoryEnvironment
from observatory.api.testing import ObservatoryApiEnvironment
from observatory.api.client import ApiClient, Configuration
from observatory.api.client.api.observatory_api import ObservatoryApi  # noqa: E501
from observatory.api.client.model.organisation import Organisation
from observatory.api.client.model.table_type import TableType
from observatory.api.client.model.dataset_type import DatasetType
from observatory.api.client.model.workflow import Workflow
from observatory.api.client.model.workflow_type import WorkflowType
from observatory.api.client.model.dataset import Dataset
from observatory.api.client.model.dataset_release import DatasetRelease
from observatory.platform.utils.release_utils import (
    get_datasets,
    get_dataset_releases,
    get_start_end_date,
    get_start_end_date_key,
    get_new_release_dates,
    get_new_start_end_release_dates,
    address_to_gcp_fields,
    get_latest_dataset_release,
    is_first_release,
    add_dataset_release,
)
from unittest.mock import patch, Mock
import pendulum
from observatory.platform.utils.test_utils import test_fixtures_path
from observatory.platform.workflows.workflow import Release
from observatory.platform.workflows.stream_telescope import StreamRelease
from observatory.platform.workflows.snapshot_telescope import SnapshotRelease


class TestReleaseUtils(ObservatoryTestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def setUp(self):
        self.timezone = "Pacific/Auckland"
        self.host = "localhost"
        self.port = 5001
        configuration = Configuration(host=f"http://{self.host}:{self.port}")
        api_client = ApiClient(configuration)
        self.api = ObservatoryApi(api_client=api_client)  # noqa: E501
        self.env = ObservatoryApiEnvironment(host=self.host, port=self.port)
        self.org_name = "Curtin University"
        self.workflow_id = 1

    def setup_api_entries(self):
        org = Organisation(name=self.org_name)
        result = self.api.put_organisation(org)
        self.assertIsInstance(result, Organisation)

        tele_type = WorkflowType(type_id="tele_type", name="My Workflow")
        result = self.api.put_workflow_type(tele_type)
        self.assertIsInstance(result, WorkflowType)

        workflow = Workflow(organisation=Organisation(id=1), workflow_type=WorkflowType(id=1))
        result = self.api.put_workflow(workflow)
        self.assertIsInstance(result, Workflow)

        table_type = TableType(
            type_id="partitioned",
            name="partitioned bq table",
        )
        self.api.put_table_type(table_type)

        dataset_type = DatasetType(
            name="My dataset type",
            type_id="type id",
            table_type=TableType(id=1),
        )

        self.api.put_dataset_type(dataset_type)

        dataset = Dataset(
            name="My dataset",
            service="bigquery",
            address="project.dataset.table",
            workflow=Workflow(id=1),
            dataset_type=DatasetType(id=1),
        )
        result = self.api.put_dataset(dataset)
        self.assertIsInstance(result, Dataset)

    @patch("observatory.platform.utils.release_utils.make_observatory_api")
    def test_get_datasets(self, m_makeapi):
        m_makeapi.return_value = self.api

        with self.env.create():
            self.setup_api_entries()
            datasets = get_datasets(workflow_id=self.workflow_id)
            self.assertEqual(len(datasets), 1)
            self.assertEqual(datasets[0].name, "My dataset")

    @patch("observatory.platform.utils.release_utils.make_observatory_api")
    def test_get_dataset_releases(self, m_makeapi):
        m_makeapi.return_value = self.api

        with self.env.create():
            self.setup_api_entries()

            dt = pendulum.now("UTC")
            dataset_release = DatasetRelease(
                dataset=Dataset(id=1),
                start_date=dt,
                end_date=dt,
            )
            result = self.api.put_dataset_release(dataset_release)
            self.assertIsInstance(result, DatasetRelease)

            dataset_releases = get_dataset_releases(dataset_id=1)
            self.assertEqual(len(dataset_releases), 1)
            self.assertEqual(dataset_releases[0].start_date, dt)

    @patch("observatory.platform.utils.release_utils.make_observatory_api")
    def test_is_first_release(self, m_makeapi):
        m_makeapi.return_value = self.api

        with self.env.create():
            self.setup_api_entries()

            self.assertTrue(is_first_release(1))

            dt = pendulum.now("UTC")
            release = DatasetRelease(
                dataset=Dataset(id=1),
                start_date=dt,
                end_date=dt,
            )
            self.api.put_dataset_release(release)

            self.assertFalse(is_first_release(1))

    def test_get_start_end_date(self):
        # Non date release_id
        release = Release("dag_id", "release_id")
        self.assertRaises(Exception, get_start_end_date, release)

        # Dated release_id
        release_id = pendulum.datetime(2021, 1, 1)
        release = Release("dag_id", "20210101")
        start, end = get_start_end_date(release)
        self.assertEqual(start, release_id)
        self.assertEqual(end, release_id)

        # Release date
        release_date = pendulum.datetime(2021, 1, 1)
        release = SnapshotRelease("dag_id", release_date)
        start, end = get_start_end_date(release)
        self.assertEqual(start, release_date)
        self.assertEqual(end, release_date)

        # Start and end date
        start_date = pendulum.datetime(2021, 1, 1)
        end_date = pendulum.datetime(2022, 1, 1)
        first_release = False
        release = StreamRelease("dag_id", start_date, end_date, first_release)
        start, end = get_start_end_date(release)
        self.assertEqual(start, start_date)
        self.assertEqual(end, end_date)

    @patch("observatory.platform.utils.release_utils.make_observatory_api")
    def test_get_new_release_dates(self, m_makeapi):
        m_makeapi.return_value = self.api

        # Setup API entries
        with self.env.create():
            self.setup_api_entries()
            release_date = pendulum.datetime(2021, 1, 1)
            self.api.put_dataset_release(
                DatasetRelease(
                    dataset=Dataset(id=1),
                    start_date=release_date,
                    end_date=release_date,
                )
            )

            releases = [pendulum.datetime(2021, 1, 1), pendulum.datetime(2021, 2, 1)]
            new_releases = get_new_release_dates(dataset_id=1, releases=releases)
            self.assertEqual(new_releases, ["20210201"])

    def test_get_start_end_date_key(self):
        key = get_start_end_date_key((pendulum.datetime(2021, 1, 1), pendulum.datetime(2022, 12, 31)))
        self.assertEqual(key, "2021010120221231")

    @patch("observatory.platform.utils.release_utils.make_observatory_api")
    def test_get_new_start_end_release_dates(self, m_makeapi):
        m_makeapi.return_value = self.api

        # Setup API entries
        with self.env.create():
            self.setup_api_entries()
            dt = pendulum.datetime(2021, 1, 1)
            self.api.put_dataset_release(
                DatasetRelease(
                    dataset=Dataset(id=1),
                    start_date=pendulum.datetime(2021, 1, 1),
                    end_date=pendulum.datetime(2021, 1, 31),
                )
            )

            releases = [
                (pendulum.datetime(2021, 1, 1), pendulum.datetime(2021, 1, 31)),
                (pendulum.datetime(2021, 3, 1), pendulum.datetime(2021, 3, 31)),
            ]
            new_releases = get_new_start_end_release_dates(dataset_id=1, releases=releases)
            self.assertEqual(new_releases, [("20210301", "20210331")])

    def test_address_to_gcp_fields(self):
        address = "project.dataset.table"
        project, dataset, table = address_to_gcp_fields(address)
        self.assertEqual(project, "project")
        self.assertEqual(dataset, "dataset")
        self.assertEqual(table, "table")

    @patch("observatory.platform.utils.release_utils.make_observatory_api")
    def test_add_dataset_release(self, m_makeapi):
        m_makeapi.return_value = self.api

        # Setup API entries
        with self.env.create():
            self.setup_api_entries()

            start_date = pendulum.datetime(2021, 1, 1)
            end_date = pendulum.datetime(2021, 1, 31)
            add_dataset_release(start_date=start_date, end_date=end_date, dataset_id=1)
            releases = self.api.get_dataset_releases(limit=1000)
            self.assertEqual(len(releases), 1)
            self.assertEqual(pendulum.instance(releases[0].start_date), pendulum.datetime(2021, 1, 1))

    @patch("observatory.platform.utils.release_utils.make_observatory_api")
    def test_get_latest_dataset_release(self, m_makeapi):
        m_makeapi.return_value = self.api

        # Setup API entries
        with self.env.create():
            self.setup_api_entries()

            start_date = pendulum.datetime(2021, 1, 1)
            end_date = pendulum.datetime(2021, 1, 31)
            add_dataset_release(start_date=start_date, end_date=end_date, dataset_id=1)

            start_date = pendulum.datetime(2021, 3, 1)
            end_date = pendulum.datetime(2021, 3, 31)
            add_dataset_release(start_date=start_date, end_date=end_date, dataset_id=1)

            releases = self.api.get_dataset_releases(limit=1000)
            self.assertEqual(len(releases), 2)

            latest = get_latest_dataset_release(releases)
            self.assertEqual(latest.end_date, end_date)
