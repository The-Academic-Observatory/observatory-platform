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
#
#
# Author: Tuan Chien


from collections import OrderedDict
import os

from observatory.api.client import ApiClient, Configuration
from observatory.api.client.api.observatory_api import ObservatoryApi  # noqa: E501
from observatory.platform.utils.test_utils import ObservatoryTestCase
from observatory.api.testing import ObservatoryApiEnvironment
from observatory.api.utils import (
    get_api_client,
    seed_dataset_type,
    seed_table_type,
    seed_workflow_type,
    seed_organisation,
    seed_workflow,
    seed_dataset,
    clear_table_types,
    clear_dataset_types,
    clear_workflow_types,
    clear_organisations,
    clear_workflows,
    clear_datasets,
    clear_dataset_releases,
    get_table_type_ids,
    get_organisation_ids,
    get_workflow_type_ids,
    get_workflows,
    get_dataset_type,
)
from observatory.api.client.model.dataset_type import DatasetType
from observatory.api.client.model.table_type import TableType
from observatory.api.client.model.dataset import Dataset
from observatory.api.client.model.dataset_release import DatasetRelease
from observatory.api.client.model.workflow_type import WorkflowType
from observatory.api.client.model.organisation import Organisation
from observatory.api.client.model.workflow import Workflow

import pendulum


class TestApiUtils(ObservatoryTestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # API environment
        self.host = "localhost"
        self.port = 5001
        configuration = Configuration(host=f"http://{self.host}:{self.port}")
        api_client = ApiClient(configuration)
        self.api = ObservatoryApi(api_client=api_client)  # noqa: E501
        self.env = ObservatoryApiEnvironment(host=self.host, port=self.port)
        self.table_type_info = self.ttinfo()
        self.dataset_type_info = self.dtinfo()
        self.workflow_type_info = self.wftinfo()
        self.org_info = self.oinfo()
        self.workflow_info = self.wfinfo()
        self.dataset_info = self.dsinfo()

    def ttinfo(self):
        table_type_info = OrderedDict()
        table_type_info["regular"] = TableType(type_id="regular", name="Regular BigQuery table")
        return table_type_info

    def dtinfo(self):
        dataset_type_info = OrderedDict()
        dataset_type_info["crossref_events"] = DatasetType(type_id="crossref_events", name="Crossref Events", table_type=TableType(id=1))
        return dataset_type_info

    def wftinfo(self):
        workflow_type_info = OrderedDict()
        workflow_type_info["crossref_events"] = WorkflowType(type_id="crossref_events", name="Crossref Events Telescope")
        return workflow_type_info

    def oinfo(self):
        org_info = OrderedDict()
        org_info["COKI Press"] = Organisation(name="COKI Press", project_id="project", download_bucket="dbucket", transform_bucket="tbucket")
        return org_info

    def wfinfo(self):
        workflow_info = OrderedDict()
        workflow_info["COKI Crossref Events Telescope"] = Workflow(name="COKI Crossref Events Telescope", workflow_type=WorkflowType(id=1), extra={}, tags=None)
        return workflow_info

    def dsinfo(self):
        dataset_info = OrderedDict()
        dataset_info["COKI Dataset"] = Dataset(name="COKI Dataset", service="google", address="project.dataset.table", workflow=Workflow(id=1), dataset_type=DatasetType(id=1))
        return dataset_info

    def test_table_types(self):
        with self.env.create():
            records = self.api.get_table_types(limit=100)
            self.assertEqual(len(records), 0)

            seed_table_type(api=self.api, table_type_info=self.table_type_info)
            records = self.api.get_table_types(limit=100)
            self.assertEqual(len(records), 1)
            
            seed_table_type(api=self.api, table_type_info=self.table_type_info)
            records = self.api.get_table_types(limit=100)
            self.assertEqual(len(records), 1)

            clear_table_types(self.api)
            
            records = self.api.get_table_types(limit=100)
            self.assertEqual(len(records), 0)

    def test_dataset_type(self):
        with self.env.create():
            seed_table_type(api=self.api, table_type_info=self.table_type_info)

            records = self.api.get_dataset_types(limit=100)
            self.assertEqual(len(records), 0)

            seed_dataset_type(api=self.api, dataset_type_info=self.dataset_type_info)
            records = self.api.get_dataset_types(limit=100)
            self.assertEqual(len(records), 1)

            seed_dataset_type(api=self.api, dataset_type_info=self.dataset_type_info)
            records = self.api.get_dataset_types(limit=100)
            self.assertEqual(len(records), 1)

            clear_dataset_types(self.api)

            records = self.api.get_dataset_types(limit=100)
            self.assertEqual(len(records), 0)

    def test_workflow_type(self):
        with self.env.create():
            records = self.api.get_workflow_types(limit=100)
            self.assertEqual(len(records), 0)

            seed_workflow_type(api=self.api, workflow_type_info=self.workflow_type_info)
            records = self.api.get_workflow_types(limit=100)
            self.assertEqual(len(records), 1)

            seed_workflow_type(api=self.api, workflow_type_info=self.workflow_type_info)
            records = self.api.get_workflow_types(limit=100)
            self.assertEqual(len(records), 1)

            clear_workflow_types(self.api)

            records = self.api.get_workflow_types(limit=100)
            self.assertEqual(len(records), 0)

    def test_organisation(self):
        with self.env.create():
            records = self.api.get_organisations(limit=100)
            self.assertEqual(len(records), 0)

            seed_organisation(api=self.api, organisation_info=self.org_info)
            records = self.api.get_organisations(limit=100)
            self.assertEqual(len(records), 1)

            seed_organisation(api=self.api, organisation_info=self.org_info)
            records = self.api.get_organisations(limit=100)
            self.assertEqual(len(records), 1)

            clear_organisations(self.api)

            records = self.api.get_organisations(limit=100)
            self.assertEqual(len(records), 0)

    def test_workflow(self):
        with self.env.create():
            records = self.api.get_workflows(limit=100)
            self.assertEqual(len(records), 0)

            seed_workflow_type(api=self.api, workflow_type_info=self.workflow_type_info)

            seed_workflow(api=self.api, workflow_info=self.workflow_info)
            records = self.api.get_workflows(limit=100)
            self.assertEqual(len(records), 1)

            seed_workflow(api=self.api, workflow_info=self.workflow_info)
            records = self.api.get_workflows(limit=100)
            self.assertEqual(len(records), 1)

            clear_workflows(self.api)

            records = self.api.get_workflows(limit=100)
            self.assertEqual(len(records), 0)

    def test_dataset(self):
        with self.env.create():
            records = self.api.get_datasets(limit=100)
            self.assertEqual(len(records), 0)

            seed_workflow_type(api=self.api, workflow_type_info=self.workflow_type_info)
            seed_workflow(api=self.api, workflow_info=self.workflow_info)
            seed_table_type(api=self.api, table_type_info=self.table_type_info)
            seed_dataset_type(api=self.api, dataset_type_info=self.dataset_type_info)

            seed_dataset(api=self.api, dataset_info=self.dataset_info)
            records = self.api.get_datasets(limit=100)
            self.assertEqual(len(records), 1)

            seed_dataset(api=self.api, dataset_info=self.dataset_info)
            records = self.api.get_datasets(limit=100)
            self.assertEqual(len(records), 1)

            clear_datasets(self.api)

            records = self.api.get_datasets(limit=100)
            self.assertEqual(len(records), 0)

    def test_dataset_release(self):
        with self.env.create():
            records = self.api.get_dataset_releases(limit=100)
            self.assertEqual(len(records), 0)

            seed_workflow_type(api=self.api, workflow_type_info=self.workflow_type_info)
            seed_workflow(api=self.api, workflow_info=self.workflow_info)
            seed_table_type(api=self.api, table_type_info=self.table_type_info)
            seed_dataset_type(api=self.api, dataset_type_info=self.dataset_type_info)
            seed_dataset(api=self.api, dataset_info=self.dataset_info)

            dt = pendulum.now()
            release = DatasetRelease(start_date=dt, end_date=dt, dataset=Dataset(id=1))
            self.api.put_dataset_release(release)

            records = self.api.get_dataset_releases(limit=100)
            self.assertEqual(len(records), 1)

            clear_dataset_releases(self.api)

            records = self.api.get_dataset_releases(limit=100)
            self.assertEqual(len(records), 0)

    def test_get_api_client(self):
        # No env var set
        api = get_api_client()
        self.assertEqual(api.api_client.configuration.host, "http://localhost:5002")
        self.assertEqual(api.api_client.configuration.api_key, {})

        # Host set
        os.environ["API_HOST"] = "testhost"
        api = get_api_client()
        self.assertEqual(api.api_client.configuration.host, "http://testhost:5002")
        self.assertEqual(api.api_client.configuration.api_key, {})

        # Port set
        os.environ["API_PORT"] = "5001"
        api = get_api_client()
        self.assertEqual(api.api_client.configuration.host, "http://testhost:5001")
        self.assertEqual(api.api_client.configuration.api_key, {})

        # API key set
        os.environ["API_KEY"] = "mykey"
        api = get_api_client()
        self.assertEqual(api.api_client.configuration.host, "http://testhost:5001")
        self.assertEqual(api.api_client.configuration.api_key, "mykey")

        # Pass in host argument
        api = get_api_client(host="host1")
        self.assertEqual(api.api_client.configuration.host, "http://host1:5001")
        self.assertEqual(api.api_client.configuration.api_key, "mykey")

        # Pass in port argument
        api = get_api_client(port=1000)
        self.assertEqual(api.api_client.configuration.host, "http://testhost:1000")
        self.assertEqual(api.api_client.configuration.api_key, "mykey")

        # Pass in key argument
        api = get_api_client(api_key="key")
        self.assertEqual(api.api_client.configuration.host, "http://testhost:5001")
        self.assertEqual(api.api_client.configuration.api_key, "key")

    def test_get_table_type_ids(self):
        with self.env.create():
            seed_table_type(api=self.api, table_type_info=self.table_type_info)
            ttids = get_table_type_ids(self.api)
            self.assertEqual(len(ttids), 1)
            self.assertEqual(ttids["regular"], 1)

    def test_get_organisation_ids(self):
        with self.env.create():
            seed_organisation(api=self.api, organisation_info=self.org_info)
            oids = get_organisation_ids(self.api)
            self.assertEqual(len(oids), 1)
            self.assertEqual(oids["COKI Press"], 1)

    def test_get_workflow_type_ids(self):
        with self.env.create():
            seed_workflow_type(api=self.api, workflow_type_info=self.workflow_type_info)
            wftids = get_workflow_type_ids(self.api)
            self.assertEqual(len(wftids), 1)
            self.assertEqual(wftids["crossref_events"], 1)

    def test_get_workflows(self):
        with self.env.create():
            seed_workflow_type(api=self.api, workflow_type_info=self.workflow_type_info)
            seed_workflow(api=self.api, workflow_info=self.workflow_info)
            wf = get_workflows(self.api)
            self.assertEqual(len(wf), 1)
            self.assertEqual(wf["COKI Crossref Events Telescope"].id, 1)            

    def test_get_dataset_type(self):
        with self.env.create():
            seed_table_type(api=self.api, table_type_info=self.table_type_info)
            seed_dataset_type(api=self.api, dataset_type_info=self.dataset_type_info)
            
            self.assertEqual(get_dataset_type(api=self.api, type_id="crossref_events").id, 1)