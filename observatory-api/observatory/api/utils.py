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


import os
from collections import OrderedDict
from urllib.parse import urlparse

from observatory.api.client import ApiClient, Configuration
from observatory.api.client.api.observatory_api import ObservatoryApi
from observatory.api.client.model.dataset_type import DatasetType
from observatory.api.client.model.workflow import Workflow


limit = int(1e6)


def get_api_client(host: str = None, port: int = None, api_key: dict = None) -> ObservatoryApi:
    """Get an API client.
    :param host: URL for api server.
    :param port: Server port.
    :param api_key: API key.
    :return: ObservatoryApi object.
    """

    url = os.environ["API_URL"]
    url_fields = urlparse(url)

    if host is None:
        host = url_fields.hostname if url_fields.hostname is not None else "localhost"

    if port is None:
        port = url_fields.port if url_fields.port is not None else 5002

    if api_key is None:
        api_key = {"api_key": url_fields.password} if url_fields.password is not None else None

    configuration = Configuration(host=f"{url_fields.scheme}://{host}:{port}", api_key=api_key)
    api_client = ApiClient(configuration)
    api = ObservatoryApi(api_client=api_client)
    return api


def seed_dataset_type(*, dataset_type_info: OrderedDict, api: ObservatoryApi):
    """Seed API DatasetType records into the database.
    :param api: API client object.
    :param dataset_type_info: DatasetType dictionary containing information to seed.
    """

    existing_dt = api.get_dataset_types(limit=limit)
    existing_tids = set([dt.type_id for dt in existing_dt])

    for dt in dataset_type_info.values():
        if dt.type_id not in existing_tids:
            api.put_dataset_type(dt)


def seed_workflow_type(*, workflow_type_info: OrderedDict, api: ObservatoryApi):
    """Seed API WorkflowType records into the database.
    :param api: API client object.
    :param workflow_type_info: WorkflowType dictionary containing information to seed.
    """

    existing_wt = api.get_workflow_types(limit=limit)
    existing_tids = set([wt.type_id for wt in existing_wt])

    for wt in workflow_type_info.values():
        if wt.type_id not in existing_tids:
            api.put_workflow_type(wt)


def seed_table_type(*, table_type_info: OrderedDict, api: ObservatoryApi):
    """Seed API TableType records into the database.
    :param api: API client object.
    :param table_type_info: TableType dictionary containing information to seed.
    """

    existing_tt = api.get_table_types(limit=limit)
    existing_tids = set([tt.type_id for tt in existing_tt])

    for table_type in table_type_info.values():
        if table_type.type_id not in existing_tids:
            api.put_table_type(table_type)


def get_table_type_ids(api: ObservatoryApi) -> dict:
    """Get TableType ids from type_id.
    :param api: API client object.
    :return: TableType id.
    """

    table_types = api.get_table_types(limit=limit)
    ttids = {tt.type_id: tt.id for tt in table_types}

    return ttids


def get_organisation_ids(api: ObservatoryApi) -> dict:
    """Get Organisation ids from type_id.
    :param api: API client object.
    :return: TableType id.
    """

    orgs = api.get_organisations(limit=limit)
    org_ids = {org.name: org.id for org in orgs}

    return org_ids


def seed_organisation(*, organisation_info: OrderedDict, api: ObservatoryApi):
    """Seed API Organisation records into the database.
    :param api: API client object.
    :param organisation_info: Organisation dictionary containing information to seed.
    """

    existing_orgs = api.get_organisations(limit=limit)
    existing_orgnames = set([org.name for org in existing_orgs])

    for org in organisation_info.values():
        if org.name not in existing_orgnames:
            api.put_organisation(org)


def get_workflow_type_ids(api: ObservatoryApi) -> dict:
    """Get WorkflowType ids from type_id.
    :param api: API client object.
    :return: TableType id.
    """

    workflow_types = api.get_workflow_types(limit=limit)
    wt_tids = {wt.type_id: wt.id for wt in workflow_types}

    return wt_tids


def seed_workflow(*, workflow_info: OrderedDict, api: ObservatoryApi):
    """Seed API Workflow records into the database.
    :param api: API client object.
    :param workflow_info: Workflow dictionary containing information to seed.
    """

    existing_wf = api.get_workflows(limit=limit)
    existing_wfnames = set([wf.name for wf in existing_wf])

    for wf in workflow_info.values():
        if wf.name not in existing_wfnames:
            api.put_workflow(wf)


def get_workflows(api: ObservatoryApi):
    """Get Workflow from name.
    :param api: API client object.
    :return: Workflow id.
    """

    workflows = api.get_workflows(limit=limit)
    wf_ids = {wf.name: Workflow(id=wf.id) for wf in workflows}

    return wf_ids


def get_dataset_type(*, api, type_id):
    """Get DatasetType from type_id.
    :param api: API client object.
    :return: DatasetType object.
    """
    wt = api.get_dataset_type(type_id=type_id)
    return DatasetType(id=wt.id)


def seed_dataset(*, dataset_info: OrderedDict, api: ObservatoryApi):
    """Seed API Dataset records into the database.
    :param api: API client object.
    :param dataset_info: Dataset dictionary containing information to seed.
    """

    existing_ds = api.get_datasets(limit=limit)
    existing_dsnames = set([ds.name for ds in existing_ds])

    for ds in dataset_info.values():
        if ds.name not in existing_dsnames:
            api.put_dataset(ds)


def clear_table_types(api: ObservatoryApi):
    """Clear all TableType records from the API db.
    :param api: API client object.
    """

    existing = api.get_table_types(limit=limit)
    for record in existing:
        api.delete_table_type(id=record.id)


def clear_dataset_types(api: ObservatoryApi):
    """Clear all DatasetType records from the API db.
    :param api: API client object.
    """

    existing = api.get_dataset_types(limit=limit)
    for record in existing:
        api.delete_dataset_type(id=record.id)


def clear_workflow_types(api: ObservatoryApi):
    """Clear all WorkflowType records from the API db.
    :param api: API client object.
    """

    existing = api.get_workflow_types(limit=limit)
    for record in existing:
        api.delete_workflow_type(id=record.id)


def clear_workflows(api: ObservatoryApi):
    """Clear all Workflow records from the API db.
    :param api: API client object.
    """

    existing = api.get_workflows(limit=limit)
    for record in existing:
        api.delete_workflow(id=record.id)


def clear_datasets(api: ObservatoryApi):
    """Clear all Dataset records from the API db.
    :param api: API client object.
    """

    existing = api.get_datasets(limit=limit)
    for record in existing:
        api.delete_dataset(id=record.id)


def clear_dataset_releases(api: ObservatoryApi):
    """Clear all DatasetRelease records from the API db.
    :param api: API client object.
    """

    existing = api.get_dataset_releases(limit=limit)
    for record in existing:
        api.delete_dataset_release(id=record.id)


def clear_organisations(api: ObservatoryApi):
    """Clear all Organisation records from the API db.
    :param api: API client object.
    """

    existing = api.get_organisations(limit=limit)
    for record in existing:
        api.delete_organisation(id=record.id)
