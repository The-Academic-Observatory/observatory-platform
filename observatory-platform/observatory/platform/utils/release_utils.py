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


import os
import re
import pendulum
from observatory.platform.utils.api import make_observatory_api
from observatory.platform.utils.config_utils import find_schema
from typing import List, Union, Tuple
from observatory.api.client.model.dataset_release import DatasetRelease
from observatory.api.client.model.dataset import Dataset
import datetime


def address_to_gcp_fields(address: str) -> Tuple[str, str, str]:
    """Take an address in the DatasetStorage field, and return the project ID, dataset ID, table ID.

    :param address: Address string in the DatasetStorage field.
    :return GCP project, dataset, table.
    """

    return tuple(address.split("."))


def get_datasets(*, workflow_id: str) -> List["Dataset"]:  # noqa: F821
    """Get a list of datasets for a workflow.
    Only returns the first 1000 datasets for that workflow.

    :param workflow_id: Workflow type ID.
    :return: List of Dataset objects.
    """

    limit = 1000
    api = make_observatory_api()
    datasets = api.get_datasets(workflow_id=workflow_id, limit=limit)
    return datasets


def get_dataset_releases(*, dataset_id: int) -> List[DatasetRelease]:
    """Get a list of dataset releases for a given dataset.

    :param dataset_id: Dataset id.
    :return: List of dataset releases.
    """

    limit = 1000
    api = make_observatory_api()
    dataset_releases = api.get_dataset_releases(dataset_id=dataset_id, limit=limit)
    return dataset_releases


def get_latest_dataset_release(releases: List[DatasetRelease]) -> DatasetRelease:
    """Get the dataset release from the list with the most recent end date.

    :param releases: List of releases.
    :return: Latest release (by end_date)
    """

    latest = releases[0]
    for release in releases:
        if release.end_date > latest.end_date:
            latest = release

    return latest


def get_start_end_date(
    release: "AbstractRelease",  # noqa: F821
) -> Tuple[pendulum.DateTime, pendulum.DateTime]:
    """Get the start date and end date from a release object.
    Supports start_date, end_date, release_date, release_id attributes.
    Those attributes need to be date or datetime objects.

    :param release: Release object.
    :return: Start date, end date.
    """

    if "start_date" in release.__dict__ and "end_date" in release.__dict__:
        start_date = release.start_date
        end_date = release.end_date
    elif "release_date" in release.__dict__:
        start_date = release.release_date
        end_date = release.release_date
    else:  # "release_id" in release.__dict__:  # This will only work if release_id is a date.
        start_date = pendulum.parse(release.release_id)
        end_date = pendulum.parse(release.release_id)

    return start_date, end_date


def get_new_release_dates(*, dataset_id: int, releases: List[datetime.datetime]) -> List[str]:
    """Get a list of new release dates, i.e., releases in the user supplied list that are not in the API db.

    :param dataset_id: The dataset id to check.
    :param releases: List of release dates to check.
    :return: List of new releases in YYYYMMDD string format.
    """

    api_releases = get_dataset_releases(dataset_id=dataset_id)
    api_releases_set = set([release.end_date.strftime("%Y%m%d") for release in api_releases])
    releases_set = set([release.strftime("%Y%m%d") for release in releases])
    new_releases = list(releases_set.difference(api_releases_set))
    return new_releases


def get_start_end_date_key(release: Tuple[datetime.datetime, datetime.datetime]) -> str:
    """Construct a string key from a start and end date.

    :param release: A (start date, end date) pair.
    :return: Key string.
    """

    key = release[0].strftime("%Y%m%d") + release[1].strftime("%Y%m%d")
    return key


def get_new_start_end_release_dates(
    *, dataset_id: int, releases: List[Tuple[datetime.datetime, datetime.datetime]]
) -> List[Tuple[str, str]]:
    """Get a list of (start date, end date) that are in the list supplied by the user but not in the API db.

    :param dataset_id: The dataset id to check.
    :param releases: List of (start date, end date) to check.
    :return: List of new (start date, end date) in YYYYMMDD string format.
    """

    api_releases = get_dataset_releases(dataset_id=dataset_id)
    api_releases_set = set([get_start_end_date_key((release.start_date, release.end_date)) for release in api_releases])
    releases_set = set([get_start_end_date_key(release) for release in releases])
    new_releases = releases_set.difference(api_releases_set)

    output = [(release[:8], release[8:]) for release in new_releases]
    return output


def is_first_release(workflow_id: int) -> bool:
    """Use the API to check whether this is the first release of a dataset, i.e., are there no dataset release records.

    :param workflow_id: API workflow id.
    :return: Whether this is the first release.
    """

    datasets = get_datasets(workflow_id=workflow_id)
    releases = get_dataset_releases(dataset_id=datasets[0].id)
    return len(releases) == 0


def add_dataset_release(
    *,
    start_date: pendulum.DateTime,
    end_date: pendulum.DateTime,
    dataset_id: int,
):
    """Add a new dataset release record to the API database.

    :param start_date: Start date.
    :param end_date: End date.
    :param dataset_id: API Dataset id.
    """

    dataset_release = DatasetRelease(
        start_date=start_date,
        end_date=end_date,
        dataset=Dataset(id=dataset_id),
    )
    api = make_observatory_api()
    api.post_dataset_release(dataset_release)
