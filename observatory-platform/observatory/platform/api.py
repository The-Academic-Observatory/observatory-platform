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

# Author: Author: Tuan Chien, James Diprose

import datetime
import logging
from typing import List, Optional

import pendulum
from airflow.hooks.base import BaseHook

from observatory.api.client.model.dataset_release import DatasetRelease
from observatory.platform.config import AirflowConns


def make_observatory_api(observatory_api_conn_id: str = AirflowConns.OBSERVATORY_API) -> "ObservatoryApi":  # noqa: F821
    """Make the ObservatoryApi object, configuring it with a host and api_key.

    :param observatory_api_conn_id: the Observatory API Airflow Connection ID.
    :return: the ObservatoryApi.
    """

    try:
        from observatory.api.client.api.observatory_api import ObservatoryApi
        from observatory.api.client.api_client import ApiClient
        from observatory.api.client.configuration import Configuration
    except ImportError as e:
        logging.error("Please install the observatory-api Python package to use the make_observatory_api function")
        raise e

    # Get connection
    conn = BaseHook.get_connection(observatory_api_conn_id)

    # Assert connection has required fields
    assert (
        conn.conn_type != "" and conn.conn_type is not None
    ), f"Airflow Connection {observatory_api_conn_id} conn_type must not be None"
    assert (
        conn.host != "" and conn.host is not None
    ), f"Airflow Connection {observatory_api_conn_id} host must not be None"

    # Make host
    host = f'{str(conn.conn_type).replace("_", "-").lower()}://{conn.host}'
    if conn.port:
        host += f":{conn.port}"

    # Only api_key when password present in connection
    api_key = None
    if conn.password != "" and conn.password is not None:
        api_key = {"api_key": conn.password}

    # Return ObservatoryApi
    config = Configuration(host=host, api_key=api_key)
    api_client = ApiClient(config)
    return ObservatoryApi(api_client=api_client)


def get_dataset_releases(*, dag_id: str, dataset_id: str) -> List[DatasetRelease]:
    """Get a list of dataset releases for a given dataset.

    :param dag_id: dag id.
    :param dataset_id: Dataset id.
    :return: List of dataset releases.
    """

    api = make_observatory_api()
    dataset_releases = api.get_dataset_releases(dag_id=dag_id, dataset_id=dataset_id)
    return dataset_releases


def get_latest_dataset_release(releases: List[DatasetRelease], date_key: str) -> Optional[DatasetRelease]:
    """Get the dataset release from the list with the most recent end date.

    :param releases: List of releases.
    :param date_key: the key for accessing dates.
    :return: Latest release (by end_date)
    """

    if len(releases) == 0:
        return None

    latest = releases[0]
    for release in releases:
        if getattr(release, date_key) > getattr(latest, date_key):
            latest = release

    return latest


def get_new_release_dates(*, dag_id: str, dataset_id: str, releases: List[datetime.datetime]) -> List[str]:
    """Get a list of new release dates, i.e., releases in the user supplied list that are not in the API db.

    :param dag_id: The DAG id to check.
    :param dataset_id: The dataset id to check.
    :param releases: List of release dates to check.
    :return: List of new releases in YYYYMMDD string format.
    """

    api_releases = get_dataset_releases(dag_id=dag_id, dataset_id=dataset_id)
    api_releases_set = set([release.end_date.strftime("%Y%m%d") for release in api_releases])
    releases_set = set([release.strftime("%Y%m%d") for release in releases])
    new_releases = list(releases_set.difference(api_releases_set))
    return new_releases


def is_first_release(dag_id: str, dataset_id: str) -> bool:
    """Use the API to check whether this is the first release of a dataset, i.e., are there no dataset release records.

    :param dag_id: DAG ID.
    :param dataset_id: dataset id.
    :return: Whether this is the first release.
    """

    releases = get_dataset_releases(dag_id=dag_id, dataset_id=dataset_id)
    return len(releases) == 0


def build_schedule(sched_start_date: pendulum.DateTime, sched_end_date: pendulum.DateTime):
    """Useful for API based data sources.

    Create a fetch schedule to specify what date ranges to use for each API call. Will default to once a month
    for now, but in the future if we are minimising API calls, this can be a more complicated scheme.

    :param sched_start_date: the schedule start date.
    :param sched_end_date: the end date of the schedule.
    :return: list of (section_start_date, section_end_date) pairs from start_date to current Airflow DAG start date.
    """

    schedule = []

    for start_date in pendulum.Period(start=sched_start_date, end=sched_end_date).range("months"):
        if start_date >= sched_end_date:
            break
        end_date = start_date.add(months=1).subtract(days=1).end_of("day")
        end_date = min(sched_end_date, end_date)
        schedule.append(pendulum.Period(start_date.date(), end_date.date()))

    return schedule
