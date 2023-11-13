# Copyright 2020-2024 Curtin University
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

from __future__ import annotations

import dataclasses
import datetime
import os.path
from typing import List, Optional, Dict, Union

import pendulum
from google.cloud import bigquery

from observatory_platform.config import module_file_path
from observatory_platform.google.bigquery import (
    bq_load_from_memory,
    bq_create_dataset,
    bq_create_empty_table,
    bq_run_query,
)


@dataclasses.dataclass
class DatasetRelease:
    id: int
    dag_id: str
    dataset_id: str
    dag_run_id: str
    data_interval_start: pendulum.DateTime
    data_interval_end: pendulum.DateTime
    snapshot_date: pendulum.DateTime
    partition_date: pendulum.DateTime
    changefile_start_date: pendulum.DateTime
    changefile_end_date: pendulum.DateTime
    sequence_start: int
    sequence_end: int
    extra: Dict
    created: pendulum.DateTime
    modified: pendulum.DateTime

    def __init__(
        self,
        *,
        dag_id: str,
        dataset_id: str,
        dag_run_id: str,
        created: pendulum.DateTime,
        modified: pendulum.DateTime,
        data_interval_start: Union[pendulum.DateTime, str] = None,
        data_interval_end: Union[pendulum.DateTime, str] = None,
        snapshot_date: Union[pendulum.DateTime, str] = None,
        partition_date: Union[pendulum.DateTime, str] = None,
        changefile_start_date: Union[pendulum.DateTime, str] = None,
        changefile_end_date: Union[pendulum.DateTime, str] = None,
        sequence_start: int = None,
        sequence_end: int = None,
        extra: dict = None,
    ):
        """Construct a DatasetRelease object.

        :param dag_id: the DAG ID.
        :param dataset_id: the dataset ID.
        :param dag_run_id: the DAG's run ID.
        :param created: datetime created in UTC.
        :param modified: datetime modified in UTC.
        :param data_interval_start: the DAGs data interval start. Date is inclusive.
        :param data_interval_end: the DAGs data interval end. Date is exclusive.
        :param snapshot_date: the release date of the snapshot.
        :param partition_date: the partition date.
        :param changefile_start_date: the date of the first changefile processed in this release.
        :param changefile_end_date: the date of the last changefile processed in this release.
        :param sequence_start: the starting sequence number of files that make up this release.
        :param sequence_end: the end sequence number of files that make up this release.
        :param extra: optional extra field for storing any data.

        """

        self.dag_id = dag_id
        self.dataset_id = dataset_id
        self.dag_run_id = dag_run_id
        self.data_interval_start = data_interval_start
        self.data_interval_end = data_interval_end
        self.snapshot_date = snapshot_date
        self.partition_date = partition_date
        self.changefile_start_date = changefile_start_date
        self.changefile_end_date = changefile_end_date
        self.sequence_start = sequence_start
        self.sequence_end = sequence_end
        self.extra = extra
        self.created = created
        self.modified = modified

    @staticmethod
    def from_dict(_dict: Dict) -> DatasetRelease:
        return DatasetRelease(
            dag_id=_dict["dag_id"],
            dataset_id=_dict["dataset_id"],
            dag_run_id=_dict["dag_run_id"],
            created=bq_timestamp_to_pendulum(_dict["created"]),
            modified=bq_timestamp_to_pendulum(_dict["modified"]),
            data_interval_start=bq_timestamp_to_pendulum(_dict.get("data_interval_start")),
            data_interval_end=bq_timestamp_to_pendulum(_dict.get("data_interval_end")),
            snapshot_date=bq_timestamp_to_pendulum(_dict.get("snapshot_date")),
            partition_date=bq_timestamp_to_pendulum(_dict.get("partition_date")),
            changefile_start_date=bq_timestamp_to_pendulum(_dict.get("changefile_start_date")),
            changefile_end_date=bq_timestamp_to_pendulum(_dict.get("changefile_end_date")),
            sequence_start=_dict.get("sequence_start"),
            sequence_end=_dict.get("sequence_end"),
            extra=_dict.get("extra"),
        )

    def to_dict(self) -> Dict:
        return dict(
            dag_id=self.dag_id,
            dataset_id=self.dataset_id,
            dag_run_id=self.dag_run_id,
            created=self.created.to_iso8601_string(),
            modified=self.modified.to_iso8601_string(),
            data_interval_start=pendulum_to_bq_timestamp(self.data_interval_start),
            data_interval_end=pendulum_to_bq_timestamp(self.data_interval_end),
            snapshot_date=pendulum_to_bq_timestamp(self.snapshot_date),
            partition_date=pendulum_to_bq_timestamp(self.partition_date),
            changefile_start_date=pendulum_to_bq_timestamp(self.changefile_start_date),
            changefile_end_date=pendulum_to_bq_timestamp(self.changefile_end_date),
            sequence_start=self.sequence_start,
            sequence_end=self.sequence_end,
            extra=self.extra,
        )

    def __eq__(self, other):
        if isinstance(other, DatasetRelease):
            return self.__dict__ == other.__dict__
        return False


class DatasetAPI:
    def __init__(
        self,
        project_id: str = None,
        dataset_id: str = "dataset_api",
        table_id: str = "dataset_releases",
        location: str = "us",
        client: Optional[bigquery.Client] = None,
    ):
        """Create a DatasetAPI instance.

        :param project_id: the BigQuery project ID.
        :param dataset_id: the BigQuery dataset ID.
        :param table_id: the BigQuery table ID.
        :param location: the BigQuery dataset location.
        :param client: Optional BigQuery client.
        """

        parts = []
        if project_id is None:
            project_id = get_bigquery_default_project()
        parts.append(project_id)
        parts.append(dataset_id)
        parts.append(table_id)

        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.location = location
        self.client = client
        self.full_table_id = ".".join(parts)
        self.schema_file_path = os.path.join(module_file_path("observatory_platform.schema"), "dataset_release.json")

    def seed_db(self):
        """Seed the BigQuery dataset and dataset release table.

        :return: None.
        """

        # Create BigQuery dataset if it does not exist
        bq_create_dataset(
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            location=self.location,
            description="Observatory Platform Dataset Release API",
            client=self.client,
        )

        # Load empty table
        bq_create_empty_table(
            table_id=self.full_table_id,
            schema_file_path=self.schema_file_path,
            exists_ok=True,
            client=self.client,
        )

    def add_dataset_release(self, release: DatasetRelease):
        """Adds a DatasetRelease.

        :param release: the release.
        :return: None.
        """

        # Load data
        success = bq_load_from_memory(
            table_id=self.full_table_id,
            records=[release.to_dict()],
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            schema_file_path=self.schema_file_path,
            client=self.client,
        )
        if not success:
            raise Exception("Failed to add dataset release")

    def get_dataset_releases(
        self, *, dag_id: str, dataset_id: str, date_key: str = "created", limit: int | None = None
    ) -> List[DatasetRelease]:
        """Get a list of dataset releases for a given dataset.

        :param dag_id: dag id.
        :param dataset_id: Dataset id.
        :param date_key: the date key to use when sorting by date. One of: "created", "modified", "data_interval_start",
        "data_interval_end", "snapshot_date", "partition_date", "changefile_start_date" or "changefile_end_date".
        :param limit: the maximum number of rows to return.
        :return: List of dataset releases.
        """

        valid_date_keys = {
            "created",
            "modified",
            "data_interval_start",
            "data_interval_end",
            "snapshot_date",
            "partition_date",
            "changefile_start_date",
            "changefile_end_date",
        }
        if date_key not in valid_date_keys:
            raise ValueError(f"get_dataset_releases: invalid date_key: {date_key}, should be one of: {valid_date_keys}")

        sql = [f"SELECT * FROM `{self.full_table_id}` WHERE dag_id = '{dag_id}' AND dataset_id = '{dataset_id}'"]
        sql.append(f"ORDER BY {date_key} DESC")
        if limit is not None:
            sql.append(f"LIMIT {limit}")

        # Fetch results
        results = bq_run_query("\n".join(sql), client=self.client)

        # Convert to DatasetRelease objects
        results = [DatasetRelease.from_dict(dict(result)) for result in results]

        return results

    def get_latest_dataset_release(self, *, dag_id: str, dataset_id: str, date_key: str) -> Optional[DatasetRelease]:
        """Get the latest dataset release.

        :param dag_id: the Airflow DAG id.
        :param dataset_id: the dataset id.
        :param date_key: the date key. One of: "created", "modified", "data_interval_start", "data_interval_end",
        "snapshot_date", "partition_date", "changefile_start_date" or "changefile_end_date".
        :return: the latest release or None if there is no release.
        """

        releases = self.get_dataset_releases(dag_id=dag_id, dataset_id=dataset_id, date_key=date_key, limit=1)
        if len(releases) == 0:
            return None
        return releases[0]

    def is_first_release(self, *, dag_id: str, dataset_id: str) -> bool:
        """Use the API to check whether this is the first release of a dataset, i.e., are there no dataset release records.

        :param dag_id: DAG ID.
        :param dataset_id: dataset id.
        :return: Whether this is the first release.
        """

        results = bq_run_query(
            f"SELECT COUNT(*) as count FROM `{self.full_table_id}` WHERE dag_id = '{dag_id}' AND dataset_id = '{dataset_id}'",
            client=self.client,
        )
        count = results[0]["count"]
        return count == 0


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


def bq_timestamp_to_pendulum(obj: any) -> pendulum.DateTime | None:
    """Convert a BigQuery timestamp to a pendulum DateTime object.

    :param obj: None, a string or datetime.datetime instance.
    :return: pendulum.DateTime or None.
    """

    if obj is None:
        return obj
    elif isinstance(obj, datetime.datetime):
        return pendulum.instance(obj)
    elif isinstance(obj, str):
        return pendulum.parse(obj)
    raise NotImplementedError("Unsupported type")


def pendulum_to_bq_timestamp(dt: pendulum.DateTime | None) -> str:
    """Convert a pendulum instance to a BigQuery timestamp string.

    :param dt: the pendulum DateTime instance or None.
    :return: the string.
    """

    return None if dt is None else dt.to_iso8601_string()


def get_bigquery_default_project() -> str:
    """Get the default BigQuery project ID.

    :return: BigQuery project ID.
    """

    client = bigquery.Client()
    return client.project
