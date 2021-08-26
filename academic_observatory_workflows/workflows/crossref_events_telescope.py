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

from __future__ import annotations

import logging
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Tuple, Union

import jsonlines
import pendulum
import requests
from airflow.exceptions import AirflowSkipException
from airflow.models.taskinstance import TaskInstance
from tenacity import RetryError, retry, stop_after_attempt, wait_exponential, wait_fixed

from academic_observatory_workflows.config import schema_folder as default_schema_folder
from observatory.platform.utils.airflow_utils import AirflowVars
from observatory.platform.utils.url_utils import get_user_agent
from observatory.platform.utils.workflow_utils import upload_files_from_list
from observatory.platform.workflows.stream_telescope import (
    StreamRelease,
    StreamTelescope,
)


class CrossrefEventsRelease(StreamRelease):
    def __init__(
        self,
        dag_id: str,
        start_date: pendulum.DateTime,
        end_date: pendulum.DateTime,
        first_release: bool,
        mailto: str,
        max_processes: int,
    ):
        """Construct a CrossrefEventsRelease instance

        :param dag_id: the id of the DAG.
        :param start_date: the start_date of the release.
        :param end_date: the end_date of the release.
        :param first_release: whether this is the first release that is processed for this DAG
        :param mailto: Email address used in the download url
        :param max_processes: Max processes used for parallel downloading
        """
        download_files_regex = r".*.jsonl$"
        transform_files_regex = r".*.jsonl$"
        super().__init__(
            dag_id,
            start_date,
            end_date,
            first_release,
            download_files_regex=download_files_regex,
            transform_files_regex=transform_files_regex,
        )
        self.mailto = mailto
        self.max_processes = max_processes

    @property
    def urls(self) -> list:
        urls = []
        start_date = self.start_date.date()
        end_date = self.end_date.date()
        period = pendulum.period(start_date, end_date)
        for dt in period.range("days"):
            date_str = dt.strftime("%Y-%m-%d")
            start_date = date_str
            end_date = date_str

            events_url = (
                f"https://api.eventdata.crossref.org/v1/events?mailto={self.mailto}"
                f"&from-collected-date={start_date}&until-collected-date={end_date}&rows=1000"
            )
            edited_url = (
                f"https://api.eventdata.crossref.org/v1/events/edited?mailto={self.mailto}"
                f"&from-updated-date={start_date}&until-updated-date={end_date}&rows=1000"
            )
            deleted_url = (
                f"https://api.eventdata.crossref.org/v1/events/deleted?mailto={self.mailto}"
                f"&from-updated-date={start_date}&until-updated-date={end_date}&rows=1000"
            )

            urls.append(events_url)
            if not self.first_release:
                urls.append(edited_url)
                urls.append(deleted_url)
        return urls

    def batch_path(self, url, cursor: bool = False) -> str:
        """Gets the appropriate file path for a single batch, either for an events or cursor file.

        :param url: The url used for a specific batch
        :param cursor: Whether this is a cursor file or file with actual events
        :return: Path to the events or cursor file
        """
        event_type, date = parse_event_url(url)
        if cursor:
            return os.path.join(self.download_folder, f"{event_type}_{date}_cursor.txt")
        else:
            return os.path.join(self.download_folder, f"{event_type}_{date}.jsonl")

    def download(self):
        """Download all events.

        :return: None.
        """
        logging.info(f"Downloading events, no. workers: {self.max_processes}")
        logging.info(f"Downloading using these URLs, but with different start and end dates: {self.urls[0]}")

        with ThreadPoolExecutor(max_workers=self.max_processes) as executor:
            futures = []
            for i, url in enumerate(self.urls):
                futures.append(executor.submit(self.download_batch, i, url))
            for future in as_completed(futures):
                future.result()
        if len(self.download_files) == 0:
            raise AirflowSkipException("No events found")

    def download_batch(self, i: int, url: str):
        """Download one day of events. When the download finished successfully, the generated cursor file is deleted.
        If there is a cursor file available at the start, it means that a previous download attempt failed. If there
        is an events file available and no cursor file, it means that a previous download attempt was successful,
        so these events will not be downloaded again.

        :param i: URL counter
        :param url: The url from which to download events
        :return: None.
        """
        events_path = self.batch_path(url)
        cursor_path = self.batch_path(url, cursor=True)
        event_type, date = parse_event_url(url)

        # if events file exists but no cursor file, previous request has finished & successful
        if os.path.isfile(events_path) and not os.path.isfile(cursor_path):
            logging.info(f"{i + 1}.{event_type} Skipped, already finished: {date}")
            return

        logging.info(f"{i + 1}.{event_type} Downloading date: {date}")
        headers = {"User-Agent": get_user_agent(package_name="academic_observatory_workflows")}
        next_cursor, counts, total_events = download_events(url, headers, events_path, cursor_path)
        counter = counts
        while next_cursor:
            tmp_url = url + f"&cursor={next_cursor}"
            next_cursor, counts, _ = download_events(tmp_url, headers, events_path, cursor_path)
            counter += counts

        if os.path.isfile(cursor_path):
            os.remove(cursor_path)
        logging.info(
            f"{i + 1}.{event_type} successful, date: {date}, total no. events: {total_events}, downloaded "
            f"events: {counter}"
        )

    def transform(self):
        """Transform all events.

        :return: None.
        """
        logging.info(f"Transforming events, no. workers: {self.max_processes}")

        with ThreadPoolExecutor(max_workers=self.max_processes) as executor:
            futures = []
            for file in self.download_files:
                futures.append(executor.submit(self.transform_batch, file))
            for future in as_completed(futures):
                future.result()

    def transform_batch(self, download_path: str):
        """Transform one day of events.

        :param download_path: The path to the downloaded file.
        :return: None.
        """
        file_name = os.path.basename(download_path)
        transform_path = os.path.join(self.transform_folder, file_name)

        logging.info(f"Transforming file: {file_name}")
        with jsonlines.open(download_path, "r") as reader:
            with jsonlines.open(transform_path, "w") as writer:
                for event in reader:
                    event = transform_events(event)
                    writer.write(event)

        logging.info(f"Finished: {file_name}")


class CrossrefEventsTelescope(StreamTelescope):
    """Crossref Events telescope"""

    DAG_ID = "crossref_events"

    def __init__(
        self,
        dag_id: str = DAG_ID,
        start_date: pendulum.DateTime = pendulum.datetime(2018, 5, 14),
        schedule_interval: str = "@weekly",
        dataset_id: str = "crossref",
        dataset_description: str = "The Crossref Events dataset: https://www.eventdata.crossref.org/guide/",
        merge_partition_field: str = "id",
        bq_merge_days: int = 7,
        schema_folder: str = default_schema_folder(),
        batch_load: bool = True,
        airflow_vars: List = None,
        mailto: str = "aniek.roelofs@curtin.edu.au",
        max_processes: int = min(32, os.cpu_count() + 4),
    ):
        """Construct a CrossrefEventsTelescope instance.

        :param dag_id: the id of the DAG.
        :param start_date: the start date of the DAG.
        :param schedule_interval: the schedule interval of the DAG.
        :param dataset_id: the dataset id.
        :param dataset_description: the dataset description.
        :param merge_partition_field: the BigQuery field used to match partitions for a merge
        :param bq_merge_days: how often partitions should be merged (every x days)
        :param schema_folder: the SQL schema path.
        :param batch_load: whether all files in the transform folder are loaded into 1 table at once
        :param airflow_vars: list of airflow variable keys, for each variable it is checked if it exists in airflow
        :param mailto: Email address used in the download url
        :param max_processes: Max processes used for parallel downloading, default is based on 7 days x 3 url categories
        """

        if airflow_vars is None:
            airflow_vars = [
                AirflowVars.DATA_PATH,
                AirflowVars.PROJECT_ID,
                AirflowVars.DATA_LOCATION,
                AirflowVars.DOWNLOAD_BUCKET,
                AirflowVars.TRANSFORM_BUCKET,
            ]
        super().__init__(
            dag_id,
            start_date,
            schedule_interval,
            dataset_id,
            merge_partition_field,
            bq_merge_days,
            schema_folder,
            dataset_description=dataset_description,
            batch_load=batch_load,
            airflow_vars=airflow_vars,
        )
        self.mailto = mailto
        self.max_processes = max_processes

        self.add_setup_task_chain([self.check_dependencies, self.get_release_info])
        self.add_task_chain(
            [self.download, self.upload_downloaded, self.transform, self.upload_transformed, self.bq_load_partition]
        )
        self.add_task_chain([self.bq_delete_old, self.bq_append_new, self.cleanup], trigger_rule="none_failed")

    def make_release(self, **kwargs) -> CrossrefEventsRelease:
        """Make a Release instance

        :param kwargs: The context passed from the PythonOperator.
        :return: CrossrefEventsRelease
        """
        ti: TaskInstance = kwargs["ti"]
        start_date, end_date, first_release = ti.xcom_pull(
            key=CrossrefEventsTelescope.RELEASE_INFO, include_prior_dates=True
        )

        start_date = pendulum.parse(start_date)
        end_date = pendulum.parse(end_date)

        release = CrossrefEventsRelease(
            self.dag_id, start_date, end_date, first_release, self.mailto, self.max_processes
        )
        return release

    def download(self, release: CrossrefEventsRelease, **kwargs):
        """Task to download the CrossrefEventsRelease release.

        :param release: a CrossrefEventsRelease instance.
        :param kwargs: The context passed from the PythonOperator.
        :return: None.
        """
        release.download()

    def upload_downloaded(self, release: CrossrefEventsRelease, **kwargs):
        """Upload the downloaded files for the given release.

        :param release: a CrossrefEventsRelease instance
        :param kwargs: The context passed from the PythonOperator.
        :return: None.
        """
        upload_files_from_list(release.download_files, release.download_bucket)

    def transform(self, release: CrossrefEventsRelease, **kwargs):
        """Task to transform the CrossrefEventsRelease release.

        :param release: a CrossrefEventsRelease instance.
        :param kwargs: The context passed from the PythonOperator.
        :return: None.
        """
        release.transform()


@retry(
    stop=stop_after_attempt(3), wait=wait_fixed(20) + wait_exponential(multiplier=10, exp_base=3, max=60 * 10),
)
def get_response(url: str, headers: dict):
    """Get response from the url with given headers and retry for certain status codes.

    :param url: The url
    :param headers: The headers dict
    :return: The response
    """
    response = requests.get(url, headers=headers)
    if response.status_code in [500, 400, 429]:
        logging.info(
            f'Downloading events from url: {url}, attempt: {get_response.retry.statistics["attempt_number"]}, '
            f'idle for: {get_response.retry.statistics["idle_for"]}'
        )
        raise ConnectionError("Retrying url")
    return response


def parse_event_url(url: str) -> (str, str):
    """Parse the URL to get the event type and date

    :param url: The url
    :return: The event type and date
    """
    event_type = url.split("?mailto")[0].split("/")[-1]
    if event_type == "events":
        date = url.split("from-collected-date=")[1].split("&")[0]
    else:
        date = url.split("from-updated-date=")[1].split("&")[0]

    return event_type, date


def download_events(url: str, headers: dict, events_path: str, cursor_path: str) -> Tuple[Union[str, None], int, int]:
    """Extract the events from the given url until no new cursor is returned or a RetryError occurs.
    The extracted events are appended to a jsonl file and the cursors are written to a text file.

    :param url: The url
    :param headers: The headers dict
    :param events_path: Path to the file in which events are stored.
    :param cursor_path: Path to the file where cursors are stored.
    :return: next_cursor, counter of events and total number of events according to the response
    """
    try:
        response = get_response(url, headers)
    except RetryError:
        # Try again with rows set to 100
        url = re.sub("rows=[0-9]*", "rows=100", url)
        response = get_response(url, headers)

    if response.status_code == 200:
        response_json = response.json()
        total_events = response_json["message"]["total-results"]
        events = response_json["message"]["events"]
        next_cursor = response_json["message"]["next-cursor"]
        counter = len(events)

        # append events and cursor
        if events:
            with open(events_path, "a") as f:
                with jsonlines.Writer(f) as writer:
                    writer.write_all(events)
        if next_cursor:
            with open(cursor_path, "a") as f:
                f.write(next_cursor + "\n")
        return next_cursor, counter, total_events
    else:
        raise ConnectionError(f"Error requesting url: {url}, response: {response.text}")


def transform_events(event):
    """Transform the dictionary with event data by replacing '-' with '_' in key names, converting all int values to
    string except for the 'total' field and parsing datetime columns for a valid datetime.

    :param event: The event dictionary
    :return: The updated event dictionary
    """
    if isinstance(event, (str, int, float)):
        return event
    if isinstance(event, dict):
        new = event.__class__()
        for k, v in event.items():
            if isinstance(v, int) and k != "total":
                v = str(v)
            if k in ["timestamp", "occurred_at", "issued", "dateModified", "updated_date"]:
                try:
                    v = str(pendulum.parse(v))
                except ValueError:
                    v = "0001-01-01T00:00:00Z"
            k = k.replace("-", "_")
            new[k] = transform_events(v)
        return new
