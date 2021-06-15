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
from datetime import datetime
from typing import List, Tuple, Union

import jsonlines
import pendulum
import requests
from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.models.taskinstance import TaskInstance
from observatory.platform.telescopes.stream_telescope import (StreamRelease, StreamTelescope)
from observatory.platform.utils.airflow_utils import AirflowVars
from observatory.platform.utils.telescope_utils import convert
from observatory.platform.utils.url_utils import get_ao_user_agent
from tenacity import retry, stop_after_attempt, wait_exponential, wait_fixed, RetryError


class CrossrefEventsRelease(StreamRelease):
    def __init__(self, dag_id: str, start_date: pendulum.Pendulum, end_date: pendulum.Pendulum, first_release: bool,
                 mailto: str, download_mode: str, max_processes: int):
        """ Construct a CrossrefEventsRelease instance

        :param dag_id: the id of the DAG.
        :param start_date: the start_date of the release.
        :param end_date: the end_date of the release.
        :param first_release: whether this is the first release that is processed for this DAG
        :param mailto: Email address used in the download url
        :param download_mode: Whether the events are downloaded in parallel, valid options: 'sequential' and 'parallel'
        :param max_processes: Max processes used for parallel downloading, default is based on 7 days x 3 url categories
        """
        transform_files_regex = r'.*.json$'
        super().__init__(dag_id, start_date, end_date, first_release, transform_files_regex=transform_files_regex)
        self.mailto = mailto
        self.download_mode = download_mode
        self.max_processes = max_processes

    @property
    def urls(self) -> list:
        urls = []
        start_date = self.start_date.date()
        end_date = self.end_date.date()
        period = pendulum.period(start_date, end_date)
        for dt in period.range('days'):
            date_str = dt.strftime("%Y-%m-%d")
            start_date = date_str
            end_date = date_str

            events_url = f'https://api.eventdata.crossref.org/v1/events?mailto={self.mailto}' \
                         f'&from-collected-date={start_date}&until-collected-date={end_date}&rows=1000'
            edited_url = f'https://api.eventdata.crossref.org/v1/events/edited?mailto={self.mailto}' \
                         f'&from-updated-date={start_date}&until-updated-date={end_date}&rows=1000'
            deleted_url = f'https://api.eventdata.crossref.org/v1/events/deleted?mailto={self.mailto}' \
                          f'&from-updated-date={start_date}&until-updated-date={end_date}&rows=1000'

            event_type_urls = [events_url]
            if not self.first_release:
                event_type_urls.append(edited_url)
                event_type_urls.append(deleted_url)
            urls.append(event_type_urls)
        return urls

    def batch_path(self, url, cursor: bool = False) -> str:
        """ Gets the appropriate file path for a single batch, either for an events or cursor file.

        :param url: The url used for a specific batch
        :param cursor: Whether this is a cursor file or file with actual events
        :return: Path to the events or cursor file
        """
        event_type, date = parse_event_url(url)
        if cursor:
            return os.path.join(self.transform_folder, f'{event_type}_{date}_cursor.txt')
        else:
            return os.path.join(self.transform_folder, f'{event_type}_{date}.json')

    def download_transform(self):
        """ Download one release of crossref events, this is from the start date of the previous successful DAG until
        the start date of this DAG. The release can be split up in periods (multiple batches), if the download mode is
        set to 'parallel'.

        :return:
        """

        if self.download_mode == 'parallel':
            no_workers = self.max_processes
            logging.info(f'Downloading events with parallel method, no. workers: {no_workers}')
        else:
            logging.info('Downloading events with sequential method')
            no_workers = 1
        # all_file_paths = self.download_events_batch(0)
        logging.info(f'Downloading using URLs with different start and end dates: {self.urls[0]}')
        all_events = 0
        with ThreadPoolExecutor(max_workers=no_workers) as executor:
            futures = []
            # select minimum, either no of batches or no of workers
            for i in range(len(self.urls)):
                futures.append(executor.submit(self.download_transform_events, i))
            for future in as_completed(futures):
                events = future.result()
                all_events += events
        if events == 0:
            raise AirflowSkipException('No events found')

    def download_transform_events(self, i: int) -> int:
        """ Download one batch (time period) of events.

        :param i: The batch number
        :return: Count of events (new, edited, deleted) found
        """
        events = 0
        # Extract all new, edited and deleted events
        for url in self.urls[i]:
            events_path = self.batch_path(url)
            cursor_path = self.batch_path(url, cursor=True)

            event_type, date = parse_event_url(url)

            # if events file exists but no cursor file, previous request has finished & successful
            if os.path.isfile(events_path) and not os.path.isfile(cursor_path):
                events_tmp = []
                with jsonlines.open(events_path, 'r') as reader:
                    with jsonlines.open(events_path + 'tmp', 'w') as writer:
                        for obj in reader:
                            event = change_keys(obj, convert)
                            writer.write(event)
                logging.info(f"{i + 1}.{event_type} Skipped, already finished: {date}")
                continue

            logging.info(f"{i + 1}.{event_type} Downloading date: {date}")

            headers = {'User-Agent': get_ao_user_agent()}
            next_cursor, counts, total_events = download_events(url, headers, events_path, cursor_path)
            counter = counts
            while next_cursor:
                tmp_url = url + f'&cursor={next_cursor}'
                next_cursor, counts, _ = download_events(tmp_url, headers, events_path, cursor_path)
                counter += counts

            if os.path.isfile(cursor_path):
                os.remove(cursor_path)
            logging.info(f'{i + 1}.{event_type} successful, date: {date}, total no. events: {total_events}, downloaded '
                         f'events: {counter}')
            events += counter
        return events


class CrossrefEventsTelescope(StreamTelescope):
    """ Crossref Events telescope """

    DAG_ID = 'crossref_events'

    def __init__(self, dag_id: str = DAG_ID, start_date: datetime = datetime(2018, 5, 14),
                 schedule_interval: str = '@weekly', dataset_id: str = 'crossref',
                 dataset_description: str = 'The Crossref Events dataset: https://www.eventdata.crossref.org/guide/',
                 merge_partition_field: str = 'id', updated_date_field: str = 'timestamp',
                 bq_merge_days: int = 7, airflow_vars: List = None, mailto: str = 'aniek.roelofs@curtin.edu.au',
                 download_mode: str = 'parallel', max_processes: int = 21):
        """ Construct a CrossrefEventsTelescope instance.

        :param dag_id: the id of the DAG.
        :param start_date: the start date of the DAG.
        :param schedule_interval: the schedule interval of the DAG.
        :param dataset_id: the dataset id.
        :param dataset_description: the dataset description.
        :param merge_partition_field: the BigQuery field used to match partitions for a merge
        :param updated_date_field: the BigQuery field used to determine newest entry for a merge
        :param bq_merge_days: how often partitions should be merged (every x days)
        :param airflow_vars: list of airflow variable keys, for each variable it is checked if it exists in airflow
        :param mailto: Email address used in the download url
        :param download_mode: Whether the events are downloaded in parallel, valid options: 'sequential' and 'parallel'
        :param max_processes: Max processes used for parallel downloading, default is based on 7 days x 3 url categories
        """

        if airflow_vars is None:
            airflow_vars = [AirflowVars.DATA_PATH, AirflowVars.PROJECT_ID, AirflowVars.DATA_LOCATION,
                            AirflowVars.DOWNLOAD_BUCKET, AirflowVars.TRANSFORM_BUCKET]
        super().__init__(dag_id, start_date, schedule_interval, dataset_id, merge_partition_field,
                         updated_date_field, bq_merge_days, dataset_description=dataset_description,
                         airflow_vars=airflow_vars, batch_load=True)
        self.mailto = mailto
        self.download_mode = download_mode
        self.max_processes = max_processes

        self.add_setup_task_chain([self.check_dependencies,
                                   self.get_release_info])
        self.add_task_chain([self.download_transform,
                             self.upload_transformed,
                             self.bq_load_partition])
        self.add_task_chain([self.bq_delete_old,
                             self.bq_append_new,
                             self.cleanup], trigger_rule='none_failed')

    def make_release(self, **kwargs) -> CrossrefEventsRelease:
        # Make Release instance
        ti: TaskInstance = kwargs['ti']
        start_date, end_date, first_release = ti.xcom_pull(key=CrossrefEventsTelescope.RELEASE_INFO,
                                                           include_prior_dates=True)

        release = CrossrefEventsRelease(self.dag_id, start_date, end_date, first_release, self.mailto,
                                        self.download_mode, self.max_processes)
        return release

    def download_transform(self, release: CrossrefEventsRelease, **kwargs):
        """ Task to download the CrossrefEventsRelease release.

        :param release: a CrossrefEventsRelease instance.
        :return: None.
        """
        # Download release
        release.download_transform()


@retry(stop=stop_after_attempt(3),
       wait=wait_fixed(20) + wait_exponential(multiplier=10,
                                              exp_base=3,
                                              max=60 * 10),
       )
def get_url(url: str, headers: dict):
    response = requests.get(url, headers=headers)
    if response.status_code in [500, 400, 429]:
        logging.info(f'Downloading events from url: {url}, attempt: {get_url.retry.statistics["attempt_number"]}, '
                     f'idle for: {get_url.retry.statistics["idle_for"]}')
        raise ConnectionError("Retrying url")
    return response


def download_events(url: str, headers: dict, events_path: str, cursor_path: str) -> Tuple[Union[str, None], int, int]:
    """
    Extract the events from the given url until no new cursor is returned or a RetryError occurs. The extracted events
    are appended to a json file, with 1 list per request.

    :param url: The crossref events api url.
    :param headers: The ao headers used in url.
    :param events_path: Path to the file in which events are stored.
    :param cursor_path: Path to the file where cursors are stored.
    :return: success, next_cursor and number of total events
    """
    try:
        response = get_url(url, headers)
    except RetryError:
        # Try again with rows set to 100
        url = re.sub('rows=[0-9]*', 'rows=100', url)
        response = get_url(url, headers)
    if response.status_code == 200:
        response_json = response.json()
        total_events = response_json['message']['total-results']
        events = response_json['message']['events']
        next_cursor = response_json['message']['next-cursor']
        counter = len(events)

        # check for valid occurred at timestamp
        for i, event in enumerate(events):
            try:
                pendulum.parse(event['occurred_at'])
            except ValueError:
                event['occurred_at'] = "0001-01-01T00:00:00Z"
            # transform keys in dict
            events[i] = change_keys(event, convert)

        # append events and cursor
        if events:
            with open(events_path, 'a') as f:
                with jsonlines.Writer(f) as writer:
                    writer.write_all(events)
        if next_cursor:
            with open(cursor_path, 'a') as f:
                f.write(next_cursor + '\n')
        return next_cursor, counter, total_events

    else:
        raise ConnectionError(f"Error requesting url: {url}, response: {response.text}")


def parse_event_url(url: str) -> (str, str):
    event_type = url.split('?mailto')[0].split('/')[-1]
    if event_type == 'events':
        date = url.split('from-collected-date=')[1].split('&')[0]
    else:
        date = url.split('from-updated-date=')[1].split('&')[0]

    return event_type, date


def change_keys(obj, convert):
    """
    Recursively goes through the dictionary obj and updates keys with the convert function.
    :param obj: The dictionary
    :param convert: The convert function that is used to update the key
    :return: The updated dictionary
    """
    if isinstance(obj, (str, int, float)):
        return obj
    if isinstance(obj, dict):
        new = obj.__class__()
        for k, v in obj.items():
            if isinstance(v, int) and k != "total":
                v = str(v)
            new[convert(k)] = change_keys(v, convert)
        return new
