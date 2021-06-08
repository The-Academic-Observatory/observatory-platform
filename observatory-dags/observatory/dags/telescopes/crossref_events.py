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

import fileinput
import json
import logging
import os
import pathlib
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from typing import List, Tuple, Union

import pendulum
from airflow.exceptions import AirflowException
from airflow.models.taskinstance import TaskInstance
from observatory.platform.telescopes.stream_telescope import (StreamRelease, StreamTelescope)
from observatory.platform.utils.airflow_utils import AirflowVars
from observatory.platform.utils.telescope_utils import convert, list_to_jsonl_gz
from observatory.platform.utils.template_utils import upload_files_from_list
from observatory.platform.utils.url_utils import retry_session, get_ao_user_agent
from requests.exceptions import RetryError


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
        download_files_regex = r'crossref_events.json'
        transform_files_regex = r'crossref_events.jsonl.gz'
        super().__init__(dag_id, start_date, end_date, first_release, download_files_regex=download_files_regex,
                         transform_files_regex=transform_files_regex)
        self.mailto = mailto
        self.download_mode = download_mode
        self.max_processes = max_processes

    @property
    def download_path(self) -> str:
        """ Path to store the downloaded crossref events file"""
        return os.path.join(self.download_folder, 'crossref_events.json')

    # @property
    # def transform_path(self) -> str:
    #     """ Path to store the transformed crossref events file"""
    #     return os.path.join(self.transform_folder, 'crossref_events.jsonl.gz')

    # @property
    # def batch_dates(self) -> list:
    #     """ Create batches of time periods, based on the start and end date of the release and the max number of
    #     processes available.
    #     :return: List of batches, where each batch is a tuple of (start_date, end_date). Both dates are strings in
    #     format YYYY-MM-DD
    #     """
    #     if self.download_mode == 'sequential':
    #         return [(self.start_date.strftime("%Y-%m-%d"), self.end_date.strftime("%Y-%m-%d"))]
    #     elif self.download_mode == 'parallel':
    #         pass
    #     else:
    #         raise AirflowException(f'Download mode has to be either "sequential" or "parallel", '
    #                                f'not "{self.download_mode}"')
    #
    #     start_date = self.start_date.date()
    #     end_date = self.end_date.date()
    #
    #     # number of days between start and end, add 1, because end date is included
    #     total_no_days = (end_date - start_date).days + 1
    #
    #     # number of days in each batch, rounded to nearest integer, minimal is 1
    #     batch_no_days = max(1, round(total_no_days / self.max_processes))
    #
    #     batches = []
    #     period = pendulum.period(start_date, end_date)
    #     for i, dt in enumerate(period.range('days', batch_no_days)):
    #         batch_start_date = dt.strftime("%Y-%m-%d")
    #         # if final batch or end date is reached before end of period, use end date of release instead
    #         if i == (self.max_processes - 1) or dt == end_date:
    #             batch_end_date = end_date.strftime("%Y-%m-%d")
    #             batches.append((batch_start_date, batch_end_date))
    #             break
    #         # use end date of period
    #         else:
    #             # end date is included, so subtract 1 day from batch_no_days
    #             batch_end_date = (dt + timedelta(days=batch_no_days - 1)).strftime("%Y-%m-%d")
    #             batches.append((batch_start_date, batch_end_date))
    #
    #     return batches

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
            edited_url = f'https://api.eventdata.crossref.org/v1/events/edited?' \
                         f'mailto={self.mailto}&from-updated-date={start_date}' \
                         f'&until-updated-date={end_date}&rows=1000'
            deleted_url = f'https://api.eventdata.crossref.org/v1/events/deleted?' \
                          f'mailto={self.mailto}&from-updated-date={start_date}' \
                          f'&until-updated-date={end_date}&rows=1000'

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
        event_type = url.split('?mailto')[0].split('/')[-1]
        if event_type == 'events':
            batch_start = url.split('from-collected-date=')[1].split('&')[0]
            batch_end = url.split('until-collected-date=')[1].split('&')[0]
        else:
            batch_start = url.split('from-updated-date=')[1].split('&')[0]
            batch_end = url.split('until-updated-date=')[1].split('&')[0]

        if cursor:
            return os.path.join(self.download_folder, f'{event_type}_{batch_start}_{batch_end}_cursor.txt')
        else:
            return os.path.join(self.download_folder, f'{event_type}_{batch_start}_{batch_end}')

    def download(self) -> bool:
        """ Download one release of crossref events, this is from the start date of the previous successful DAG until
        the start date of this DAG. The release can be split up in periods (multiple batches), if the download mode is
        set to 'parallel'.

        :return:
        """
        all_results = []

        if self.download_mode == 'parallel':
            no_workers = self.max_processes
            logging.info(f'Downloading events with parallel method, no. workers: {no_workers}')
        else:
            logging.info('Downloading events with sequential method')
            no_workers = 1

        with ThreadPoolExecutor(max_workers=no_workers) as executor:
            futures = []
            # select minimum, either no of batches or no of workers
            for i in range(len(self.urls)):
                futures.append(executor.submit(self.download_transform_events, i))
            for future in as_completed(futures):
                batch_result = future.result()
                all_results += batch_result

        # get paths of failed request attempts
        failed_files = [result[0] for result in all_results if not result[1]]
        if failed_files:
            raise AirflowException(f'Max retries exceeded for file(s): '
                                   f'{", ".join(failed_files)}, saved cursor to corresponding file.')

        batch_files = [result[0] for result in all_results]
        # combine all event types for all batches to one file
        continue_dag = False
        with open(self.download_path, 'w') as fout, fileinput.input(batch_files) as fin:
            for line in fin:
                if line != '[]\n':
                    fout.write(line)
                    continue_dag = True

        # only continue if file is not empty
        return continue_dag

    def download_events_batch(self, i: int) -> list:
        """ Download one batch (time period) of events.

        :param i: The batch number
        :return: A list of batch results with a tuple of (events_path, success). These describe the path to the events
        file and whether the events were extracted successfully
        """
        batch_results = []
        # Extract all new, edited and deleted events
        for j, url in enumerate(self.urls[i]):
            logging.info(f"{i + 1}.{j + 1} Downloading from url: {url}")
            events_path = self.batch_path(url)
            cursor_path = self.batch_path(url, cursor=True)
            # check if cursor files exist from a previous failed request
            if os.path.isfile(cursor_path):
                pass
                # # retrieve cursor
                # with open(cursor_path, 'r') as f:
                #     next_cursor = f.read()
                # # # delete file
                # # pathlib.Path(cursor_path).unlink()
            # if events path exists but no cursor, previous request has finished & successful
            if os.path.isfile(events_path) and not next_cursor:
                continue

            if not success:
                with open(cursor_path, 'w') as f:
                    f.write(next_cursor)
            batch_results.append((events_path, success))
            logging.info(f'{i + 1}.{j + 1} successful: {success}, number of events: {total_events}')

        return batch_results

    def transform(self):
        """
        Transforms the crossref events release. The download file contains multiple lists, one for each request,
        and each list contains multiple events. Each event is transformed so that the field names do not contain '-'
        and have a valid timestamp at 'occurred_at'. The events are written out individually and separated by a newline.

        :return: None
        """
        # load and transform events
        events = []
        with open(self.download_path, 'r') as f:
            for line in f:
                for event in json.loads(line):
                    try:
                        pendulum.parse(event['occurred_at'])
                    except ValueError:
                        event['occurred_at'] = "0001-01-01T00:00:00Z"
                    transformed_event = change_keys(event, convert)
                    events.append(transformed_event)

        list_to_jsonl_gz(self.transform_path, events)


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

    # def upload_downloaded(self, release: CrossrefEventsRelease, **kwargs):
    #     """ Task to upload the downloadeded CrossrefEventsRelease release.
    #
    #     :param release: a CrossrefEventsRelease instance.
    #     :return: None.
    #     """
    #     # Upload each downloaded release
    #     upload_files_from_list(release.download_files, release.download_bucket)

    # def transform(self, release: CrossrefEventsRelease, **kwargs):
    #     release.transform()

def download_events(url: str, events_path: str, next_cursor: str = None, success: bool = True) -> \
        Tuple[bool, Union[str, None], Union[int, None]]:
    """
    Extract the events from the given url until no new cursor is returned or a RetryError occurs. The extracted events
    are appended to a json file, with 1 list per request.

    :param url: The crossref events api url
    :param events_path: Path to the file in which events are stored
    :param next_cursor: The next cursor, this is in the response of the api
    :param success: Whether all events were extracted successfully or an error occurred
    :return: success, next_cursor and number of total events
    """
    headers = {
        'User-Agent': get_ao_user_agent()
    }
    tmp_url = url + f'&cursor={next_cursor}' if next_cursor else url
    try:
        response = retry_session(num_retries=15, backoff_factor=0.5,
                                 status_forcelist=(500, 400)).get(tmp_url, headers=headers)
    except RetryError:
        return False, next_cursor, None
    if response.status_code == 200:
        response_json = response.json()
        total_events = response_json['message']['total-results']
        events = response_json['message']['events']
        next_cursor = response_json['message']['next-cursor']
        # write events so far
        with open(events_path, 'a') as f:
            f.write(json.dumps(events) + '\n')
        if next_cursor:
            success, next_cursor, total_events = download_events(url, events_path, next_cursor, success)
            return success, next_cursor, total_events
        else:
            return True, None, total_events
    else:
        raise ConnectionError(f"Error requesting url: {tmp_url}, response: {response.text}")


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
            new[convert(k)] = change_keys(v, convert)
        return new
