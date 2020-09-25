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

import fileinput
import json
import logging
import math
import os
import pathlib
import pendulum
from datetime import datetime, timedelta
from multiprocessing import cpu_count
from requests.exceptions import RetryError
from typing import Tuple, Union

from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.models.taskinstance import TaskInstance
from concurrent.futures import ThreadPoolExecutor, as_completed
from croniter import croniter_range
from google.cloud import bigquery
from google.cloud.bigquery import SourceFormat

from observatory_platform.utils.config_utils import (AirflowVar,
                                                     SubFolder,
                                                     check_variables,
                                                     find_schema,
                                                     schema_path,
                                                     telescope_path)
from observatory_platform.utils.gc_utils import (create_bigquery_dataset,
                                                 load_bigquery_table,
                                                 upload_file_to_cloud_storage)
from observatory_platform.utils.url_utils import retry_session
from tests.observatory_platform.config import test_fixtures_path


def extract_events(url: str, events_path: str, next_cursor: str = None, success: bool = True) -> Tuple[bool,
                                                                                                       Union[str, None]]:

    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/39.0.2171.95 Safari/537.36'
    }
    tmp_url = url + f'&cursor={next_cursor}' if next_cursor else url
    try:
        session = retry_session(num_retries=10, backoff_factor=1.0).get(tmp_url, headers=headers)
        #TODO prevent unclosed socket warning
        # # close session to prevent unclosed socket warning
        # session.close()
    except RetryError:
        return False, next_cursor
    if session.status_code == 200:
        response_dict = json.loads(session.text)
        events = response_dict['message']['events']
        next_cursor = response_dict['message']['next-cursor']
        # write events so far
        with open(events_path, 'a') as f:
            f.write(json.dumps(events) + '\n')
        if next_cursor:
            success, next_cursor = extract_events(url, events_path, next_cursor, success)
            return success, next_cursor
        else:
            return True, None
    else:
        raise ConnectionError(f"Error requesting url: {url}, response: {session.text}")


def list_batch_dates(release: 'CrossrefEventsRelease'):
    # number of days between start and end
    total_no_days = (release.end_date - release.start_date).days
    #TODO check that works when batch no days is 1 (e.g. only 2 days and 6 cpu)
    # number of days in each batch, minimal is 1
    batch_no_days = math.ceil(total_no_days / CrossrefEventsTelescope.MAX_PROCESSES)

    # create batch start and end date
    batches = []
    # transform to datetime objects
    start_date = datetime.fromtimestamp(release.start_date.timestamp())
    end_date = datetime.fromtimestamp(release.end_date.timestamp())
    days = 0
    batch_start_date = start_date
    for dt in croniter_range(start_date, end_date, f"0 0 * * *"):
        days += 1
        # add start and end date every x no of days (where x = batch_no_days)
        if days % batch_no_days == 0:
            batch_end_date = dt
            batches.append((batch_start_date, batch_end_date))
            # end date is included, so start date should be one day later
            batch_start_date = batch_end_date + timedelta(days=1)
    # check if final batch has to be added
    if batch_start_date < end_date:
        batches.append((batch_start_date, end_date))
    return batches


def download_events_batch(release: 'CrossrefEventsRelease', start_date, end_date) -> list:
    batch_results = []
    # Extract all new, edited and deleted events
    for event_type, url in release.urls.items():
        start_date_str = start_date.strftime("%Y-%m-%d")
        end_date_str = end_date.strftime("%Y-%m-%d")
        if event_type == 'events':
            url = url.format(start_date=start_date_str, end_date=end_date_str)
        else:
            url = url.format(start_date=start_date_str)

        logging.info(f"Downloading from url: {url}")

        events_path = release.download_batch_path(event_type, start_date_str)
        cursor_path = release.cursor_path(event_type, start_date_str)
        # check if cursor files exist from a previous failed request
        if os.path.isfile(cursor_path):
            # retrieve cursor
            with open(cursor_path, 'r') as f:
                next_cursor = json.load(f)
                # delete file
                pathlib.Path(cursor_path).unlink()
            # extract events
            success, next_cursor = extract_events(url, events_path, next_cursor)
        # if events path exists but no cursor file previous request has finished & successful
        elif os.path.isfile(events_path):
            success = True
            next_cursor = None
        # first time request
        else:
            # extract events
            success, next_cursor = extract_events(url, events_path)

        if not success:
            with open(cursor_path, 'w') as f:
                json.dump(next_cursor, f)
        batch_results.append((events_path, success))
        logging.info(f'Extracted events from url {url}, successful: {success}')

    return batch_results


def download_release(release: 'CrossrefEventsRelease'):
    all_results = []

    if CrossrefEventsTelescope.DOWNLOAD_MODE == 'parallel':
        logging.info('Downloading events with parallel method')

        batch_dates = list_batch_dates(release)
        no_workers = CrossrefEventsTelescope.MAX_PROCESSES
        with ThreadPoolExecutor(max_workers=no_workers) as executor:
            futures = []
            for i in range(no_workers):
                start_date = batch_dates[i][0]
                end_date = batch_dates[i][1]
                futures.append(executor.submit(download_events_batch, release, start_date, end_date))
            for future in as_completed(futures):
                batch_result = future.result()
                all_results += batch_result

    elif CrossrefEventsTelescope.DOWNLOAD_MODE == 'sequential':
        logging.info('Downloading events with sequential method')
        all_results = download_events_batch(release, release.start_date, release.end_date)
    else:
        raise AirflowException(f'Download mode has to be either "sequential" or "parallel", '
                               f'not "{CrossrefEventsTelescope.DOWNLOAD_MODE}"')

    # get paths of failed request attempts
    failed_files = [result[0] for result in all_results if not result[1]]
    if failed_files:
        raise AirflowException(f'Max retries exceeded for file(s): '
                               f'{", ".join(failed_files)}, saved cursor to corresponding file.')

    batch_files = [result[0] for result in all_results]
    # combine all event types for all batches to one file
    continue_dag = False
    with open(release.download_path, 'w') as fout, fileinput.input(batch_files) as fin:
        for line in fin:
            if line != '[]\n':
                fout.write(line)
                continue_dag = True

    # delete all individual event files
    for file_path in batch_files:
        pathlib.Path(file_path).unlink()

    # only continue if file is not empty
    return continue_dag


def convert(k):
    return k.replace('-', '_')


def change_keys(obj, convert):
    """
    Recursively goes through the dictionary obj and replaces keys with the convert function.
    """
    if isinstance(obj, (str, int, float)):
        return obj
    if isinstance(obj, dict):
        new = obj.__class__()
        for k, v in obj.items():
            new[convert(k)] = change_keys(v, convert)
    elif isinstance(obj, (list, set, tuple)):
        new = obj.__class__(change_keys(v, convert) for v in obj)
    else:
        return obj
    return new


class CrossrefEventsRelease:
    """ Used to store info on a given crossref release """

    def __init__(self, start_date: datetime, end_date: datetime):
        self.start_date = start_date
        self.end_date = end_date

        self.urls = {'events': CrossrefEventsTelescope.EVENTS_URL, 'edited': CrossrefEventsTelescope.EDITED_URL,
                     'deleted': CrossrefEventsTelescope.DELETED_URL}

    @property
    def download_path(self):
        #TODO fix up paths, create subdir for each release
        return self.get_path(SubFolder.downloaded, CrossrefEventsTelescope.DAG_ID)

    def download_batch_path(self, event_type: str, batch_start: str):
        return self.get_path(SubFolder.downloaded, f'{event_type}_{batch_start}')

    def cursor_path(self, event_type: str, batch_start: str):
        return self.get_path(SubFolder.downloaded, f'{event_type}_{batch_start}_cursor')

    @property
    def transform_path(self):
        return self.get_path(SubFolder.transformed, CrossrefEventsTelescope.DAG_ID)

    def get_path(self, sub_folder: SubFolder, name: str) -> str:
        """ Gets complete path of file for download/transform directory or file.

        :param sub_folder: name of subfolder
        :param name: partial file name
        :return: the path.
        """

        date_str = self.start_date.strftime("%Y_%m_%d") + "-" + self.end_date.strftime("%Y_%m_%d")

        file_name = f"{name}_{date_str}.json"

        path = os.path.join(telescope_path(sub_folder, CrossrefEventsTelescope.DAG_ID), file_name)
        return path

    def get_blob_name(self, sub_folder: SubFolder) -> str:
        """ Gives blob name that is used to determine path inside storage bucket

        :param sub_folder: name of subfolder
        :return: blob name
        """

        file_name = os.path.basename(self.get_path(sub_folder, CrossrefEventsTelescope.DAG_ID))
        blob_name = f'telescopes/{CrossrefEventsTelescope.DAG_ID}/{file_name}'

        return blob_name


def pull_release(ti: TaskInstance) -> CrossrefEventsRelease:
    """ Pull a CrossrefMetadataRelease instance with xcom.

    :param ti: the Apache Airflow task instance.
    :return: the list of CrossrefMetadataRelease instances.
    """

    return ti.xcom_pull(key=CrossrefEventsTelescope.RELEASES_TOPIC_NAME,
                        task_ids=CrossrefEventsTelescope.TASK_ID_DOWNLOAD, include_prior_dates=False)


class CrossrefEventsTelescope:
    """ A container for holding the constants and static functions for the crossref metadata telescope. """

    DAG_ID = 'crossref_events'
    DATASET_ID = 'crossref'
    DESCRIPTION = 'The Crossref Metadata Plus dataset: ' \
                  'https://www.crossref.org/services/metadata-retrieval/metadata-plus/'
    RELEASES_TOPIC_NAME = "releases"
    QUEUE = 'remote_queue'
    MAX_PROCESSES = cpu_count()
    MAX_CONNECTIONS = cpu_count()
    MAX_RETRIES = 3
    DOWNLOAD_MODE = 'parallel'  # Valid options: ['sequential', 'parallel']

    MAILTO = 'aniek.roelofs@curtin.edu.au'
    #TODO remove source reddit
    EVENTS_URL = f'https://api.eventdata.crossref.org/v1/events?mailto={MAILTO}&source=reddit&from-collected-date={{' \
                 f'start_date}}&until-collected-date={{end_date}}&rows=10000'
    #TODO get info in false 'until-updated-date' field
    # EDITED_URL = f'https://api.eventdata.crossref.org/v1/events/edited?mailto={MAILTO}&from-updated-date={{' \
    #              f'start_date}}&until-updated-date={{end_date}}&rows=10000'
    EDITED_URL = f'https://api.eventdata.crossref.org/v1/events/edited?mailto={MAILTO}&from-updated-date={{' \
                 f'start_date}}&rows=10000'
    DELETED_URL = f'https://api.eventdata.crossref.org/v1/events/deleted?mailto={MAILTO}&from-updated-date={{' \
                  f'start_date}}&rows=10000'
    DEBUG_FILE_PATH = os.path.join(test_fixtures_path(), 'telescopes', 'crossref_metadata.json.tar.gz')

    TASK_ID_CHECK_DEPENDENCIES = "check_dependencies"
    TASK_ID_CHECK_RELEASE = f"check_release"
    TASK_ID_DOWNLOAD = f"download"
    TASK_ID_UPLOAD_DOWNLOADED = 'upload_downloaded'
    TASK_ID_EXTRACT = f"extract"
    TASK_ID_TRANSFORM = f"transform_releases"
    TASK_ID_UPLOAD_TRANSFORMED = 'upload_transformed'
    TASK_ID_BQ_LOAD = f"bq_load"
    TASK_ID_CLEANUP = f"cleanup"

    @staticmethod
    def check_dependencies():
        """ Check that all variables exist that are required to run the DAG.
        :return: None.
        """

        vars_valid = check_variables(AirflowVar.data_path.get(), AirflowVar.project_id.get(),
                                     AirflowVar.data_location.get(), AirflowVar.download_bucket_name.get(),
                                     AirflowVar.transform_bucket_name.get())

        if not vars_valid:
            raise AirflowException('Required variables or connections are missing')

    @staticmethod
    def download(**kwargs):
        ti: TaskInstance = kwargs['ti']

        prev_start_date = kwargs['prev_start_date_success']
        # if DAG is run for first time, set to start date of this DAG (note: different than start date of DAG run)
        if not prev_start_date:
            prev_start_date = kwargs['dag'].default_args['start_date']
        start_date = pendulum.instance(kwargs['dag_run'].start_date)

        release = CrossrefEventsRelease(prev_start_date, start_date)
        ti.xcom_push(CrossrefEventsTelescope.RELEASES_TOPIC_NAME, release)

        download_release(release)

    @staticmethod
    def upload_downloaded(**kwargs):
        """ Upload the downloaded files to a Google Cloud Storage bucket.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        # Pull release
        ti: TaskInstance = kwargs['ti']
        release = pull_release(ti)

        # Get variables
        bucket_name = Variable.get(AirflowVar.download_bucket_name.get())

        # Upload each release
        upload_file_to_cloud_storage(bucket_name, release.get_blob_name(SubFolder.downloaded),
                                     file_path=release.download_path)

    @staticmethod
    def transform(**kwargs):
        """ Transform release with sed command and save to new file.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        # Pull releases
        ti: TaskInstance = kwargs['ti']
        release = pull_release(ti)

        # load events
        events = []
        with open(release.download_path, 'r') as f:
            for line in f:
                events += json.loads(line)

        # transform release
        with open(release.transform_path, 'w') as f:
            for event in events:
                event_updated = change_keys(event, convert)
                f.write(json.dumps(event_updated) + '\n')

    @staticmethod
    def upload_transformed(**kwargs):
        """ Upload transformed release to a Google Cloud Storage bucket.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        # Pull releases
        ti: TaskInstance = kwargs['ti']
        release = pull_release(ti)

        # Get variables
        bucket_name = Variable.get(AirflowVar.transform_bucket_name.get())

        # Upload files
        logging.info(f'Uploading transformed file')
        success = upload_file_to_cloud_storage(bucket_name, release.get_blob_name(SubFolder.transformed),
                                               file_path=release.transform_path)
        if success:
            logging.info(f'upload_transformed success: {release}')
        else:
            logging.error(f"upload_transformed error: {release}")
            exit(os.EX_DATAERR)

    @staticmethod
    def bq_load(**kwargs):
        """ Upload transformed release to a bigquery table.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        # Pull releases
        ti: TaskInstance = kwargs['ti']
        release = pull_release(ti)

        # Get variables
        project_id = Variable.get(AirflowVar.project_id.get())
        data_location = Variable.get(AirflowVar.data_location.get())
        bucket_name = Variable.get(AirflowVar.transform_bucket_name.get())

        # Create dataset
        dataset_id = CrossrefEventsTelescope.DATASET_ID
        create_bigquery_dataset(project_id, dataset_id, data_location, CrossrefEventsTelescope.DESCRIPTION)

        # Select schema file based on release date
        analysis_schema_path = schema_path('telescopes')
        release_date = pendulum.instance(release.end_date)
        schema_file_path = find_schema(analysis_schema_path, CrossrefEventsTelescope.DAG_ID, release_date)
        if schema_file_path is None:
            logging.error(f'No schema found with search parameters: analysis_schema_path={analysis_schema_path}, '
                          f'table_name={CrossrefEventsTelescope.DAG_ID}, release_date={release_date}')
            exit(os.EX_CONFIG)

        # Load BigQuery table
        uri = f"gs://{bucket_name}/{release.get_blob_name(SubFolder.transformed)}"
        logging.info(f"URI: {uri}")

        load_bigquery_table(uri, dataset_id, data_location, CrossrefEventsTelescope.DAG_ID, schema_file_path,
                            SourceFormat.NEWLINE_DELIMITED_JSON, partition=True, partition_field='timestamp',
                            partition_type=bigquery.TimePartitioningType.DAY, require_partition_filter=True,
                            write_disposition=bigquery.WriteDisposition.WRITE_APPEND)

    @staticmethod
    def cleanup(**kwargs):
        """ Delete files of downloaded, extracted and transformed release.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        # Pull releases
        ti: TaskInstance = kwargs['ti']
        release = pull_release(ti)

        # Delete files for the release
        try:
            pathlib.Path(release.download_path).unlink()
        except FileNotFoundError as e:
            logging.warning(f"No such file or directory {release.download_path}: {e}")

        try:
            pathlib.Path(release.transform_path).unlink()
        except FileNotFoundError as e:
            logging.warning(f"No such file or directory {release.transform_path}: {e}")
