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
import os
import pathlib
import shutil
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import timedelta
from multiprocessing import cpu_count
from typing import Tuple, Union

import pendulum
from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.models.taskinstance import TaskInstance
from google.cloud import bigquery
from google.cloud.bigquery import SourceFormat
from requests.exceptions import RetryError

from observatory_platform.utils.config_utils import (AirflowVar,
                                                     SubFolder,
                                                     check_variables,
                                                     find_schema,
                                                     schema_path,
                                                     telescope_path,
                                                     telescope_templates_path)
from observatory_platform.utils.gc_utils import (bigquery_partitioned_table_id,
                                                 create_bigquery_dataset,
                                                 load_bigquery_table,
                                                 run_bigquery_query,
                                                 upload_file_to_cloud_storage)
from observatory_platform.utils.jinja2_utils import (make_sql_jinja2_filename, render_template)
from observatory_platform.utils.url_utils import retry_session


def extract_events(url: str, events_path: str, next_cursor: str = None, success: bool = True) -> \
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
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/39.0.2171.95 Safari/537.36'
    }
    tmp_url = url + f'&cursor={next_cursor}' if next_cursor else url
    try:
        response = retry_session(num_retries=15, backoff_factor=0.5).get(tmp_url, headers=headers)
    except RetryError:
        return False, next_cursor, None
    if response.status_code == 200:
        response_dict = json.loads(response.text)
        total_events = response_dict['message']['total-results']
        events = response_dict['message']['events']
        next_cursor = response_dict['message']['next-cursor']
        # write events so far
        with open(events_path, 'a') as f:
            f.write(json.dumps(events) + '\n')
        if next_cursor:
            success, next_cursor, total_events = extract_events(url, events_path, next_cursor, success)
            return success, next_cursor, total_events
        else:
            return True, None, total_events
    else:
        raise ConnectionError(f"Error requesting url: {url}, response: {response.text}")


def download_events_batch(release: 'CrossrefEventsRelease', i: int) -> list:
    """
    Download one batch (time period) of events.

    :param release: The crossref events release
    :param i: The batch number
    :return: A list of batch results with a tuple of (events_path, success). These describe the path to the events
    file and whether the events were extracted successfully
    """
    batch_results = []
    # Extract all new, edited and deleted events
    for j, url in enumerate(release.urls[i]):
        logging.info(f"{i + 1}.{j + 1} Downloading from url: {url}")
        events_path = release.batch_path(url)
        cursor_path = release.batch_path(url, cursor=True)
        # check if cursor files exist from a previous failed request
        if os.path.isfile(cursor_path):
            # retrieve cursor
            with open(cursor_path, 'r') as f:
                next_cursor = json.load(f)
                # delete file
                pathlib.Path(cursor_path).unlink()
            # extract events
            success, next_cursor, total_events = extract_events(url, events_path, next_cursor)
        # if events path exists but no cursor file previous request has finished & successful
        elif os.path.isfile(events_path):
            success = True
            next_cursor = None
            total_events = 'See previous successful attempt'
        # first time request
        else:
            # extract events
            success, next_cursor, total_events = extract_events(url, events_path)

        if not success:
            with open(cursor_path, 'w') as f:
                json.dump(next_cursor, f)
        batch_results.append((events_path, success))
        logging.info(f'{i + 1}.{j + 1} successful: {success}, number of events: {total_events}')

    return batch_results


def download_release(release: 'CrossrefEventsRelease') -> bool:
    """
    Download one release of crossref events, this is from the start date of the previous successful DAG until the
    start date of this DAG. The release can be split up in periods (multiple batches), if the download mode is set to
    'parallel'.

    :param release: The crossref events release
    :return: Boolean whether to continue DAG. Continue DAG is True if the events file is not empty.
    """
    all_results = []

    if CrossrefEventsTelescope.DOWNLOAD_MODE == 'parallel':
        no_workers = CrossrefEventsTelescope.MAX_PROCESSES
        logging.info(f'Downloading events with parallel method, no. workers: {no_workers}')

    else:
        logging.info('Downloading events with sequential method')
        no_workers = 1

    with ThreadPoolExecutor(max_workers=no_workers) as executor:
        futures = []
        # select minimum, either no of batches or no of workers
        for i in range(min(no_workers, len(release.urls))):
            futures.append(executor.submit(download_events_batch, release, i))
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
    with open(release.download_path, 'w') as fout, fileinput.input(batch_files) as fin:
        for line in fin:
            if line != '[]\n':
                fout.write(line)
                continue_dag = True

    # only continue if file is not empty
    return continue_dag


def transform_release(release: 'CrossrefEventsRelease'):
    """
    Transforms the crossref events release. The download file contains multiple lists, one for each request,
    and each list contains multiple events. Each event is transformed so that the field names do not contain
    '-' and have a valid timestamp at 'occurred_at'. The events are written out individually and separated by a newline.

    :param release: The crossref events release
    :return: None
    """
    # load events
    events = []
    with open(release.download_path, 'r') as f:
        for line in f:
            events += json.loads(line)

    # transform release
    with open(release.transform_path, 'w') as f:
        for event in events:
            try:
                pendulum.parse(event['occurred_at'])
            except ValueError:
                event['occurred_at'] = "0001-01-01T00:00:00Z"
            event_updated = change_keys(event, convert)
            f.write(json.dumps(event_updated) + '\n')


def convert(k: str) -> str:
    """
    Replaces '-' with '_'

    :param k: Dictionary key
    :return: Updated dictionary key
    """
    return k.replace('-', '_')


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
    elif isinstance(obj, (list, set, tuple)):
        new = obj.__class__(change_keys(v, convert) for v in obj)
    else:
        return obj
    return new


class CrossrefEventsRelease:
    """ Used to store info on a given crossref events release """

    def __init__(self, start_date: pendulum.Pendulum, end_date: pendulum.Pendulum, first_release: bool = False):
        """
        :param start_date: Start date of this release
        :param end_date: End date of this release
        :param first_release: Whether this is the first release that is downloaded (if so, no edited/deleted events
        need to be obtained)
        """
        self.start_date = start_date
        self.end_date = end_date
        self.first_release = first_release

        if CrossrefEventsTelescope.DOWNLOAD_MODE == 'parallel':
            batches = self.batch_dates
        elif CrossrefEventsTelescope.DOWNLOAD_MODE == 'sequential':
            batches = [(start_date, end_date)]
        else:
            raise AirflowException(f'Download mode has to be either "sequential" or "parallel", '
                                   f'not "{CrossrefEventsTelescope.DOWNLOAD_MODE}"')

        self.urls = []
        edited_deleted = False
        for batch in batches:
            start_date = batch[0]
            end_date = batch[1]
            event_type_urls = [CrossrefEventsTelescope.EVENTS_URL.format(start_date=start_date, end_date=end_date)]
            # only add edited/deleted urls to first batch, since no end date can be specified
            if edited_deleted or first_release:
                pass
            else:
                event_type_urls.append(CrossrefEventsTelescope.EDITED_URL.format(start_date=start_date))
                event_type_urls.append(CrossrefEventsTelescope.DELETED_URL.format(start_date=start_date))
                edited_deleted = True
            self.urls.append(event_type_urls)

    @property
    def batch_dates(self) -> list:
        """
        Create batches of time periods, based on the start and end date of the release and the max number of
        processes available.

        :return: List of batches, where each batch is a tuple of (start_date, end_date). Both dates are strings in
        format YYYY-MM-DD
        """
        start_date = self.start_date.date()
        end_date = self.end_date.date()
        max_processes = CrossrefEventsTelescope.MAX_PROCESSES

        # number of days between start and end, add 1, because end date is included
        total_no_days = (end_date - start_date).days + 1

        # number of days in each batch, rounded down, minimal is 1
        batch_no_days = max(1, round(total_no_days / max_processes))

        batches = []
        batch_start_date = start_date
        no_batches = total_no_days if total_no_days < max_processes else max_processes
        # subtract one for possible final batch
        for x in range(no_batches - 1):
            batch_end_date = batch_start_date + timedelta(days=batch_no_days - 1)
            batches.append((batch_start_date.strftime("%Y-%m-%d"), batch_end_date.strftime("%Y-%m-%d")))
            # end date is included, so start date should be one day later
            batch_start_date = batch_end_date + timedelta(days=1)

        # check if final batch has to be added
        if batch_start_date <= end_date:
            batches.append((batch_start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")))
        return batches

    @property
    def download_path(self) -> str:
        """
        :return: The file path for the downloaded crossref events
        """
        return self.get_path(SubFolder.downloaded, CrossrefEventsTelescope.DAG_ID)

    @property
    def transform_path(self) -> str:
        """
        :return: The file path for the transformed crossref events
        """
        return self.get_path(SubFolder.transformed, CrossrefEventsTelescope.DAG_ID)

    def batch_path(self, url, cursor: bool = False) -> str:
        """
        Gets the appropriate file path for a single batch, either for an events or cursor file.

        :param url: The url used for a specific batch
        :param cursor: Whether this is a cursor file or file with actual events
        :return: Path to the events or cursor file
        """
        event_type = url.split('?mailto')[0].split('/')[-1]
        if event_type == 'events':
            batch_start = url.split('from-collected-date=')[1].split('&')[0]
            batch_end = url.split('until-collected-date=')[1].split('&')[0]
        else:
            batch_start = self.start_date.strftime("%Y-%m-%d")
            batch_end = self.end_date.strftime("%Y-%m-%d")

        if cursor:
            return self.get_path(SubFolder.downloaded, f'{event_type}_{batch_start}_{batch_end}_cursor')
        else:
            return self.get_path(SubFolder.downloaded, f'{event_type}_{batch_start}_{batch_end}')

    def subdir(self, sub_folder: SubFolder):
        """
        Path to subdirectory of a specific release for either downloaded/transformed files.

        :param sub_folder:
        :return:
        """
        date_str = self.start_date.strftime("%Y_%m_%d") + "-" + self.end_date.strftime("%Y_%m_%d")
        return os.path.join(telescope_path(sub_folder, CrossrefEventsTelescope.DAG_ID), date_str)

    def get_path(self, sub_folder: SubFolder, name: str) -> str:
        """
        Gets path to json file based on subfolder and name. Will also create the subfolder if it doesn't exist yet.

        :param sub_folder: Name of the subfolder
        :param name: File base name, without extension
        :return: The file path.
        """

        release_subdir = self.subdir(sub_folder)
        if not os.path.exists(release_subdir):
            os.makedirs(release_subdir, exist_ok=True)

        file_name = f"{name}.json"

        path = os.path.join(release_subdir, file_name)
        return path

    @property
    def blob_name(self) -> str:
        """
        Returns blob name that is used to determine path inside google cloud storage bucket

        :return: The blob name
        """
        date_str = self.start_date.strftime("%Y_%m_%d") + "-" + self.end_date.strftime("%Y_%m_%d")

        file_name = f"{CrossrefEventsTelescope.DAG_ID}_{date_str}.json"
        blob_name = f'telescopes/{CrossrefEventsTelescope.DAG_ID}/{file_name}'

        return blob_name


def pull_release(ti: TaskInstance) -> CrossrefEventsRelease:
    """
    Pull a CrossrefEventsRelease instance with xcom.

    :param ti: the Apache Airflow task instance.
    :return: the CrossrefEventsRelease instance.
    """

    return ti.xcom_pull(key=CrossrefEventsTelescope.RELEASES_TOPIC_NAME,
                        task_ids=CrossrefEventsTelescope.TASK_ID_DOWNLOAD, include_prior_dates=False)


class CrossrefEventsTelescope:
    """ A container for holding the constants and static functions for the crossref events telescope. """

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
    EVENTS_URL = f'https://api.eventdata.crossref.org/v1/events?mailto={MAILTO}&from-collected-date={{' \
                 f'start_date}}&until-collected-date={{end_date}}&rows=10000'
    # TODO get info in false 'until-updated-date' field
    # EDITED_URL = f'https://api.eventdata.crossref.org/v1/events/edited?mailto={MAILTO}&from-updated-date={{' \
    #              f'start_date}}&until-updated-date={{end_date}}&rows=10000'
    EDITED_URL = f'https://api.eventdata.crossref.org/v1/events/edited?mailto={MAILTO}&from-updated-date={{' \
                 f'start_date}}&rows=10000'
    DELETED_URL = f'https://api.eventdata.crossref.org/v1/events/deleted?mailto={MAILTO}&from-updated-date={{' \
                  f'start_date}}&rows=10000'

    TASK_ID_CHECK_DEPENDENCIES = "check_dependencies"
    TASK_ID_CHECK_RELEASE = "check_release"
    TASK_ID_DOWNLOAD = "download"
    TASK_ID_UPLOAD_DOWNLOADED = 'upload_downloaded'
    TASK_ID_EXTRACT = "extract"
    TASK_ID_TRANSFORM = "transform_releases"
    TASK_ID_UPLOAD_TRANSFORMED = 'upload_transformed'
    TASK_ID_BQ_LOAD_SHARD = "bq_load_shard"
    TASK_ID_BQ_DELETE_OLD = "bq_delete_old"
    TASK_ID_BQ_APPEND_NEW = "bq_append_new"
    TASK_ID_CLEANUP = "cleanup"

    @staticmethod
    def check_dependencies():
        """
        Check that all variables exist that are required to run the DAG.

        :return: None.
        """

        vars_valid = check_variables(AirflowVar.data_path.get(), AirflowVar.project_id.get(),
                                     AirflowVar.data_location.get(), AirflowVar.download_bucket_name.get(),
                                     AirflowVar.transform_bucket_name.get())

        if not vars_valid:
            raise AirflowException('Required variables or connections are missing')

    @staticmethod
    def download(**kwargs):
        """
        Download the crossref events release. The start date of this release is set to the DAG run start date of the
        previous successful run, the end date of this release is set to the start date of this DAG run minus 1 day.
        One day is subtracted from the end date, because the day has not finished yet so all events of that day can not
        be collected on the same day.
        If this is the first time a release is obtained, the start date will be set to the start date in the
        default_args of this DAG.

        This function is used for a shortcircuitoperator, so it will return a boolean value which determines whether
        the DAG will be continued or not.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: Boolean whether to continue DAG or not
        """
        ti: TaskInstance = kwargs['ti']

        prev_start_date = kwargs['prev_start_date_success']
        # if DAG is run for first time, set to start date of this DAG (note: different than start date of DAG run)
        if prev_start_date:
            first_release = False
        else:
            first_release = True
            prev_start_date = kwargs['dag'].default_args['start_date']
        start_date = pendulum.instance(kwargs['dag_run'].start_date) - timedelta(days=1)

        logging.info(f'Start date: {prev_start_date}, end date:{start_date}, first release: {first_release}')
        if prev_start_date < start_date:
            raise AirflowException("Start date has to be before end date.")

        release = CrossrefEventsRelease(prev_start_date, start_date, first_release)
        ti.xcom_push(CrossrefEventsTelescope.RELEASES_TOPIC_NAME, release)

        continue_dag = download_release(release)
        return continue_dag

    @staticmethod
    def upload_downloaded(**kwargs):
        """
        Upload the downloaded events file to a Google Cloud Storage bucket.

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
        upload_file_to_cloud_storage(bucket_name, release.blob_name, file_path=release.download_path)

    @staticmethod
    def transform(**kwargs):
        """
        Transform the downloaded events and save to a new file.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        # Pull release
        ti: TaskInstance = kwargs['ti']
        release = pull_release(ti)

        transform_release(release)

    @staticmethod
    def upload_transformed(**kwargs):
        """
        Upload the transformed events file to a Google Cloud Storage bucket.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        # Pull release
        ti: TaskInstance = kwargs['ti']
        release = pull_release(ti)

        # Get variables
        bucket_name = Variable.get(AirflowVar.transform_bucket_name.get())

        # Upload files
        logging.info(f'Uploading transformed file')
        success = upload_file_to_cloud_storage(bucket_name, release.blob_name, file_path=release.transform_path)
        if success:
            logging.info(f'upload_transformed success: {release}')
        else:
            logging.error(f"upload_transformed error: {release}")
            exit(os.EX_DATAERR)

    @staticmethod
    def bq_load_shard(**kwargs):
        """
        Create a table shard containing only events of this release. The date in the table name is based on the end
        date of this release.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None
        """
        # Pull release
        ti: TaskInstance = kwargs['ti']
        release = pull_release(ti)

        if release.first_release:
            logging.info('Skipped, because first release')
            return

        # Get variables
        data_location = Variable.get(AirflowVar.data_location.get())
        bucket_name = Variable.get(AirflowVar.transform_bucket_name.get())

        # Select schema file based on release date
        analysis_schema_path = schema_path('telescopes')
        release_date = pendulum.instance(release.end_date)
        schema_file_path = find_schema(analysis_schema_path, CrossrefEventsTelescope.DAG_ID, release_date)
        if schema_file_path is None:
            logging.error(f'No schema found with search parameters: analysis_schema_path={analysis_schema_path}, '
                          f'table_name={CrossrefEventsTelescope.DAG_ID}, release_date={release_date}')
            exit(os.EX_CONFIG)

        # Load BigQuery table
        uri = f"gs://{bucket_name}/{release.blob_name}"
        logging.info(f"URI: {uri}")

        # Create partition with events related to release
        table_id = bigquery_partitioned_table_id(CrossrefEventsTelescope.DAG_ID, release_date)

        # Create separate partitioned table
        dataset_id = CrossrefEventsTelescope.DATASET_ID
        load_bigquery_table(uri, dataset_id, data_location, table_id, schema_file_path,
                            SourceFormat.NEWLINE_DELIMITED_JSON)

    @staticmethod
    def bq_delete_old(**kwargs):
        """
        Run a BigQuery MERGE query, merging the sharded table of this release into the main table containing all
        events.
        The tables are matched on the 'id' field and if a match occurs, a check will be done to determine whether the
        'updated date' of the corresponding event in the main table either does not exist or is older than that of the
        event in the sharded table. If this is the case, the event will be deleted from the main table.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None
        """
        # Pull releases
        ti: TaskInstance = kwargs['ti']
        release = pull_release(ti)

        if release.first_release:
            logging.info('Skipped, because first release')
            return

        # Get merge variables
        dataset_id = CrossrefEventsTelescope.DATASET_ID
        release_date = pendulum.instance(release.end_date)
        main_table = CrossrefEventsTelescope.DAG_ID
        sharded_table = bigquery_partitioned_table_id(CrossrefEventsTelescope.DAG_ID, release_date)
        merge_condition_field = 'id'
        updated_date_field = 'updated_date'

        template_path = os.path.join(telescope_templates_path(), make_sql_jinja2_filename('merge_delete_matched'))
        query = render_template(template_path, dataset=dataset_id, main_table=main_table,
                                sharded_table=sharded_table, merge_condition_field=merge_condition_field,
                                updated_date_field=updated_date_field)
        run_bigquery_query(query)

    @staticmethod
    def bq_append_new(**kwargs):
        """
        All events from this release in the corresponding table shard will be appended to the main table.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        # Pull release
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
        uri = f"gs://{bucket_name}/{release.blob_name}"
        logging.info(f"URI: {uri}")

        # Append to current events table
        load_bigquery_table(uri, dataset_id, data_location, CrossrefEventsTelescope.DAG_ID, schema_file_path,
                            SourceFormat.NEWLINE_DELIMITED_JSON,
                            write_disposition=bigquery.WriteDisposition.WRITE_APPEND)

    @staticmethod
    def cleanup(**kwargs):
        """
        Delete subdirectories for downloaded and transformed events files.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        # Pull release
        ti: TaskInstance = kwargs['ti']
        release = pull_release(ti)

        try:
            print(release.subdir(SubFolder.downloaded))
            shutil.rmtree(release.subdir(SubFolder.downloaded))
        except FileNotFoundError as e:
            logging.warning(f"No such file or directory {release.subdir(SubFolder.downloaded)}: {e}")

        try:
            print(release.subdir(SubFolder.transformed))
            shutil.rmtree(release.subdir(SubFolder.transformed))
        except FileNotFoundError as e:
            logging.warning(f"No such file or directory {release.subdir(SubFolder.transformed)}: {e}")
