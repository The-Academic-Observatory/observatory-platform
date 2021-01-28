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
from typing import Tuple, Union

import pendulum
from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable
from airflow.models.taskinstance import TaskInstance
from google.cloud import bigquery
from google.cloud.bigquery import SourceFormat
from requests.exceptions import RetryError

from observatory.platform.utils.airflow_utils import AirflowVariable as Variable
from observatory.platform.utils.config_utils import (AirflowVars, AirflowConns, SubFolder, find_schema, telescope_path)
from observatory.platform.utils.config_utils import (SubFolder,
                                                     check_connections,
                                                     check_variables,
                                                     find_schema,
                                                     telescope_path)
from observatory.platform.utils.gc_utils import (bigquery_partitioned_table_id,
                                                 create_bigquery_dataset,
                                                 load_bigquery_table,
                                                 run_bigquery_query,
                                                 upload_file_to_cloud_storage)
from observatory.platform.utils.jinja2_utils import (make_sql_jinja2_filename, render_template)
from observatory.platform.utils.url_utils import retry_session
from observatory.dags.config import schema_path
from airflow.utils.dates import cron_presets
from croniter import croniter
import six
from datetime import datetime
from observatory.dags.config import workflow_sql_templates_path
import csv


def get_downloads_per_country(countries_url: str) -> Tuple[list, int]:
    print(countries_url)
    response = retry_session(num_retries=5).get(countries_url)
    response_content = response.content.decode('utf-8')
    if response_content == '\n':
        return [], 0
    response_csv = csv.DictReader(response_content.splitlines())
    results = []
    total_downloads = 0
    for row in response_csv:
        download_count = int(row['count'].strip('="'))
        country_code = row['value']
        country_name = row['description'].split('</span>')[0].split('>')[-1]
        results.append({'country_code': country_code,
                        'country_name': country_name,
                        'download_count': download_count})
        total_downloads += download_count

    return results, total_downloads


def download_release(release: 'UclDiscoveryRelease') -> bool:
    """
    Download one release of crossref events, this is from the start date of the previous successful DAG until the
    start date of this DAG. The release can be split up in periods (multiple batches), if the download mode is set to
    'parallel'.

    :param release: The crossref events release
    :return: Boolean whether to continue DAG. Continue DAG is True if the events file is not empty.
    """
    begin_date = release.start_date.strftime("%Y-%m-%d")
    end_date = release.end_date.strftime("%Y-%m-%d")

    print(release.list_ids_url)
    response = retry_session(num_retries=5).get(release.list_ids_url)
    if response.status_code == 200:
        response_content = response.content.decode('utf-8')
        response_csv = csv.DictReader(response_content.splitlines())
        if os.path.exists(release.download_path):
            os.remove(release.download_path)
        result = {}
        with open(release.download_path, 'a') as json_out:
            current_id = ''
            for row in response_csv:
                book_id = row['eprintid']
                print(book_id)
                if current_id == book_id:
                    current_id = book_id
                    continue
                downloads_per_country, total_downloads = get_downloads_per_country(release.countries_url+book_id)
                if total_downloads == 0:
                    current_id = book_id
                    continue
                result = {
                    'book_id': book_id,
                    'book_title': row['title'],
                    'begin_date': begin_date,
                    'end_date': end_date,
                    'total_downloads': total_downloads,
                    'downloads_per_country': downloads_per_country
                }
                json.dump(result, json_out)
                json_out.write('\n')
                current_id = book_id
        return True if result else False
    else:
        return False


class UclDiscoveryRelease:
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

        # self.url = UclDiscoveryTelescope.STATISTICS_URL.format(start_date=start_date.strftime("%Y%m%d"),
        #                                                        end_date=end_date.strftime("%Y%m%d"))
        self.list_ids_url = UclDiscoveryTelescope.LIST_IDS_URL.format(end_date=end_date.strftime("%Y"))
        self.countries_url = UclDiscoveryTelescope.COUNTRIES_URL.format(start_date=start_date.strftime("%Y%m%d"),
                                                                        end_date=end_date.strftime("%Y%m%d"))

    @property
    def download_path(self) -> str:
        """
        :return: The file path for the downloaded crossref events
        """
        return self.get_path(SubFolder.downloaded, UclDiscoveryTelescope.DAG_ID)

    def subdir(self, sub_folder: SubFolder):
        """
        Path to subdirectory of a specific release for either downloaded/transformed files.

        :param sub_folder:
        :return:
        """
        date_str = self.start_date.strftime("%Y_%m_%d") + "-" + self.end_date.strftime("%Y_%m_%d")
        return os.path.join(telescope_path(sub_folder, UclDiscoveryTelescope.DAG_ID), date_str)

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

        file_name = f"{UclDiscoveryTelescope.DAG_ID}_{date_str}.json"
        blob_name = f'telescopes/{UclDiscoveryTelescope.DAG_ID}/{file_name}'

        return blob_name


def pull_release(ti: TaskInstance) -> UclDiscoveryRelease:
    """
    Pull a CrossrefEventsRelease instance with xcom.

    :param ti: the Apache Airflow task instance.
    :return: the CrossrefEventsRelease instance.
    """

    return ti.xcom_pull(key=UclDiscoveryTelescope.RELEASES_TOPIC_NAME,
                        task_ids=UclDiscoveryTelescope.TASK_ID_DOWNLOAD, include_prior_dates=False)


def normalize_schedule_interval(schedule_interval: str):
    """
    Returns Normalized Schedule Interval. This is used internally by the Scheduler to
    schedule DAGs.
    1. Converts Cron Preset to a Cron Expression (e.g ``@monthly`` to ``0 0 1 * *``)
    2. If Schedule Interval is "@once" return "None"
    3. If not (1) or (2) returns schedule_interval
    """
    if isinstance(schedule_interval, six.string_types) and schedule_interval in cron_presets:
        _schedule_interval = cron_presets.get(schedule_interval)
    elif schedule_interval == '@once':
        _schedule_interval = None
    else:
        _schedule_interval = schedule_interval
    return _schedule_interval


class UclDiscoveryTelescope:
    """ A container for holding the constants and static functions for the crossref events telescope. """

    DAG_ID = 'ucl_discovery'
    DATASET_ID = 'ucl_discovery'
    DESCRIPTION = 'The Crossref Events dataset: https://www.eventdata.crossref.org/guide/'
    RELEASES_TOPIC_NAME = "releases"
    QUEUE = 'remote_queue'
    # max processes based on 7 days x 3 url categories
    MAX_PROCESSES = 21
    MAX_RETRIES = 3

    # STATISTICS_URL = 'https://discovery.ucl.ac.uk/cgi/stats/get?from={start_date}&to={end_date}&set_name=type&' \
    #                  'set_value=book&irs2report=main&datatype=downloads&top=eprint&view=Table' \
    #                  '&title_phrase=top_downloads&limit=all&export=CSV'
    LIST_IDS_URL = 'https://discovery.ucl.ac.uk/cgi/search/archive/advanced/export_discovery_CSV.csv?' \
                   'screen=Search&dataset=archive&_action_export=1&output=CSV' \
                   '&exp=0|1|-date/creators_name/title|archive|-|date:date:ALL:EQ:-{end_date}|primo:primo:ANY:EQ:open' \
                   '|type:type:ANY:EQ:book|-|eprint_status:eprint_status:ANY:EQ:archive' \
                   '|metadata_visibility:metadata_visibility:ANY:EQ:show'
    # DOWNLOADS_URL = 'https://discovery.ucl.ac.uk/cgi/stats/get?from={start_date}&to={end_date}&set_name=eprint' \
    #                 '&set_value={book_id}&irs2report=eprint&datatype=downloads&view=Table&limit=all&top=eprint&export=CSV'
    COUNTRIES_URL = 'https://discovery.ucl.ac.uk/cgi/stats/get?from={start_date}&to={end_date}&irs2report=eprint' \
                  '&datatype=countries&top=countries&view=Table&limit=all&set_name=eprint&export=CSV&set_value='


    TASK_ID_CHECK_DEPENDENCIES = "check_dependencies"
    TASK_ID_CHECK_RELEASE = "check_release"
    TASK_ID_DOWNLOAD = "download"
    TASK_ID_UPLOAD_DOWNLOADED = 'upload_downloaded'
    # TASK_ID_EXTRACT = "extract"
    # TASK_ID_TRANSFORM = "transform_releases"
    # TASK_ID_UPLOAD_TRANSFORMED = 'upload_transformed'
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

        vars_valid = check_variables(AirflowVars.DATA_PATH, AirflowVars.PROJECT_ID,
                                     AirflowVars.DATA_LOCATION, AirflowVars.DOWNLOAD_BUCKET,
                                     AirflowVars.TRANSFORM_BUCKET)
        if not vars_valid:
            raise AirflowException('Required variables are missing')

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
            # prev_start_date = kwargs['dag'].default_args['start_date']
        cron_schedule = normalize_schedule_interval(kwargs['dag'].schedule_interval)
        # start_date = pendulum.instance(kwargs['dag_run'].execution_date).subtract(months=1)
        start_date = pendulum.instance(kwargs['dag_run'].execution_date)
        cron_iter = croniter(cron_schedule, start_date)
        end_date = pendulum.instance(cron_iter.get_next(datetime))

        logging.info(f'Start date: {start_date}, end date:{end_date}, first release: {first_release}')
        # if prev_start_date > start_date:
        #     raise AirflowException("Start date has to be before end date.")

        release = UclDiscoveryRelease(start_date, end_date, first_release)
        ti.xcom_push(UclDiscoveryTelescope.RELEASES_TOPIC_NAME, release)

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
        bucket_name = Variable.get(AirflowVars.DOWNLOAD_BUCKET)

        # Upload each release
        upload_file_to_cloud_storage(bucket_name, release.blob_name, file_path=release.download_path)

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
        data_location = Variable.get(AirflowVars.DATA_LOCATION)
        bucket_name = Variable.get(AirflowVars.DOWNLOAD_BUCKET)

        # Select schema file based on release date
        analysis_schema_path = schema_path()
        release_date = pendulum.instance(release.end_date)
        schema_file_path = find_schema(analysis_schema_path, UclDiscoveryTelescope.DAG_ID, release_date)
        if schema_file_path is None:
            logging.error(f'No schema found with search parameters: analysis_schema_path={analysis_schema_path}, '
                          f'table_name={UclDiscoveryTelescope.DAG_ID}, release_date={release_date}')
            exit(os.EX_CONFIG)

        # Load BigQuery table
        uri = f"gs://{bucket_name}/{release.blob_name}"
        logging.info(f"URI: {uri}")

        # Create partition with events related to release
        table_id = bigquery_partitioned_table_id(UclDiscoveryTelescope.DAG_ID, release_date)

        # Create separate partitioned table
        dataset_id = UclDiscoveryTelescope.DATASET_ID
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
        dataset_id = UclDiscoveryTelescope.DATASET_ID
        release_date = pendulum.instance(release.end_date)
        main_table = UclDiscoveryTelescope.DAG_ID
        sharded_table = bigquery_partitioned_table_id(UclDiscoveryTelescope.DAG_ID, release_date)
        merge_condition_field = 'book_id'
        updated_date_field = 'end_date'

        template_path = os.path.join(workflow_sql_templates_path(), make_sql_jinja2_filename('merge_delete_matched'))
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
        project_id = Variable.get(AirflowVars.PROJECT_ID)
        data_location = Variable.get(AirflowVars.DATA_LOCATION)
        bucket_name = Variable.get(AirflowVars.DOWNLOAD_BUCKET)

        # Create dataset
        dataset_id = UclDiscoveryTelescope.DATASET_ID
        create_bigquery_dataset(project_id, dataset_id, data_location, UclDiscoveryTelescope.DESCRIPTION)

        # Select schema file based on release date
        analysis_schema_path = schema_path()
        release_date = pendulum.instance(release.end_date)
        schema_file_path = find_schema(analysis_schema_path, UclDiscoveryTelescope.DAG_ID, release_date)
        if schema_file_path is None:
            logging.error(f'No schema found with search parameters: analysis_schema_path={analysis_schema_path}, '
                          f'table_name={UclDiscoveryTelescope.DAG_ID}, release_date={release_date}')
            exit(os.EX_CONFIG)

        # Load BigQuery table
        uri = f"gs://{bucket_name}/{release.blob_name}"
        logging.info(f"URI: {uri}")

        # Append to current events table
        load_bigquery_table(uri, dataset_id, data_location, UclDiscoveryTelescope.DAG_ID, schema_file_path,
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
