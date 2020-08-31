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

import json
import logging
import os
import pathlib
from multiprocessing import cpu_count
from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.models.taskinstance import TaskInstance
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


def extract_events(url, events=None, next_cursor=None) -> list:
    if events is None:
        events = []
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/39.0.2171.95 Safari/537.36'
    }
    if next_cursor:
        tmp_url = url + f'&cursor={next_cursor}'
    else:
        tmp_url = url
    response = retry_session().get(tmp_url, headers=headers)
    if response.status_code == 200:
        response_dict = json.loads(response.text)
        new_events = response_dict['message']['events']
        next_cursor = response_dict['message']['next-cursor']
        if next_cursor:
            events += new_events
            extract_events(url, events, next_cursor)
            return events
        else:
            return events
    else:
        raise ConnectionError(f"Error requesting url: {url}, response: {response.text}")


def download_release(release: 'CrossrefEventsRelease'):
    """ Downloads release

    :param release: Instance of CrossrefRelease class
    :return:  None.
    """

    logging.info(f"Downloading from url: {release.url}")

    # Extract events
    events = extract_events(release.url)

    # Save events to file
    with open(release.download_path, 'w') as json_out:
        json.dump(events, json_out)

    logging.info(f"Successfully downloaded events to {release.download_path}")


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

    def __init__(self, start_date, end_date):
        self.url = CrossrefEventsTelescope.TELESCOPE_URL.format(mail_to=CrossrefEventsTelescope.MAILTO,
                                                                start_date=start_date.strftime("%Y-%m-%d"),
                                                                end_date=end_date.strftime("%Y-%m-%d"))
        self.start_date = start_date
        self.end_date = end_date

    @property
    def download_path(self):
        return self.get_path(SubFolder.downloaded)

    @property
    def transform_path(self):
        return self.get_path(SubFolder.transformed)

    def get_path(self, sub_folder: SubFolder) -> str:
        """ Gets complete path of file for download/transform directory or file.

        :param sub_folder: name of subfolder
        :return: the path.
        """

        date_str = self.start_date.strftime("%Y_%m_%d") + "-" + self.end_date.strftime("%Y_%m_%d")

        file_name = f"{CrossrefEventsTelescope.DAG_ID}_{date_str}.json"

        path = os.path.join(telescope_path(sub_folder, CrossrefEventsTelescope.DAG_ID), file_name)
        return path

    def get_blob_name(self, sub_folder: SubFolder) -> str:
        """ Gives blob name that is used to determine path inside storage bucket

        :param sub_folder: name of subfolder
        :return: blob name
        """

        file_name = os.path.basename(self.get_path(sub_folder))
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

    MAILTO = 'aniek.roelofs@curtin.edu.au'
    TELESCOPE_URL = 'https://api.eventdata.crossref.org/v1/events?mailto={mail_to}&' \
                    'from-collected-date={start_date}&until-collected-date={end_date}&rows=10000'
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

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
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

        execution_date = kwargs['execution_date']
        next_execution_date = kwargs['next_execution_date']

        release = CrossrefEventsRelease(execution_date, next_execution_date)
        ti.xcom_push(CrossrefEventsTelescope.RELEASES_TOPIC_NAME, release, execution_date)

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

        # transform release
        with open(release.download_path, 'r') as json_download, open(release.transform_path, 'w') as json_transform:
            events = json.load(json_download)
            for event in events:
                event_updated = change_keys(event, convert)
                json_transform.write(json.dumps(event_updated))
                json_transform.write('\n')

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
        schema_file_path = find_schema(analysis_schema_path, CrossrefEventsTelescope.DAG_ID, release.end_date)
        if schema_file_path is None:
            logging.error(f'No schema found with search parameters: analysis_schema_path={analysis_schema_path}, '
                          f'table_name={CrossrefEventsTelescope.DAG_ID}, release_date={release.end_date}')
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
