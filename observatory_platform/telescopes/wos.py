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

# Author: Tuan Chien


import backoff
import json
import logging
import pendulum
import os
import urllib.request
import xmltodict

from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor, as_completed
from google.cloud.bigquery import SourceFormat
from typing import List
from math import ceil
from ratelimit import limits, sleep_and_retry
from suds import WebFault
from urllib.error import URLError
from wos import WosClient

from airflow.exceptions import AirflowException
from airflow.models.taskinstance import TaskInstance
from airflow.models import Variable

from observatory_platform.utils.telescope_utils import (
    build_schedule,
    delete_msg_files,
    json_to_db,
    validate_date,
    write_pickled_xml_to_json,
    write_pickle,
    zip_files,
)

from observatory_platform.utils.config_utils import (
    AirflowVar,
    SubFolder,
    check_variables,
    find_schema,
    list_connections,
    telescope_path,
    schema_path,
)

from observatory_platform.utils.gc_utils import (
    bigquery_partitioned_table_id,
    create_bigquery_dataset,
    load_bigquery_table,
    upload_telescope_file_list,
)


class WosUtility:
    """ Handles the interaction with Web of Science """

    # WoS limits as a guide for reference. Throttle limits more conservative than this.
    RESULT_LIMIT = 100  # Return 100 results max per query.
    CALL_LIMIT = 2  # WoS says they can do 2 api calls / second
    CALL_PERIOD = 1  # seconds
    SESSION_CALL_LIMIT = 5  # 5 calls per 5 min.
    SESSION_CALL_PERIOD = 300  # 5 minutes.

    @staticmethod
    def build_query(inst_id: str, period: tuple) -> OrderedDict:
        """ Build a WoS API query.

        :param inst_id: Institution ID needed to filter the search results.
        :param period: A tuple containing start and end dates.
        :return: Constructed web query.
        """
        start_date = period[0].isoformat()
        end_date = period[1].isoformat()
        organisation = inst_id

        query = OrderedDict([('query', f'OG={organisation}'),
                             ('count', WosUtility.RESULT_LIMIT),
                             ('offset', 1),
                             ('timeSpan', {'begin': start_date, 'end': end_date})
                             ])
        return query

    @staticmethod
    def parse_query(records) -> dict:
        """ Parse XML tree record into a dict.

        :param records: XML tree returned by the web query.
        :return: Dictionary version of the web response.
        """

        try:
            records_dict = xmltodict.parse(records)
        except Exception as e:
            raise AirflowException(f'XML to dict parsing failed: {e}')

        records = records_dict['records']
        schema_id = records['@xmlns']
        if schema_id != WosTelescope.SCHEMA_ID:
            logging.warning(f'WOS schema has changed.\nExpected: {WosTelescope.SCHEMA_ID}\nReceived: {schema_id}')

        return records['REC']

    @staticmethod
    def download_wos_period(client: WosClient, conn: str, period: tuple, inst_id: str, download_path: str) -> str:
        """ Download records for a stated date range.

         :param client: WebClient object.
         :param conn: file name for saved response as a pickle file.
         """
        timestamp = pendulum.datetime.now().isoformat()
        inst_str = conn[4:]
        save_file = os.path.join(download_path, f'{inst_str}-{period[0]}_{timestamp}.pkl')
        query = WosUtility.build_query(inst_id, period)
        result = WosUtility.make_query(client, query)
        logging.info(f'{conn}: retrieving period {period[0]} - {period[1]}')
        write_pickle(result, save_file)

        return save_file

    @staticmethod
    @backoff.on_exception(backoff.constant, WebFault, max_tries=3, interval=360)
    def download_wos_batch(login: str, password: str, batch: list, conn: str, inst_id: str, download_path: str):
        """ Download one batch of WoS snapshots. Throttling limits are more conservative than WoS limits.
        Throttle limits may or may not be enforced. Probably depends on how executors spin up tasks.

        :param login: login.
        :param password: password.
        :param batch: List of tuples of (start_date, end_date) to fetch.
        :param conn: connection_id string from Airflow variable.
        :param inst_id: institution id to query.
        :param download_path: download path to save response to.
        :return: List of saved files from this batch.
        """

        file_list = []
        with WosClient(login, password) as client:
            for period in batch:
                file = WosUtility.download_wos_period(client, conn, period, inst_id, download_path)
                file_list.append(file)
        return file_list

    @staticmethod
    def download_wos_parallel(login: str, password: str, schedule: list, conn: str, inst_id: str, download_path: str):
        """ Download WoS snapshot with parallel sessions. Using threads.

        :param login: WoS login
        :param password: WoS password
        :param schedule: List of date range (start_date, end_date) tuples to download.
        :param conn: Airflow connection_id string.
        :param inst_id: Institutioanl ID to query, .e.g, "Curtin University"
        :param download_path: Path to download to.
        :return: List of files downloaded.
        """

        sessions = WosUtility.SESSION_CALL_LIMIT
        batch_size = ceil(len(schedule) / sessions)

        # Evenly distribute schedule among the session batches. Last session gets whatever the remaining tail is.
        batches = [schedule[i * batch_size:(i + 1) * batch_size] for i in range(sessions - 1)]
        batches.append(schedule[(sessions - 1) * batch_size:])  # Last session

        file_list = []
        with ThreadPoolExecutor(max_workers=sessions) as executor:
            futures = []
            for i in range(sessions):
                futures.append(
                    executor.submit(WosUtility.download_wos_batch, login, password, batches[i], conn, inst_id,
                                    download_path))
            for future in as_completed(futures):
                files = future.result()
                file_list = file_list + files

        return file_list

    @staticmethod
    def download_wos_sequential(login: str, password: str, schedule: list, conn: str, inst_id: str, download_path: str):
        """ Download WoS snapshot sequentially.

        :param login: WoS login
        :param password: WoS password
        :param schedule: List of date range (start_date, end_date) tuples to download.
        :param conn: Airflow connection_id string.
        :param inst_id: Institutioanl ID to query, .e.g, "Curtin University"
        :param download_path: Path to download to.
        :return: List of files downloaded.
        """

        return WosUtility.download_wos_batch(login, password, schedule, conn, inst_id, download_path)

    @staticmethod
    def download_wos_snapshot(download_path: str, conn, dag_start, mode: str):
        """ Download snapshot from Web of Science for the given institution.

        :param download_path: the directory where the downloaded wos snapshot should be saved.
        :param conn: Airflow connection object.
        :param dag_start: start date of this DAG run.
        :param mode: Download mode to use. 'sequential' or 'parallel'
        """

        extra = json.loads(conn.extra)
        inst_id = extra['id']  # Institution id
        login = conn.login
        password = conn.password
        start_date = pendulum.parse(extra['start_date']).date()
        schedule = build_schedule(start_date, dag_start)

        if mode == 'sequential' or len(schedule) <= WosUtility.SESSION_CALL_LIMIT:
            return WosUtility.download_wos_sequential(login, password, schedule, str(conn), inst_id, download_path)

        if mode == 'parallel':
            return WosUtility.download_wos_parallel(login, password, schedule, str(conn), inst_id, download_path)

    @staticmethod
    def make_query(client: WosClient, query: OrderedDict):
        """Make the API calls to retrieve information from Web of Science.

        :param client: WosClient object.
        :param query: Constructed search query from use build_query.

        :return: List of XML responses.
        """

        results = WosUtility.wos_search(client, query)
        num_results = int(results.recordsFound)
        record_list = [results.records]

        if num_results > WosUtility.RESULT_LIMIT:
            for offset in range(2, num_results, WosUtility.RESULT_LIMIT):
                query['offset'] = offset
                record_list.append(WosUtility.wos_search(client, query).records)

        return record_list

    @staticmethod
    @sleep_and_retry
    @limits(calls=1, period=2)
    def wos_search(client: WosClient, query: OrderedDict):
        """ Throttling wrapper for the API call. This is a global limit for this API when called from a program on the
        same machine. If you are throttled, it will throw a WebFault and the exception message will contain the phrase
        'Request denied by Throttle server'

        Limiting to 1 call per second even though theoretical limit is 2 per second just in case.
        Throttle limits may or may not be enforced. Probably depends on how executors spin up tasks.

        :param client: WosClient object.
        :param query: Query object.
        :returns: Query results.
        """

        return client.search(**query)


class WosTelescope:
    """ A container for holding the constants and static functions for the Web of Science telescope."""

    DAG_ID = 'wos'
    SUBDAG_ID_DOWNLOAD = 'download'
    DESCRIPTION = 'Web of Science: https://www.clarivate.com/webofsciencegroup'
    SCHEDULE_INTERVAL = '@monthly'
    RELEASES_TOPIC_NAME = 'releases'
    QUEUE = 'remote_queue'
    RETRIES = 3

    TASK_ID_CHECK_DEPENDENCIES = 'check_dependencies'
    TASK_CHECK_API_SERVER = 'check_api_server'
    TASK_ID_DOWNLOAD = 'download'
    TASK_ID_UPLOAD_DOWNLOADED = 'upload_downloaded'
    TASK_ID_TRANSFORM_XML = 'transform_xml'
    TASK_ID_UPLOAD_JSON = 'upload_json'
    TASK_ID_TRANSFORM_DB_FORMAT = 'transform_db_format'
    TASK_ID_UPLOAD_TRANSFORMED = 'upload_transformed'
    TASK_ID_BQ_LOAD = 'bq_load'
    TASK_ID_CLEANUP = 'cleanup'
    TASK_ID_STOP = 'stop_dag'

    XCOM_DOWNLOAD_PATH = 'download_path'
    XCOM_UPLOAD_ZIP_PATH = 'download_zip_path'
    XCOM_JSON_PATH = 'json_path'
    XCOM_JSON_ZIP_PATH = 'json_zip_path'
    XCOM_HARVEST_DATETIME = 'harvest_datetime'
    XCOM_JSONL_PATH = 'jsonl_path'
    XCOM_JSONL_ZIP_PATH = 'jsonl_zip_path'
    XCOM_JSONL_BLOB_PATH = 'jsonl_blob_path'

    # Implement multiprocessing later if needed. WosClient seems to be synchronous i/o so asyncio doesn't help.
    DOWNLOAD_MODE = 'parallel'  # Valid options: ['sequential', 'parallel']

    API_SERVER = 'http://scientific.thomsonreuters.com'
    SCHEMA_ID = 'http://scientific.thomsonreuters.com/schema/wok5.4/public/FullRecord'

    @staticmethod
    def check_dependencies(**kwargs):
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
            raise AirflowException('Required variables are missing')

        conns = list_connections('wos')

        # Make sure there are connections configured
        if len(conns) == 0:
            raise AirflowException('No connection ids are set')

        # Validate start_date exists in every connection and is set correctly
        for conn in conns:
            extra = conn.extra

            try:
                extra_dict = json.loads(extra)
            except:
                raise AirflowException(f'Error processing json extra fields in {conn} connection id profile')

            # Check date is ok
            start_date = extra_dict['start_date']
            if not validate_date(start_date):
                raise AirflowException(f'Invalid date string for {conn}: {start_date}')

            # Check institution id is set
            if 'id' not in extra_dict:
                raise AirflowException(f'The "id" field is not set for {conn}.')

    @staticmethod
    def check_api_server(**kwargs):
        """ Check that http://scientific.thomsonreuters.com is still contactable.

        :param kwargs: the context passed from the BranchPythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: the identifier of the task to execute next.
        """

        HTTP_CODE_OK = 200

        try:
            http_code = urllib.request.urlopen(WosTelescope.API_SERVER).getcode()
        except URLError as e:
            raise ValueError(f'Failed to fetch url because of: {e}')

        if http_code != HTTP_CODE_OK:
            raise ValueError(f'HTTP response code {http_code} received.')

    @staticmethod
    def download(**kwargs):
        """ Task to download the WoS snapshots.

        Pushes the following xcom:
            download_path (str): the path to a pickled file containing the list of xml responses in a month query.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        # Prepare paths
        wos_download_path = telescope_path(SubFolder.downloaded, WosTelescope.DAG_ID)
        dag_start = pendulum.parse(kwargs['dag_start']).date()
        download_files = WosUtility.download_wos_snapshot(wos_download_path, kwargs['conn'], dag_start,
                                                          WosTelescope.DOWNLOAD_MODE)

        # Construct message to send to next task
        msgs_out = [{WosTelescope.XCOM_DOWNLOAD_PATH: file} for file in download_files]

        # Notify next task of the files downloaded.
        ti: TaskInstance = kwargs['ti']
        ti.xcom_push(WosTelescope.RELEASES_TOPIC_NAME, msgs_out, kwargs['execution_date'])

    @staticmethod
    def upload_downloaded(**kwargs):
        """ Task to upload the downloaded WoS snapshots.

        Pushes the following xcom:
            upload_zip_path (str): the path to pickle zip file of downloaded response.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        execution_date = kwargs['execution_date'].date().isoformat()

        # Get bucket name
        bucket_name = Variable.get(AirflowVar.download_bucket_name.get())

        # Pull messages
        ti: TaskInstance = kwargs['ti']
        dag_id = f'{WosTelescope.DAG_ID}.{WosTelescope.SUBDAG_ID_DOWNLOAD}'
        msgs_in = ti.xcom_pull(key=WosTelescope.RELEASES_TOPIC_NAME, task_ids=None,
                               include_prior_dates=False, dag_id=dag_id)

        # Upload each snapshot
        download_list = [msg[WosTelescope.XCOM_DOWNLOAD_PATH] for msg in msgs_in]
        zip_list = zip_files(download_list)
        upload_telescope_file_list(bucket_name, WosTelescope.DAG_ID, execution_date, zip_list)

        # Notify next task of the files downloaded.
        msgs_out = [{WosTelescope.XCOM_UPLOAD_ZIP_PATH: file} for file in zip_list]
        ti.xcom_push(WosTelescope.RELEASES_TOPIC_NAME, msgs_out, kwargs['execution_date'])

    @staticmethod
    def transform_xml(**kwargs):
        """ Task to transform the XML from the query to json.

        Pushes the following xcom:
            json_path (str): the path to json file of a converted xml response.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        ti: TaskInstance = kwargs['ti']
        dag_id = f'{WosTelescope.DAG_ID}.{WosTelescope.SUBDAG_ID_DOWNLOAD}'
        msgs_in = ti.xcom_pull(key=WosTelescope.RELEASES_TOPIC_NAME,
                               include_prior_dates=False, dag_id=dag_id)

        # Process each pickled file
        pickle_files = [msg[WosTelescope.XCOM_DOWNLOAD_PATH] for msg in msgs_in]
        json_file_list = write_pickled_xml_to_json(pickle_files, WosUtility.parse_query)

        # Notify next task of the converted json files
        msgs_out = []
        for file in json_file_list:
            harvest_datetime = file[file.rfind('_') + 1:-5]
            msg_out = {WosTelescope.XCOM_JSON_PATH: file, WosTelescope.XCOM_HARVEST_DATETIME: harvest_datetime}
            msgs_out.append(msg_out)
        ti.xcom_push(WosTelescope.RELEASES_TOPIC_NAME, msgs_out, kwargs['execution_date'])

    @staticmethod
    def upload_json(**kwargs):
        """ Task to upload the transformed json files.

        Pushes the following xcom:
            upload_json_zip_path (str): the path to json zip file of a converted xml response.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        execution_date = kwargs['execution_date'].date().isoformat()

        # Get bucket name
        bucket_name = Variable.get(AirflowVar.download_bucket_name.get())

        # Pull messages
        ti: TaskInstance = kwargs['ti']
        msgs_in = ti.xcom_pull(key=WosTelescope.RELEASES_TOPIC_NAME,
                               task_ids=WosTelescope.TASK_ID_TRANSFORM_XML,
                               include_prior_dates=False)

        # Upload each snapshot
        json_paths = [msg[WosTelescope.XCOM_JSON_PATH] for msg in msgs_in]
        zip_list = zip_files(json_paths)
        upload_telescope_file_list(bucket_name, WosTelescope.DAG_ID, execution_date, zip_list)

        # Notify next task of the files downloaded.
        msgs_out = [{WosTelescope.XCOM_JSON_ZIP_PATH: file} for file in zip_list]
        ti.xcom_push(WosTelescope.RELEASES_TOPIC_NAME, msgs_out, kwargs['execution_date'])

    @staticmethod
    def transform_db_format(**kwargs):
        """ Task to transform the json into db field format (and in jsonlines form).

        Pushes the following xcom:
            version (str): the version of the GRID release.
            json_gz_file_name (str): the file name for the transformed GRID release.
            json_gz_file_path (str): the path to the transformed GRID release (including file name).

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        release_date = kwargs['execution_date'].date().isoformat()

        # Pull messages
        ti: TaskInstance = kwargs['ti']
        msgs_in = ti.xcom_pull(key=WosTelescope.RELEASES_TOPIC_NAME,
                               task_ids=WosTelescope.TASK_ID_TRANSFORM_XML,
                               include_prior_dates=False)

        json_files = [(msg[WosTelescope.XCOM_JSON_PATH], msg[WosTelescope.XCOM_HARVEST_DATETIME]) for msg in msgs_in]
        json_files.sort()  # Sort by institution and date

        # Apply field extraction and transformation to jsonlines
        jsonl_list = json_to_db(json_files, release_date, WosJsonParser.parse_json)

        # Notify next task
        msgs_out = [{WosTelescope.XCOM_JSONL_PATH: file} for file in jsonl_list]
        ti.xcom_push(WosTelescope.RELEASES_TOPIC_NAME, msgs_out, kwargs['execution_date'])

    @staticmethod
    def upload_transformed(**kwargs):
        """ Task to upload the transformed WoS data into jsonlines files.

        Pushes the following xcom:
            release_date (str): the release date of the GRID release.
            blob_name (str): the name of the blob on the Google Cloud storage bucket.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        execution_date = kwargs['execution_date'].date().isoformat()

        # Get bucket name
        bucket_name = Variable.get(AirflowVar.transform_bucket_name.get())

        # Pull messages
        ti: TaskInstance = kwargs['ti']
        msgs_in = ti.xcom_pull(key=WosTelescope.RELEASES_TOPIC_NAME,
                               task_ids=WosTelescope.TASK_ID_TRANSFORM_DB_FORMAT,
                               include_prior_dates=False)

        # Upload each snapshot
        jsonl_paths = [msg[WosTelescope.XCOM_JSONL_PATH] for msg in msgs_in]
        zip_list = zip_files(jsonl_paths)
        blob_list = upload_telescope_file_list(bucket_name, WosTelescope.DAG_ID, execution_date, zip_list)

        # Notify next task of the files downloaded.
        msgs_out = [{WosTelescope.XCOM_JSONL_BLOB_PATH: file} for file in blob_list]
        msgs_out = msgs_out + [{WosTelescope.XCOM_JSONL_ZIP_PATH: file} for file in zip_list]
        ti.xcom_push(WosTelescope.RELEASES_TOPIC_NAME, msgs_out, kwargs['execution_date'])

    @staticmethod
    def bq_load(**kwargs):
        """ Task to load the transformed WoS snapshot into BigQuery.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        ti: TaskInstance = kwargs['ti']
        msgs_in = ti.xcom_pull(key=WosTelescope.RELEASES_TOPIC_NAME, task_ids=WosTelescope.TASK_ID_UPLOAD_TRANSFORMED,
                               include_prior_dates=False)

        execution_date = kwargs['execution_date'].date()
        blobs = filter(lambda x: WosTelescope.XCOM_JSONL_BLOB_PATH in x, msgs_in)
        jsonl_zip_blobs = [msg[WosTelescope.XCOM_JSONL_BLOB_PATH] for msg in blobs]

        # Upload each release
        project_id = Variable.get(AirflowVar.project_id.get())
        data_location = Variable.get(AirflowVar.data_location.get())
        bucket_name = Variable.get(AirflowVar.transform_bucket_name.get())

        # Create dataset
        dataset_id = 'clarivate'
        create_bigquery_dataset(project_id, dataset_id, data_location, WosTelescope.DESCRIPTION)

        analysis_schema_path = schema_path('telescopes')
        table_name = WosTelescope.DAG_ID

        # Create table id
        table_id = bigquery_partitioned_table_id(WosTelescope.DAG_ID, execution_date)

        # Load into BigQuery
        for file in jsonl_zip_blobs:
            schema_file_path = find_schema(analysis_schema_path, table_name, execution_date)
            if schema_file_path is None:
                logging.error(f'No schema found with search parameters: analysis_schema_path={analysis_schema_path}, '
                              f'table_name={table_name}, release_date={execution_date}')
                exit(os.EX_CONFIG)

            # Load BigQuery table
            uri = f"gs://{bucket_name}/{file}"
            logging.info(f"URI: {uri}")
            load_bigquery_table(uri, dataset_id, data_location, table_id, schema_file_path,
                                SourceFormat.NEWLINE_DELIMITED_JSON)

    @staticmethod
    def cleanup(**kwargs):
        """ Delete files of downloaded, extracted and transformed releases.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        ti: TaskInstance = kwargs['ti']
        topic = WosTelescope.RELEASES_TOPIC_NAME
        dag_id = WosTelescope.DAG_ID
        subdag_id = f'{dag_id}.{WosTelescope.SUBDAG_ID_DOWNLOAD}'

        delete_msg_files(ti, topic, None, WosTelescope.XCOM_DOWNLOAD_PATH, subdag_id)
        delete_msg_files(ti, topic, WosTelescope.TASK_ID_UPLOAD_DOWNLOADED, WosTelescope.XCOM_UPLOAD_ZIP_PATH, dag_id)
        delete_msg_files(ti, topic, WosTelescope.TASK_ID_TRANSFORM_XML, WosTelescope.XCOM_JSON_PATH, dag_id)
        delete_msg_files(ti, topic, WosTelescope.TASK_ID_UPLOAD_JSON, WosTelescope.XCOM_JSON_ZIP_PATH, dag_id)
        delete_msg_files(ti, topic, WosTelescope.TASK_ID_TRANSFORM_DB_FORMAT, WosTelescope.XCOM_JSONL_PATH, dag_id)
        delete_msg_files(ti, topic, WosTelescope.TASK_ID_UPLOAD_TRANSFORMED, WosTelescope.XCOM_JSONL_ZIP_PATH, dag_id)


class WosNameAttributes:
    """ Helper class for parsing name attributes."""

    def __init__(self, data: dict):
        self._contribs = self._get_contribs(data)

    def _get_contribs(self, data: dict):
        """ Helper function to parse the contributors structure to aid extraction of fields.

        :param base: dictionary to query.
        :return: Dictionary of attributes keyed by full_name string.
        """

        contrib_dict = dict()

        if 'contributors' in data['static_data']:
            contributors = WosJsonParser.get_as_list(data['static_data']['contributors'], 'contributor')
            for contributor in contributors:
                name_field = contributor['name']
                first_name = name_field['first_name']
                last_name = name_field['last_name']
                attrib = dict()
                full_name = f'{first_name} {last_name}'
                if '@r_id' in name_field:
                    attrib['r_id'] = name_field['@r_id']
                if '@orcid_id' in name_field:
                    attrib['orcid'] = name_field['@orcid_id']
                contrib_dict[full_name] = attrib

        return contrib_dict

    def get_orcid(self, full_name: str):
        """ Get the orcid id of a person. Note that full name must be the combination of first and last name.
            This is not necessarily the full_name field.

            :param full_name: The 'first_name last_name' string.
            :return: orcid id.
        """
        if full_name is None or full_name not in self._contribs or 'orcid' not in self._contribs[full_name]:
            return None
        return self._contribs[full_name]['orcid']

    def get_r_id(self, full_name: str):
        """ Get the r_id of a person. Note that full name must be the combination of first and last name.
            This is not necessarily the full_name field.

            :param full_name: The 'first_name last_name' string.
            :return: r_id.
        """

        if full_name is None or full_name not in self._contribs or 'r_id' not in self._contribs[full_name]:
            return None
        return self._contribs[full_name]['r_id']


class WosJsonParser:
    """ Helper methods to process the the converted json from Web of Science. """

    @staticmethod
    def get_as_list(base: dict, target):
        """ Helper function that returns the target as a list.

        :param base: dictionary to query.
        :param target: target key.
        :return: base[target] as a list (if it isn't already).
        """

        if target not in base:
            return None

        if type(base[target]) != type(list()):
            return [base[target]]

        return base[target]

    @staticmethod
    def get_as_list_or_none(base: dict, key, subkey):
        """ Helper function that returns a list or None if key is missing.

        :param base: dictionary to query.
        :param target: target key.
        :param subkey: subkey to target.
        :return: entry or None.
        """

        if key not in base or base[key]['@count'] == "0":
            return None

        return WosJsonParser.get_as_list(base[key], subkey)

    @staticmethod
    def get_entry_or_none(base: dict, target):
        """ Helper function that returns an entry or None if key is missing.

        :param base: dictionary to query.
        :param target: target key.
        :return: entry or None.
        """

        if target not in base:
            return None
        return base[target]

    @staticmethod
    def get_identifiers(data: dict):
        """ Extract identifier information.

        :param data: dictionary of web response.
        :return: Identifier record.
        """

        recognised_types = {'issn', 'eissn', 'isbn', 'eisbn', 'art_no', 'meeting_abs', 'xref_doi', 'parent_book_doi',
                            'doi'}
        field = dict()
        field['uid'] = data['UID']

        for thing in recognised_types:
            field[thing] = None

        if 'dynamic_data' not in data or 'cluster_related' not in data['dynamic_data'] or 'identifiers' not in \
                data['dynamic_data']['cluster_related'] or 'identifier' not in data['dynamic_data']['cluster_related'][
            'identifiers']:
            return field

        identifiers = data['dynamic_data']['cluster_related']['identifiers']
        identifier = WosJsonParser.get_as_list(identifiers, 'identifier')
        if type(identifier) != type(list()):  # Fix another 'gotcha'
            identifier = [identifier]

        for entry in identifier:
            type_ = entry['@type']
            value = entry['@value']

            if type_ in recognised_types:
                field[type_] = value

        return field

    @staticmethod
    def get_pub_info(data: dict):
        """ Extract publication information fields.

        :param data: dictionary of web response.
        :return: Publication info record.
        """

        summary = data['static_data']['summary']

        field = dict()
        field['sort_date'] = None
        field['pub_type'] = None
        field['page_count'] = None
        field['source'] = None
        field['doc_type'] = None
        field['publisher'] = None
        field['publisher_city'] = None

        if 'pub_info' in summary:
            pub_info = summary['pub_info']
            field['sort_date'] = pub_info['@sortdate']
            field['pub_type'] = pub_info['@pubtype']
            field['page_count'] = int(pub_info['page']['@page_count'])

        if 'publishers' in summary and 'publisher' in summary['publishers']:
            publisher = summary['publishers']['publisher']
            field['publisher'] = publisher['names']['name']['full_name']
            field['publisher_city'] = publisher['address_spec']['city']

        if 'titles' in summary and 'title' in summary['titles']:
            titles = WosJsonParser.get_as_list(summary['titles'], 'title')
            for title in titles:
                if title['@type'] == 'source':
                    field['source'] = title['#text']
                    break

        if 'doctypes' in summary:
            doctypes = WosJsonParser.get_as_list(summary['doctypes'], 'doctype')
            field['doc_type'] = doctypes[0]

        return field

    @staticmethod
    def get_title(data: dict):
        """ Extract title. May raise exception on error.

        :param data: dictionary of web response.
        :return: String of title or None if not found.
        """
        if 'titles' not in data['static_data']['summary']:
            return None

        for entry in data['static_data']['summary']['titles']['title']:
            if '@type' in entry and entry['@type'] == 'item' and '#text' in entry:
                return entry['#text']

        entry = data['static_data']['summary']['titles']['title']
        raise AirflowException('Schema change detected in title field. Please review.')

    @staticmethod
    def get_names(data: dict):
        """ Extract names fields.

        :param data: dictionary of web response.
        :return: List of name records.
        """

        field = list()
        if 'names' not in data['static_data']['summary']:
            return field

        names = data['static_data']['summary']['names']
        if 'name' not in names:
            return field

        attrib = WosNameAttributes(data)
        names = WosJsonParser.get_as_list(data['static_data']['summary']['names'], 'name')

        for name in names:
            entry = dict()
            entry['seq_no'] = int(WosJsonParser.get_entry_or_none(name, '@seq_no'))
            entry['role'] = WosJsonParser.get_entry_or_none(name, '@role')
            entry['first_name'] = WosJsonParser.get_entry_or_none(name, 'first_name')
            entry['last_name'] = WosJsonParser.get_entry_or_none(name, 'last_name')
            entry['wos_standard'] = WosJsonParser.get_entry_or_none(name, 'wos_standard')
            entry['daisng_id'] = WosJsonParser.get_entry_or_none(name, '@daisng_id')
            entry['full_name'] = WosJsonParser.get_entry_or_none(name, 'full_name')

            # Get around errors / booby traps for name retrieval
            first_name = entry['first_name']
            last_name = entry['last_name']
            full_name = f'{first_name} {last_name}'
            entry['orcid'] = attrib.get_orcid(full_name)
            entry['r_id'] = attrib.get_r_id(full_name)
            field.append(entry)

        return field

    @staticmethod
    def get_languages(data: dict):
        """ Extract language fields.

        :param data: dictionary of web response.
        :return: List of language records.
        """

        lang_list = list()
        if 'languages' not in data['static_data']['fullrecord_metadata']:
            return lang_list

        if 'language' not in data['static_data']['fullrecord_metadata']['languages']:
            return lang_list

        languages = WosJsonParser.get_as_list(data['static_data']['fullrecord_metadata']['languages'], 'language')
        for entry in languages:
            lang_list.append({"type": entry['@type'], "name": entry['#text']})
        return lang_list

    @staticmethod
    def get_refcount(data: dict):
        """ Extract reference count.

        :param data: dictionary of web response.
        :return: Reference count.
        """

        if 'refs' not in data['static_data']['fullrecord_metadata']:
            return None

        if '@count' not in data['static_data']['fullrecord_metadata']['refs']:
            return None

        return int(data['static_data']['fullrecord_metadata']['refs']['@count'])

    @staticmethod
    def get_abstract(data: dict):
        """ Extract abstracts.

        :param data: dictionary of web response.
        :return: List of abstracts.
        """

        abstract_list = list()
        if 'abstracts' not in data['static_data']['fullrecord_metadata']:
            return abstract_list
        if 'abstract' not in data['static_data']['fullrecord_metadata']['abstracts']:
            return abstract_list

        abstracts = WosJsonParser.get_as_list(data['static_data']['fullrecord_metadata']['abstracts'], 'abstract')
        for abstract in abstracts:
            texts = WosJsonParser.get_as_list(abstract['abstract_text'], 'p')
            for text in texts:
                abstract_list.append(text)
        return abstract_list

    @staticmethod
    def get_keyword(data: dict):
        """ Extract keywords. Will also get the keywords from keyword plus if they are available.

        :param data: dictionary of web response.
        :return: List of keywords.
        """

        keywords = list()
        if 'keywords' not in data['static_data']['fullrecord_metadata']:
            return keywords
        if 'keyword' not in data['static_data']['fullrecord_metadata']['keywords']:
            return keywords

        keywords = WosJsonParser.get_as_list(data['static_data']['fullrecord_metadata']['keywords'], 'keyword')
        if 'item' in data['static_data'] and 'keywords_plus' in data['static_data']['item']:
            plus = WosJsonParser.get_as_list(data['static_data']['item']['keywords_plus'], 'keyword')
            keywords = keywords + plus
        return keywords

    @staticmethod
    def get_conference(data: dict):
        """ Extract conference information.

        :param data: dictionary of web response.
        :return: List of conferences.
        """

        conferences = list()
        if 'conferences' not in data['static_data']['summary']:
            return conferences
        if 'conference' not in data['static_data']['summary']['conferences']:
            return conferences

        conf_list = WosJsonParser.get_as_list(data['static_data']['summary']['conferences'], 'conference')
        for conf in conf_list:
            conference = dict()
            conference['id'] = int(WosJsonParser.get_entry_or_none(conf, '@conf_id'))
            conference['name'] = None

            if 'conf_titles' in conf and 'conf_title' in conf['conf_titles']:
                titles = WosJsonParser.get_as_list(conf['conf_titles'], 'conf_title')
                conference['name'] = titles[0]
            conferences.append(conference)
        return conferences

    @staticmethod
    def get_orgs(data: dict) -> list:
        """ Extract the organisation information.

        :param data: dictionary of web response.
        :return: list of organisations or None
        """

        orgs = list()

        if 'addresses' not in data['static_data']['fullrecord_metadata']:
            return orgs

        addr_list = WosJsonParser.get_as_list(data['static_data']['fullrecord_metadata']['addresses'], 'address_name')
        for addr in addr_list:
            spec = addr['address_spec']
            org = dict()

            org['city'] = WosJsonParser.get_entry_or_none(spec, 'city')
            org['state'] = WosJsonParser.get_entry_or_none(spec, 'state')
            org['country'] = WosJsonParser.get_entry_or_none(spec, 'country')

            if 'organizations' not in addr['address_spec']:
                orgs.append(org)
                return orgs

            org_list = WosJsonParser.get_as_list(addr['address_spec']['organizations'], 'organization')
            for entry in org_list:
                if '@pref' in entry and entry['@pref'] == 'Y':
                    org['org_name'] = entry['#text']
                    break
            else:
                org['org_name'] = org_list[0]
                if type(org['org_name']) != type(str()):
                    raise AirflowException('Schema parsing error for org.')

            if 'suborganizations' in addr['address_spec']:
                org['suborgs'] = WosJsonParser.get_as_list(addr['address_spec']['suborganizations'], 'suborganization')

            if 'names' in addr and 'name' in addr['names']:
                names = WosJsonParser.get_as_list(addr['names'], 'name')
                names_list = list()
                for name in names:
                    entry = dict()
                    entry['first_name'] = WosJsonParser.get_entry_or_none(name, 'first_name')
                    entry['last_name'] = WosJsonParser.get_entry_or_none(name, 'last_name')
                    entry['daisng_id'] = WosJsonParser.get_entry_or_none(name, '@daisng_id')
                    entry['full_name'] = WosJsonParser.get_entry_or_none(name, 'full_name')
                    entry['wos_standard'] = WosJsonParser.get_entry_or_none(name, 'wos_standard')
                    names_list.append(entry)
                org['names'] = names_list
            orgs.append(org)
        return orgs

    @staticmethod
    def get_fund_ack(data: dict) -> dict:
        """ Extract funding acknowledgements.

        :param data: dictionary of web response.
        :return: Funding acknowledgement information.
        """

        fund_ack = dict()
        fund_ack['text'] = list()
        fund_ack['grants'] = list()

        if 'fund_ack' not in data['static_data']['fullrecord_metadata']:
            return fund_ack

        entry = data['static_data']['fullrecord_metadata']['fund_ack']
        if 'fund_text' in entry and 'p' in entry['fund_text']:
            fund_ack['text'] = WosJsonParser.get_as_list(entry['fund_text'], 'p')

        if 'grants' not in entry:
            return fund_ack

        grants = WosJsonParser.get_as_list(entry['grants'], 'grant')
        for grant in grants:
            grant_info = dict()
            grant_info['agency'] = WosJsonParser.get_entry_or_none(grant, 'grant_agency')
            grant_info['ids'] = list()
            if 'grant_ids' in grant:
                grant_info['ids'] = WosJsonParser.get_as_list(grant['grant_ids'], 'grant_id')
            fund_ack['grants'].append(grant_info)
        return fund_ack

    @staticmethod
    def get_categories(data: dict) -> dict:
        """ Extract categories.

        :param data: dictionary of web response.
        :return: categories dictionary.
        """

        category_info = dict()
        if 'category_info' not in data['static_data']['fullrecord_metadata']:
            return category_info

        entry = data['static_data']['fullrecord_metadata']['category_info']
        category_info['headings'] = WosJsonParser.get_as_list_or_none(entry, 'headings', 'heading')
        category_info['subheadings'] = WosJsonParser.get_as_list_or_none(entry, 'subheadings', 'subheading')

        subject_list = list()
        subjects = WosJsonParser.get_as_list_or_none(entry, 'subjects', 'subject')
        for subject in subjects:
            subject_dict = dict()
            subject_dict['ascatype'] = WosJsonParser.get_entry_or_none(subject, '@ascatype')
            subject_dict['code'] = WosJsonParser.get_entry_or_none(subject, '@code')
            subject_dict['text'] = WosJsonParser.get_entry_or_none(subject, '#text')
            subject_list.append(subject_dict)
        category_info['subjects'] = subject_list

        return category_info

    @staticmethod
    def parse_json(data: dict, harvest_datetime: str, release_date: str) -> dict:
        """ Turn json data into db schema format.

        :param data: dictionary of web response.
        :param harvest_datetime: isoformat string of time the fetch took place.
        :param release_date: DAG execution date.
        :return: dict of data in right field format.
        """

        entry = dict()
        entry['harvest_datetime'] = harvest_datetime
        entry['release_date'] = release_date
        entry['identifiers'] = WosJsonParser.get_identifiers(data)
        entry['pub_info'] = WosJsonParser.get_pub_info(data)
        entry['title'] = WosJsonParser.get_title(data)
        entry['names'] = WosJsonParser.get_names(data)
        entry['languages'] = WosJsonParser.get_languages(data)
        entry['ref_count'] = WosJsonParser.get_refcount(data)
        entry['abstract'] = WosJsonParser.get_abstract(data)
        entry['keywords'] = WosJsonParser.get_keyword(data)
        entry['conferences'] = WosJsonParser.get_conference(data)
        entry['fund_ack'] = WosJsonParser.get_fund_ack(data)
        entry['categories'] = WosJsonParser.get_categories(data)
        entry['orgs'] = WosJsonParser.get_orgs(data)

        return entry
