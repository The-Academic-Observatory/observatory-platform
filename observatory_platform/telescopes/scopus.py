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
import calendar
import json
import logging
import pendulum
import os
import urllib.request
import xmltodict

from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor, as_completed
from elsapy.elsclient import ElsClient
from elsapy.elssearch import ElsSearch
from google.cloud.bigquery import SourceFormat
from typing import List
from math import ceil
from ratelimit import limits, sleep_and_retry
from requests import HTTPError
from urllib.error import URLError

from airflow.exceptions import AirflowException
from airflow.models.taskinstance import TaskInstance
from airflow.models import Variable

from observatory_platform.utils.config_utils import (
    AirflowVar,
    check_variables,
    SubFolder,
    telescope_path,
)

from observatory_platform.utils.telescope_utils import (
    get_entry_or_none,
    get_as_list,
    validate_date,
    write_to_file,
)

class ScopusRelease:
    """ Used to store info on a given SCOPUS release.

    :param inst_id: institution id from the airflow connection (minus the wos_)
    :param scopus_inst_id: List of institution ids to use in the SCOPUS query.
    :param release_date: Release date (currently the execution date).
    :param dag_start: Start date of the dag (not execution date).
    :param project_id: The project id to use.
    :param download_bucket_name: Download bucket name to use for storing downloaded files in the cloud.
    :param transform_bucket_name: Transform bucket name to use for storing transformed files in the cloud.
    :param data_location: Location of the data servers
    """

    def __init__(self, inst_id: str, scopus_inst_id: List[str], release_date: pendulum.Pendulum,
                 dag_start: pendulum.Pendulum, project_id: str,
                 download_bucket_name: str, transform_bucket_name: str, data_location: str, schema_ver: str):
        self.inst_id = inst_id
        self.scopus_inst_id = scopus_inst_id
        self.release_date = release_date
        self.dag_start = dag_start
        self.download_path = telescope_path(SubFolder.downloaded, ScopusTelescope.DAG_ID)
        self.transform_path = telescope_path(SubFolder.transformed, ScopusTelescope.DAG_ID)
        self.telescope_path = f'telescopes/{ScopusTelescope.DAG_ID}/{release_date}'
        self.project_id = project_id
        self.download_bucket_name = download_bucket_name
        self.transform_bucket_name = transform_bucket_name
        self.data_location = data_location
        self.schema_ver = schema_ver


class ScopusTelescope:
    """ A container for holding the constants and static functions for the SCOPUS telescope."""

    DAG_ID = 'scopus'
    ID_STRING_OFFSET = len(DAG_ID) + 1
    DESCRIPTION = 'SCOPUS: https://www.scopus.com'
    API_SERVER = 'https://api.elsevier.com'
    SCHEDULE_INTERVAL = '@monthly'
    QUEUE = 'remote_queue'
    RETRIES = 3
    DATASET_ID = 'elsevier'
    SCHEMA_PATH = 'telescopes'
    TABLE_NAME = DAG_ID
    SCHEMA_VER = 'scopus1'  # Internal version. SCOPUS returns no version in response.

    TASK_ID_CHECK_DEPENDENCIES = 'check_dependencies'
    TASK_CHECK_API_SERVER = 'check_api_server'
    TASK_ID_DOWNLOAD = 'download'
    TASK_ID_UPLOAD_DOWNLOADED = 'upload_downloaded'
    TASK_ID_TRANSFORM_DB_FORMAT = 'transform_db_format'
    TASK_ID_UPLOAD_TRANSFORMED = 'upload_transformed'
    TASK_ID_BQ_LOAD = 'bq_load'
    TASK_ID_CLEANUP = 'cleanup'

    XCOM_RELEASES = 'releases'
    XCOM_DOWNLOAD_PATH = 'download_path'
    XCOM_UPLOAD_ZIP_PATH = 'download_zip_path'
    XCOM_JSON_PATH = 'json_path'
    XCOM_JSON_HARVEST = 'json_harvest'
    XCOM_JSONL_PATH = 'jsonl_path'
    XCOM_JSONL_ZIP_PATH = 'jsonl_zip_path'
    XCOM_JSONL_BLOB_PATH = 'jsonl_blob_path'

    DOWNLOAD_MODE = 'sequential'  # Valid options: ['sequential', 'parallel']

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

        conn = kwargs['conn']

        # Validate extra field is set correctly.
        logging.info(f'Validating json in extra field of {conn}')
        extra = conn.extra
        try:
            extra_dict = json.loads(extra)
        except Exception as e:
            raise AirflowException(f'Error processing json extra fields in {conn} connection id profile: {e}')

        logging.info(f'Validating extra field keys for {conn}')

        # Check date is ok
        start_date = extra_dict['start_date']
        if not validate_date(start_date):
            raise AirflowException(f'Invalid date string for {conn}: {start_date}')

        # Check institution id is set
        if 'id' not in extra_dict:
            raise AirflowException(f'The "id" field is not set for {conn}.')

        # Check API keys are present
        if 'api_keys' not in extra_dict or len(extra_dict['api_keys']) == 0:
            raise AirflowException(f'No API keys are set for {conn}.')

        logging.info(f'Checking for airflow override variables of {conn}')
        # Set project id override
        project_id = Variable.get(AirflowVar.project_id.get())
        if 'project_id' in extra_dict:
            project_id = extra_dict['project_id']
            logging.info(f'Override for project_id found. Using: {project_id}')

        # Set download bucket name override
        download_bucket_name = Variable.get(AirflowVar.download_bucket_name.get())
        if 'download_bucket_name' in extra_dict:
            download_bucket_name = extra_dict['download_bucket_name']
            logging.info(f'Override for download_bucket_name found. Using: {download_bucket_name}')

        # Set transform bucket name override
        transform_bucket_name = Variable.get(AirflowVar.transform_bucket_name.get())
        if 'transform_bucket_name' in extra_dict:
            transform_bucket_name = extra_dict['transform_bucket_name']
            logging.info(f'Override for transform_bucket_name found. Using: {transform_bucket_name}')

        # Set data location override
        data_location = Variable.get(AirflowVar.data_location.get())
        if 'data_location' in extra_dict:
            data_location = extra_dict['data_location']
            logging.info(f'Override for data_location found. Using: {data_location}')

        # Push release information for other tasks
        scopus_inst_id = get_as_list(extra_dict, 'id')
        release = ScopusRelease(inst_id=kwargs['institution'], scopus_inst_id=scopus_inst_id,
                                release_date=kwargs['execution_date'].date(),
                                dag_start=pendulum.parse(kwargs['dag_start']).date(), project_id=project_id,
                                download_bucket_name=download_bucket_name, transform_bucket_name=transform_bucket_name,
                                data_location=data_location, schema_ver=ScopusTelescope.SCHEMA_VER)

        logging.info(
            f'ScopusRelease contains:\ndownload_bucket_name: {release.download_bucket_name}, transform_bucket_name: ' +
            f'{release.transform_bucket_name}, data_location: {release.data_location}')

        ti: TaskInstance = kwargs['ti']
        ti.xcom_push(ScopusTelescope.XCOM_RELEASES, release)

    @staticmethod
    def check_api_server(**kwargs):
        """ Check that the API server is still contactable.

        :param kwargs: the context passed from the BranchPythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: the identifier of the task to execute next.
        """

        http_code_ok = 200

        try:
            http_code = urllib.request.urlopen(ScopusTelescope.API_SERVER).getcode()
        except URLError as e:
            raise ValueError(f'Failed to fetch url because of: {e}')

        if http_code != http_code_ok:
            raise ValueError(f'HTTP response code {http_code} received.')

    @staticmethod
    def download(**kwargs):
        """ Task to download the SCOPUS snapshots.

        Pushes the following xcom:
            download_path (str): the path to a pickled file containing the list of xml responses in a month query.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

    @staticmethod
    def upload_downloaded(**kwargs):
        """ Task to upload the downloaded SCOPUS snapshots.

        Pushes the following xcom:
            upload_zip_path (str): the path to pickle zip file of downloaded response.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

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

    @staticmethod
    def upload_transformed(**kwargs):
        """ Task to upload the transformed SCOPUS data into jsonlines files.

        Pushes the following xcom:
            release_date (str): the release date of the GRID release.
            blob_name (str): the name of the blob on the Google Cloud storage bucket.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

    @staticmethod
    def bq_load(**kwargs):
        """ Task to load the transformed SCOPUS snapshot into BigQuery.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

    @staticmethod
    def cleanup(**kwargs):
        """ Delete files of downloaded, extracted and transformed releases.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

    @staticmethod
    def pull_release(ti: TaskInstance):
        """ Get the ScopusRelease object from XCOM message. """
        return ti.xcom_pull(key=ScopusTelescope.XCOM_RELEASES, task_ids=ScopusTelescope.TASK_ID_CHECK_DEPENDENCIES,
                            include_prior_dates=False)


class ScopusUtilConst:
    """ Constants for the SCOPUS utility class. """

    # WoS limits. Not sure if necessary but imposing to be safe.
    RESULT_LIMIT = 100  # Return 100 results max per query.
    CALL_LIMIT = 1  # WoS says they can do 2 api calls / second.
    CALL_PERIOD = 1  # seconds
    API_KEY_QUERY_QUOTA = 20000  # API key limit for the Curtin scopus keys.
    RETRIES = 3
    ELSAPY_RESULT_LIMIT = 5000  # Upper limit on number of results returned


class ScopusUtility:
    """ Handles the SCOPUS interactions. """

    @staticmethod
    def build_query(scopus_inst_id: List[str], period: tuple) -> str:
        """ Build a SCOPUS API query.

        :param scopus_inst_id: List of Institutional ID to query, e.g, "60031226" (Curtin University)
        :param period: A tuple containing start and end dates.
        :return: Constructed web query.
        """

        # Build organisations. This needs testing.
        organisations = str()
        for i, inst in enumerate(scopus_inst_id):
            organisations += f'AF-ID({inst}) OR '
        organisations = organisations[:-4]  # remove last ' OR '

        # Build publication date range
        search_months = str()
        for year in range(period[0].year, period[1].year+1):
            for month in range(1, 13):
                search_month = pendulum.date(year, month, 1)
                if period[0] <= search_month <= period[1]:
                    month_name = calendar.month_name[month]
                    search_months += f'"{month_name} {year}" or '
        search_months = search_months[:-4]  # remove last ' or '

        query = f'({organisations}) AND PUBDATETXT({search_months})'
        return query

    @staticmethod
    def download_scopus_period(client: ElsClient, conn: str, period: tuple, inst_id: str, download_path: str) -> str:
        """ Download records for a stated date range.
        The elsapy package currently has a cap of 5000 results per query. So in the unlikely event any institution has
        more than 5000 entries per month, this will present a problem.

        :param client: ElsClient object.
        :param conn: file name for saved response as a pickle file.
        :param period: Period to fetch (start_date, end_date). Will fetch up to
        :param inst_id: Institutional ID to query, e.g, "60031226" (Curtin University)
        :param download_path: Path to download to.
        :return: Save file, quota remaining (str), and quota reset date (str)
         """

        timestamp = pendulum.datetime.now().isoformat()
        inst_str = conn[4:]
        save_file = os.path.join(download_path, f'{inst_str}-{period[0]}_{timestamp}.json')
        logging.info(f'{conn}: retrieving period {period[0]} - {period[1]}')
        query = ScopusUtility.build_query(inst_id, period)
        search = ElsSearch(query, 'scopus')
        search.execute(client, get_all=True)  # This could throw a HTTPError if we exceed quota.
        result = json.dumps(search.results)
        quota_remaining = search.quota_remaining  # String of integers
        quota_reset = search.quota_reset  # Pendulum parsable string

        if len(result) >= ScopusUtilConst.ELSAPY_RESULT_LIMIT:
            logging.warning(
                f'{conn}: Result limit {ScopusUtilConst.ELSAPY_RESULT_LIMIT} reached for {period[0]} - {period[1]}')

        write_to_file(result, save_file)

        return save_file, quota_remaining, quota_reset

    @staticmethod
    def download_scopus_sequential(api_keys: List[str], schedule: list, conn: str, inst_id: str, download_path: str):
        """ Download SCOPUS snapshot sequentially. Tasks will be distributed in a round robin to the available keys.

        :param api_keys: List of API keys used to access SCOPUS service.
        :param schedule: List of date range (start_date, end_date) tuples to download.
        :param conn: Airflow connection_id string.
        :param inst_id: Institutioanl ID to query, e.g, "60031226" (Curtin University)
        :param download_path: Path to download to.
        :return: List of files downloaded.
        """

        return ScopusUtility.download_scopus_batch(api_keys, schedule, conn, inst_id, download_path)

    @staticmethod
    def download_scopus_parallel(api_keys: List[str], schedule: list, conn: str, inst_id: str, download_path: str):
        """ Download SCOPUS snapshot with parallel sessions. Tasks will be distributed in parallel to the available
        keys.

        :param api_keys: List of API keys used to access SCOPUS service.
        :param schedule: List of date range (start_date, end_date) tuples to download.
        :param conn: Airflow connection_id string.
        :param inst_id: Institutioanl ID to query, .e.g, "Curtin University"
        :param download_path: Path to download to.
        :return: List of files downloaded.
        """

    @staticmethod
    def download_scopus_batch(api_key: str, batch: list, conn: str, inst_id: str, download_path: str):
        """ Download one batch of SCOPUS snapshots. Handle quota backoff in here.

            quota_exceeded = True if fetch_log["error"].startswith("HTTPError('QuotaExceeded") else False
            if quota_exceeded:
                renews_at = str(fetch_log["error"])[37:-2]

                fetch_log["quota_renews_at"] = renews_at

        :param api_key: API key used to access SCOPUS service.
        :param batch: List of tuples of (start_date, end_date) to fetch.
        :param conn: connection_id string from Airflow variable.
        :param inst_id: institution id to query.
        :param download_path: download path to save response to.
        :return: List of saved files from this batch.
        """

    @staticmethod
    def make_query(client: ElsClient, query: str):
        """Make the API calls to retrieve information from SCOPUS.

        :param client: ElsClient object.
        :param query: Constructed search query from use build_query.

        :return: List of XML responses.
        """

    @staticmethod
    @sleep_and_retry
    @limits(calls=ScopusUtilConst.CALL_LIMIT, period=ScopusUtilConst.CALL_PERIOD)
    def scopus_search(client: ElsClient, query: str):
        """ Throttling wrapper for the API call. This is a global limit for this API when called from a program on the
        same machine. Limits specified in ScopusUtilConst class.

        Throttle limits may or may not be enforced. Probably depends on how executors spin up tasks.

        :param client: ElsClient object.
        :param query: Query object.
        :returns: Query results.
        """


class ScopusJsonParser:
    """ Helper methods to process the json from SCOPUS into desired structure. """

    def get_affiliations(self, data):
        """ Get the affiliation field. """

        affiliations = list()
        if 'affiliation' not in data:
            return affiliations

        for affiliation in data['affiliations']:
            affil = dict()
            affil['name'] = get_entry_or_none(affiliation, 'affilname')
            affil['city'] = get_entry_or_none(affiliation, 'affiliation-city')
            affil['country'] = get_entry_or_none(affiliation, 'affiliation-country')
            affiliations.append(affil)

        return affiliations

    @staticmethod
    def parse_json(data: dict, harvest_datetime: str, release_date: str) -> dict:
        """ Turn json data into db schema format.

        :param data: json response from SCOPUS.
        :param harvest_datetime: isoformat string of time the fetch took place.
        :param release_date: DAG execution date.
        :return: dict of data in right field format.
        """

        entry = dict()
        entry['title'] = get_entry_or_none(data, 'dc:title')
        entry['identifier'] = get_entry_or_none(data, 'dc:identifier')
        entry['creator'] = get_entry_or_none(data, 'dc:creator')
        entry['publication_name'] = get_entry_or_none(data, 'prism:publicationName')
        entry['cover_date'] = get_entry_or_none(data, 'prism:coverDate')
        entry['doi'] = get_entry_or_none(data, 'prism:doi')
        entry['eissn'] = get_entry_or_none(data, 'prism:eIssn')
        entry['issn'] = get_entry_or_none(data, 'prism:issn')
        entry['aggregation_type'] = get_entry_or_none(data, 'prism:aggregationType')
        entry['pubmed-id'] = get_entry_or_none(data, 'pubmed-id')
        entry['pii'] = get_entry_or_none(data, 'pii')
        entry['eid'] = get_entry_or_none(data, 'eid')
        entry['subtype_description'] = get_entry_or_none(data, 'subtypeDescription')
        entry['open_access'] = get_entry_or_none(data, 'openaccess')
        entry['open_access_flag'] = get_entry_or_none(data, 'openaccessFlag')
        entry['citedby_count'] = get_entry_or_none(data, 'citedby-count')
        entry['source-id'] = get_entry_or_none(data, 'source-id')
        entry['affiliations'] = ScopusJsonParser.get_affiliations(data)
        return entry
