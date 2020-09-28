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


import calendar
import json
import logging
import pendulum
import os
import urllib.request

from concurrent.futures import ThreadPoolExecutor, as_completed
from elsapy.elsclient import ElsClient
from elsapy.elssearch import ElsSearch
from google.cloud.bigquery import SourceFormat
from queue import Queue, Empty
from threading import Event
from time import sleep
from typing import List
from ratelimit import limits, sleep_and_retry
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
    build_schedule,
    get_entry_or_none,
    get_as_list,
    validate_date,
    write_to_file,
)


class ScopusRelease:
    """ Used to store info on a given SCOPUS release.

    :param inst_id: institution id from the airflow connection (minus the scopus_)
    :param scopus_inst_id: List of institution ids to use in the SCOPUS query.
    :param release_date: Release date (currently the execution date).
    :param start_date: Start date of the dag where to start pulling records from.
    :param end_date: End of records to pull.
    :param project_id: The project id to use.
    :param download_bucket_name: Download bucket name to use for storing downloaded files in the cloud.
    :param transform_bucket_name: Transform bucket name to use for storing transformed files in the cloud.
    :param data_location: Location of the data servers
    """

    def __init__(self, inst_id: str, scopus_inst_id: List[str], release_date: pendulum.Pendulum,
                 start_date: pendulum.Pendulum, end_date: pendulum.Pendulum, project_id: str,
                 download_bucket_name: str, transform_bucket_name: str, data_location: str, schema_ver: str,
                 view: str = 'standard'):
        self.inst_id = inst_id
        self.scopus_inst_id = sorted(scopus_inst_id)
        self.release_date = release_date
        self.start_date = start_date
        self.end_date = end_date
        self.download_path = telescope_path(SubFolder.downloaded, ScopusTelescope.DAG_ID)
        self.transform_path = telescope_path(SubFolder.transformed, ScopusTelescope.DAG_ID)
        self.telescope_path = f'telescopes/{ScopusTelescope.DAG_ID}/{release_date}'
        self.project_id = project_id
        self.download_bucket_name = download_bucket_name
        self.transform_bucket_name = transform_bucket_name
        self.data_location = data_location
        self.schema_ver = schema_ver
        self.view = view


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

        # Check if view is set. Options: 'standard' or 'complete'. Default if not set is 'standard'.
        # Used if elsapy is replaced.
        view = 'standard'
        if 'view' in extra_dict:
            view = extra_dict['view']

        # Push release information for other tasks
        scopus_inst_id = get_as_list(extra_dict, 'id')
        release = ScopusRelease(inst_id=kwargs['institution'], scopus_inst_id=scopus_inst_id,
                                release_date=kwargs['execution_date'].date(),
                                start_date=pendulum.parse(start_date).date(),
                                end_date=pendulum.parse(kwargs['dag_start']).date(), project_id=project_id,
                                download_bucket_name=download_bucket_name, transform_bucket_name=transform_bucket_name,
                                data_location=data_location, schema_ver=ScopusTelescope.SCHEMA_VER, view=view)

        logging.info(
            f'ScopusRelease contains:\ndownload_bucket_name: {release.download_bucket_name}, transform_bucket_name: ' +
            f'{release.transform_bucket_name}, data_location: {release.data_location}')

        ti: TaskInstance = kwargs['ti']
        ti.xcom_push(ScopusTelescope.XCOM_RELEASES, release)

    @staticmethod
    def check_api_server():
        """ Check that the API server is still contactable.

        :return: None.
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
    SCOPUS_RESULT_LIMIT = 5000  # Upper limit on number of results returned


class ScopusUtilWorker:
    """ Worker class """

    def __init__(self, client_id: int, client: ElsClient, quota_reset_date: pendulum.datetime):
        """ Constructor.

        :param client: ElsClient object for an API key.
        """

        self.client_id = client_id  # Internal identifier. Use this to identify the client in debug messages.
        self.client = client
        self.quota_reset_date = quota_reset_date


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
        for year in range(period[0].year, period[1].year + 1):
            for month in range(1, 13):
                search_month = pendulum.date(year, month, 1)
                if period[0] <= search_month <= period[1]:
                    month_name = calendar.month_name[month]
                    search_months += f'"{month_name} {year}" or '
        search_months = search_months[:-4]  # remove last ' or '

        query = f'({organisations}) AND PUBDATETXT({search_months})'
        return query

    @staticmethod
    def download_scopus_period(client: ElsClient, conn: str, period: tuple, inst_id: List[str],
                               download_path: str) -> str:
        """ Download records for a stated date range.
        The elsapy package currently has a cap of 5000 results per query. So in the unlikely event any institution has
        more than 5000 entries per month, this will present a problem.

         """

        timestamp = pendulum.datetime.now('UTC').isoformat()
        inst_str = conn[ScopusTelescope.ID_STRING_OFFSET:]

        save_file = os.path.join(download_path, f'{inst_str}-{period[0]}-{period[1]}_{timestamp}.json')
        logging.info(f'{conn}: retrieving period {period[0]} - {period[1]}')
        query = ScopusUtility.build_query(inst_id, period)
        search = ElsSearch(query, 'scopus')
        search.execute(client, get_all=True)  # This could throw a HTTPError if we exceed quota.
        result = json.dumps(search.results)

        if len(search.results) >= ScopusUtilConst.SCOPUS_RESULT_LIMIT:
            logging.warning(
                f'{conn}: Result limit {ScopusUtilConst.SCOPUS_RESULT_LIMIT} reached for {period[0]} - {period[1]}')

        write_to_file(result, save_file)
        return save_file

    @staticmethod
    def download_sequential(workers: List[ScopusUtilWorker], taskq: Queue, conn: str, inst_id: List[str],
                            download_path: str):
        """ Download SCOPUS snapshot sequentially. Tasks will be distributed in a round robin to the available keys.
        Block each task until it's done or failed.


        """

        timeout = 20
        qe_workers = list()  # Quota exceeded workers
        saved_files = list()
        reset_date = pendulum.now('UTC')

        workerq = Queue()
        for worker in workers:
            workerq.put(worker)

        while True:
            # Fetch a task to farm out
            try:
                logging.info(f'{conn}: Attempting to get task')
                task = taskq.get(block=True, timeout=timeout)
            except Empty:
                logging.info(f'{conn}: Task queue empty. All work done.')
                break  # All work done

            # Got a job. Need to find a worker to give it to.
            try:
                logging.info(f'{conn}: Attempting to get free worker')
                worker = workerq.get(block=True, timeout=timeout)

            # If we have a job and no workers it means quota has been exceeded on all workers. Sleep until cooldown
            # for at least one key has passed, then restart matching process.
            except Empty:
                logging.info(f'{conn}: no free workers. All workers exceeded quota. Sleeping until worker available.')
                now = pendulum.now('UTC')
                sleep_time = (reset_date - now).seconds + 1
                logging.info(f'{conn}: Sleeping for {sleep_time} seconds until one is ready.')
                sleep(sleep_time)  # + 1 just to be sure
                for worker in qe_workers:
                    if now >= worker.quota_reset_date:
                        logging.info(f'{conn}: adding client {worker.client_id} back to queue.')
                        workerq.put(worker)
                qe_workers = [x for x in qe_workers if now < x.quota_reset_date]
                taskq.put(task)
                continue

            # Got a task and a worker.  Assign the job.
            try:
                logging.info(f'{conn}: using client {worker.client_id} to fetch {task}')
                saved_file = ScopusUtility.download_scopus_period(worker.client, conn, task, inst_id, download_path)
                saved_files.append(saved_file)
                workerq.put(worker)
            except Exception as e:
                # If quota exceeded, handle reset to date
                error_msg = repr(e)
                if error_msg.startswith("HTTPError('QuotaExceeded"):
                    renews_ts = int(error_msg[37:-2])  # Maybe? Might throw error if old code out of date.
                    logging.warning(
                        f'{conn}: Quota exceeded for client {worker.client_id} Received renews_ts: {renews_ts}')
                    # Elsevier probably gave ms even though documentation says s
                    if renews_ts >= pendulum.datetime.max.timestamp():
                        renews_ts /= 1000  # Convert back into seconds
                    worker.quota_reset_date = pendulum.from_timestamp(renews_ts)
                    taskq.put(task)
                    reset_date = min(worker.quota_reset_date, reset_date)
                    logging.warning(f'{conn}: new reset_date: {reset_date.isoformat()}')
                    qe_workers.append(worker)
                    continue
                else:
                    raise AirflowException(
                        f'{conn}: uncaught exception processing client {worker.client_id} and task {task} with message {error_msg}')
                # If some other error, raise airflow exception because there is an unexpected error

        return saved_files

    @staticmethod
    def download_worker(worker: ScopusUtilWorker, exit_event: Event, taskq: Queue, conn: str, inst_id: List[str],
                        download_path: str):
        """ Download thread. Pulls tasks from queue when available """

        timeout = 20
        saved_files = list()

        while True:
            now = pendulum.now('UTC')
            if worker.quota_reset_date > now:
                offset = (worker.quota_reset_date - now).seconds + 1
                logging.warning(f'{conn} client {worker.client_id}: cool down required. Sleeping for {offset} seconds.')
                sleep(offset)

            try:
                logging.info(f'{conn} client {worker.client_id}: attempting to get a task')
                task = taskq.get(block=True, timeout=timeout)
            except Empty:
                if exit_event.is_set():
                    logging.info(f'{conn} client {worker.client_id}: received exit event. Returning results.')
                    break
                logging.info(f'{conn} client {worker.client_id}: failed to get task but did not exit. Retrying.')
                continue

            # Got a task. Try to process it.
            try:
                logging.info(f'{conn} using client {worker.client_id}: fetching {task}')
                saved_file = ScopusUtility.download_scopus_period(worker.client, conn, task, inst_id, download_path)
                saved_files.append(saved_file)
                taskq.task_done()
            except Exception as e:
                # If quota exceeded, handle reset to date
                error_msg = repr(e)
                if error_msg.startswith("HTTPError('QuotaExceeded"):
                    renews_ts = int(error_msg[37:-2])  # Maybe? Might throw error if old code out of date.
                    logging.warning(
                        f'{conn} client {worker.client_id}: Quota exceeded, renews_ts: {renews_ts}')

                    # Elsevier probably gave ms even though documentation says s
                    if renews_ts >= pendulum.datetime.max.timestamp():
                        renews_ts /= 1000  # Convert back into seconds
                    worker.quota_reset_date = pendulum.from_timestamp(renews_ts)
                    taskq.put(task)
                    taskq.task_done()
                    continue

        return saved_files

    @staticmethod
    def download_parallel(workers: List[ScopusUtilWorker], taskq: Queue, conn: str, inst_id: List[str],
                          download_path: str):
        """ Download SCOPUS snapshot with parallel sessions. Tasks will be distributed in parallel to the available
        keys. Each key will independently fetch a task from the queue when it's free so there's no guarantee of load
        balance.

        :return: List of files downloaded.
        """

        saved_files = list()
        sessions = len(workers)
        with ThreadPoolExecutor(max_workers=sessions) as executor:
            futures = list()
            thread_exit = Event()

            for worker in workers:
                futures.append(
                    executor.submit(ScopusUtility.download_worker, worker, thread_exit, taskq, conn, inst_id,
                                    download_path))

            taskq.join()  # Wait until all tasks done
            logging.info(f'{conn}: all tasks fetched. Signalling threads to exit.')
            thread_exit.set()  # Notify for threads to return results

            for future in as_completed(futures):
                saved_files += future.result()

        return saved_files

    @staticmethod
    def download_snapshot(api_keys: List[str], release: ScopusRelease, mode: str):
        """ Download snapshot from SCOPUS for the given institution.

        """

        logging.info(f'Downloading snapshot with {mode} method.')

        schedule = build_schedule(release.start_date, release.end_date)
        task_queue = Queue()
        for period in schedule:
            task_queue.put(period)

        now = pendulum.now('UTC')
        clients = [ScopusUtilWorker(i, ElsClient(key), now) for i, key in enumerate(api_keys)]

        if mode == 'sequential':
            return ScopusUtility.download_sequential(clients, task_queue, release.inst_id, release.scopus_inst_id,
                                                     release.download_path)
        if mode == 'parallel':
            return ScopusUtility.download_parallel(clients, task_queue, release.inst_id, release.scopus_inst_id,
                                                   release.download_path)

        raise AirflowException(f'Unsupported mode {mode} received')

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
