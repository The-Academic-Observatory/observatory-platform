import json
import logging
import os
import pathlib
import re
import subprocess
import shutil
import xml.etree.ElementTree as ET
from typing import Tuple

import pendulum
from airflow import settings
from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.exceptions import AirflowException
from airflow.models import Connection
from airflow.models import Variable
from airflow.models.taskinstance import TaskInstance
from pendulum import Pendulum


from academic_observatory.utils.config_utils import (
    bigquery_schema_path,
    debug_file_path,
    is_vendor_google,
    ObservatoryConfig,
    SubFolder,
    telescope_path,
)
from academic_observatory.utils.data_utils import get_file
from academic_observatory.utils.url_utils import retry_session


def xcom_pull_info(ti: TaskInstance) -> Tuple[list, str, str, str]:
    """
    Pulls xcom messages, release_urls and config_dict.
    Parses retrieved config_dict and returns those values next to release_urls.

    :param ti:
    :return: release_urls, environment, bucket, project_id
    """
    release_urls = ti.xcom_pull(key=UnpaywallTelescope.XCOM_MESSAGES_NAME, task_ids=UnpaywallTelescope.TASK_ID_LIST,
                                include_prior_dates=False)
    config_dict = ti.xcom_pull(key=UnpaywallTelescope.XCOM_CONFIG_NAME, task_ids=UnpaywallTelescope.TASK_ID_SETUP,
                               include_prior_dates=False)
    environment = config_dict['environment']
    bucket = config_dict['bucket']
    project_id = config_dict['project_id']
    return release_urls, environment, bucket, project_id


def list_releases(telescope_url: str) -> list:
    """
    Parses xml string retrieved from GET request to create list of urls for
    different releases.

    :param telescope_url: url on which to execute GET request
    :return: snapshot_list, list of urls
    """
    snapshot_list = []

    xml_string = retry_session().get(telescope_url).text
    if xml_string:
        root = ET.fromstring(xml_string)
        for release in root.findall('.//{http://s3.amazonaws.com/doc/2006-03-01/}Key'):
            snapshot_url = os.path.join(telescope_url, release.text)
            snapshot_list.append(snapshot_url)

    return snapshot_list


def release_date(url: str) -> str:
    """
    Finds date in a given url.

    :param url: url of specific release
    :return: date in 'YYYY-MM-DD' format
    """
    date = re.search(r'\d{4}-\d{2}-\d{2}', url).group()

    return date


def table_name(url: str) -> str:
    """
    Creates a table name that can be used in bigquery.

    :param url: url of specific release
    :return: table name
    """
    date = release_date(url)
    table = f"{UnpaywallTelescope.DAG_ID}_{date}".replace('-', '_')

    return table


def filepath_download(url: str) -> str:
    """
    Gives path to downloaded release.

    :param url: url of specific release
    :return: absolute file path
    """
    date = release_date(url)
    compressed_file_name = f"{UnpaywallTelescope.DAG_ID}_{date}.jsonl.gz".replace('-', '_')
    download_dir = telescope_path(UnpaywallTelescope.DAG_ID, SubFolder.downloaded)
    path = os.path.join(download_dir, compressed_file_name)

    return path


def filepath_extract(url: str) -> str:
    """
    Gives path to extracted and decompressed release.

    :param url: url of specific release
    :return: absolute file path
    """
    date = release_date(url)
    decompressed_file_name = f"{UnpaywallTelescope.DAG_ID}_{date}.jsonl".replace('-', '_')
    extract_dir = telescope_path(UnpaywallTelescope.DAG_ID, SubFolder.extracted)
    path = os.path.join(extract_dir, decompressed_file_name)

    return path


def filepath_transform(url: str) -> str:
    """
    Gives path to transformed release.

    :param url: url of specific release
    :return: absolute file path
    """
    date = release_date(url)
    decompressed_file_name = f"{UnpaywallTelescope.DAG_ID}_{date}.jsonl".replace('-', '_')
    transform_dir = telescope_path(UnpaywallTelescope.DAG_ID, SubFolder.transformed)
    path = os.path.join(transform_dir, decompressed_file_name)

    return path


def download_release(url: str):
    """
    Downloads release from url.

    :param url: url of specific release
    """
    filename = filepath_download(url)
    download_dir = os.path.dirname(filename)
    get_file(fname=filename, origin=url, cache_dir=download_dir)


class UnpaywallTelescope:
    DAG_ID = 'unpaywall'
    DATASET_ID = DAG_ID
    XCOM_MESSAGES_NAME = "messages"
    XCOM_CONFIG_NAME = "config"
    TELESCOPE_URL = 'https://unpaywall-data-snapshots.s3-us-west-2.amazonaws.com/'
    TELESCOPE_DEBUG_URL = 'https://unpaywall-data-snapshots.s3-us-west-2.amazonaws.com/unpaywall_snapshot_3000-01-27T153236.jsonl.gz'
    SCHEMA_FILE_PATH = bigquery_schema_path('unpaywall.json')
    DEBUG_FILE_PATH = debug_file_path('unpaywall.jsonl.gz')

    TASK_ID_SETUP = "check_setup_requirements"
    TASK_ID_LIST = f"list_{DAG_ID}_releases"
    TASK_ID_STOP = f"stop_{DAG_ID}_workflow"
    TASK_ID_DOWNLOAD = f"download_{DAG_ID}_releases"
    TASK_ID_DECOMPRESS = f"decompress_{DAG_ID}_releases"
    TASK_ID_TRANSFORM = f"transform_{DAG_ID}_releases"
    TASK_ID_UPLOAD = f"upload_{DAG_ID}_releases"
    TASK_ID_BQ_LOAD = f"bq_load_{DAG_ID}_releases"
    TASK_ID_CLEANUP = f"cleanup_{DAG_ID}_releases"

    @staticmethod
    def check_setup_requirements(**kwargs):
        """
        Checks two set-up requirements:
        - 'CONFIG_PATH' airflow variable available
        - existing/valid config file
        If these are both valid it will store the values from the config file in dict and push them to xcom, so they can
        be accessed in consequent tasks.

        kwargs is required to access task instance
        :param kwargs: NA
        """
        invalid_list = []
        config_dict = {}

        if is_vendor_google():
            default_config = None
        else:
            default_config = ObservatoryConfig.CONTAINER_DEFAULT_PATH

        config_path = Variable.get('CONFIG_PATH', default_var=default_config)
        if config_path is None:
            logging.info("'CONFIG_PATH' airflow variable not set, please set in UI")

        config_valid, config_validator, config = ObservatoryConfig.load(config_path)
        if not config_valid:
            invalid_list.append(f'Config file not valid: {config_validator}')

        if invalid_list:
            for invalid_reason in invalid_list:
                logging.info("-", invalid_reason, "\n\n")
                raise AirflowException
        else:
            config_dict['environment'] = config.environment.value
            config_dict['bucket'] = config.bucket_name
            config_dict['project_id'] = config.project_id
            # Push messages
            ti: TaskInstance = kwargs['ti']
            ti.xcom_push(UnpaywallTelescope.XCOM_CONFIG_NAME, config_dict)

    @staticmethod
    def list_releases_last_month(**kwargs):
        """
        Based on a list of all releases, checks which ones were released between this and the next execution date of the
        DAG.
        If the release falls within the time period mentioned above, checks if a bigquery table doesn't exist yet for
        the release.
        A list of releases that passed both checks is passed to the next tasks. If the list is empty the workflow will
        stop.

        kwargs is required to access task instance
        """
        # Pull messages
        release_urls, environment, bucket, project_id = xcom_pull_info(kwargs['ti'])

        if environment == 'dev':
            release_urls_out = [UnpaywallTelescope.TELESCOPE_DEBUG_URL]
            # Push messages
            ti: TaskInstance = kwargs['ti']
            ti.xcom_push(UnpaywallTelescope.XCOM_MESSAGES_NAME, release_urls_out)
            return UnpaywallTelescope.TASK_ID_DOWNLOAD if release_urls_out else UnpaywallTelescope.TASK_ID_STOP

        execution_date = kwargs['execution_date']
        next_execution_date = kwargs['next_execution_date']
        releases_list = list_releases(UnpaywallTelescope.TELESCOPE_URL)
        logging.info(f'All releases:\n{releases_list}\n')

        bq_hook = BigQueryHook()
        # Select the releases that were published on or after the execution_date and before the next_execution_date
        release_urls_out = []
        logging.info('All releases between current and next execution date:')
        for release_url in releases_list:
            date = release_date(release_url)
            published_date: Pendulum = pendulum.parse(date)

            if execution_date <= published_date < next_execution_date:
                table_exists = bq_hook.table_exists(
                    project_id=project_id,
                    dataset_id=UnpaywallTelescope.DATASET_ID,
                    table_id=table_name(release_url)
                )
                if table_exists:
                    logging.info(f'Table exists: {project_id}.{UnpaywallTelescope.DATASET_ID}.{table_name(release_url)}')
                else:
                    logging.info(f"Table doesn't exist yet, processing {release_url} in this workflow")
                    release_urls_out.append(release_url)

        # Push messages
        ti: TaskInstance = kwargs['ti']
        ti.xcom_push(UnpaywallTelescope.XCOM_MESSAGES_NAME, release_urls_out)
        return UnpaywallTelescope.TASK_ID_DOWNLOAD if release_urls_out else UnpaywallTelescope.TASK_ID_STOP

    @staticmethod
    def download_releases(**kwargs):
        """
        Download release to file.
        If dev environment, copy debug file from this repository to the right location. Else download from url.

        kwargs is required to access task instance
        """
        # Pull messages
        release_urls, environment, bucket, project_id = xcom_pull_info(kwargs['ti'])

        for url in release_urls:
            if environment == 'dev':
                shutil.copy(UnpaywallTelescope.DEBUG_FILE_PATH, filepath_download(url))

            else:
                download_release(url)

    @staticmethod
    def decompress_release(**kwargs):
        """
        Unzip release to new file.

        kwargs is required to access task instance
        """
        # Pull messages
        release_urls, environment, bucket, project_id = xcom_pull_info(kwargs['ti'])

        for url in release_urls:
            cmd = f"gunzip -c {filepath_download(url)} > {filepath_extract(url)}"

            p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                 executable='/bin/bash')
            stdout, stderr = p.communicate()
            if stdout:
                logging.info(stdout)
            if stderr:
                raise AirflowException(f"bash command failed for {url}: {stderr}")

    @staticmethod
    def transform_release(**kwargs):
        """
        Transform release with sed command and save to new file.

        kwargs is required to access task instance
        """
        # Pull messages
        release_urls, environment, bucket, project_id = xcom_pull_info(kwargs['ti'])

        for url in release_urls:
            cmd = f"sed 's/authenticated-orcid/authenticated_orcid/g' {filepath_extract(url)} > " \
                  f"{filepath_transform(url)}"

            p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                 executable='/bin/bash')
            stdout, stderr = p.communicate()
            if stdout:
                logging.info(stdout)
            if stderr:
                raise AirflowException(f"bash command failed for {url}: {stderr}")

    @staticmethod
    def upload_release_to_gcs(**kwargs):
        """
        Upload transformed release to a google cloud storage bucket.

        kwargs is required to access task instance
        """
        # Pull messages
        release_urls, environment, bucket, project_id = xcom_pull_info(kwargs['ti'])

        gcs_hook = GoogleCloudStorageHook()

        for url in release_urls:
            gcs_hook.upload(
                bucket=bucket,
                object=os.path.basename(filepath_transform(url)),
                filename=filepath_transform(url)
            )

    @staticmethod
    def load_release_to_bq(**kwargs):
        """
        Upload transformed release to a bigquery table.

        kwargs is required to access task instance
        """
        release_urls, environment, bucket, project_id = xcom_pull_info(kwargs['ti'])

        if environment == 'dev':
            # Create custom bq hook with a project id added to the bigquery connection, prevents error
            # 'ValueError: INTERNAL: No default project is specified'
            session = settings.Session()
            bq_conn = Connection(
                conn_id='bigquery_custom',
                conn_type='google_cloud_platform',
            )

            conn_extra_json = json.dumps({'extra__google_cloud_platform__project': project_id})
            bq_conn.set_extra(conn_extra_json)

            session.query(Connection).filter(Connection.conn_id == bq_conn.conn_id).delete()
            session.add(bq_conn)
            session.commit()

            bq_hook = BigQueryHook(bigquery_conn_id='bigquery_custom')
        else:
            bq_hook = BigQueryHook()

        with open(UnpaywallTelescope.SCHEMA_FILE_PATH, 'r') as json_file:
            schema_fields = json.loads(json_file.read())

        conn = bq_hook.get_conn()
        cursor = conn.cursor()

        # Pull messages
        ti: TaskInstance = kwargs['ti']
        release_urls = ti.xcom_pull(key=UnpaywallTelescope.XCOM_MESSAGES_NAME, task_ids=UnpaywallTelescope.TASK_ID_LIST, include_prior_dates=False)
        for url in release_urls:
            # Check if dataset already exists and create dataset if it doesn't exist yet.
            try:
                cursor.create_empty_dataset(
                    dataset_id=UnpaywallTelescope.DATASET_ID,
                    project_id=project_id
                )
            except AirflowException as e:
                logging.info(f"Dataset already exists, continuing to create table. See traceback:\n{e}")

            # Upload to bigquery
            cursor.run_load(
                destination_project_dataset_table=f"{UnpaywallTelescope.DATASET_ID}.{table_name(url)}",
                source_uris=f"gs://{bucket}/{os.path.basename(filepath_transform(url))}",
                schema_fields=schema_fields,
                autodetect=False,
                source_format='NEWLINE_DELIMITED_JSON',
            )

    @staticmethod
    def cleanup_releases(**kwargs):
        """
        Delete files of downloaded, extracted and transformed release.

        kwargs is required to access task instance
        """
        # Pull messages
        release_urls, environment, bucket, project_id = xcom_pull_info(kwargs['ti'])

        for url in release_urls:
            try:
                pathlib.Path(filepath_download(url)).unlink()
            except FileNotFoundError as e:
                logging.warning(f"No such file or directory {filepath_download(url)}: {e}")

            try:
                pathlib.Path(filepath_extract(url)).unlink()
            except FileNotFoundError as e:
                logging.warning(f"No such file or directory {filepath_extract(url)}: {e}")

            try:
                pathlib.Path(filepath_transform(url)).unlink()
            except FileNotFoundError as e:
                logging.warning(f"No such file or directory {filepath_transform(url)}: {e}")
