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
from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.models.taskinstance import TaskInstance
from google.cloud import storage
from google.cloud.bigquery import SourceFormat
from pendulum import Pendulum


from academic_observatory.utils.config_utils import (
    debug_file_path,
    is_composer,
    find_schema,
    ObservatoryConfig,
    SubFolder,
    schema_path,
    telescope_path,
)
from academic_observatory.utils.data_utils import get_file
from academic_observatory.utils.gc_utils import (
    bigquery_partitioned_table_id,
    create_bigquery_dataset,
    load_bigquery_table,
    upload_file_to_cloud_storage
)
from academic_observatory.utils.proc_utils import wait_for_process
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


def download_release(url: str):
    """
    Downloads release from url.

    :param url: url of specific release
    """

    filename = filepath_download(url)
    download_dir = os.path.dirname(filename)

    # Download
    logging.info(f"Downloading file: {filename}, url: {url}")
    file_path, updated = get_file(fname=filename, origin=url, cache_dir=download_dir)

    if file_path:
        logging.info(f'Success downloading release: {filename}')
    else:
        logging.error(f"Error downloading release: {filename}")
        exit(os.EX_DATAERR)

    return file_path


def decompress_release(url: str):
    logging.info(f"Extracting file: {filepath_download(url)}")

    cmd = f"gunzip -c {filepath_download(url)} > {filepath_extract(url)}"
    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                         executable='/bin/bash')
    stdout, stderr = wait_for_process(p)
    if stdout:
        logging.info(stdout)
    if stderr:
        raise AirflowException(f"bash command failed for {url}: {stderr}")
    logging.info(f"File extracted to: {filepath_extract(url)}")

    return filepath_extract(url)


def transform_release(url: str):
    cmd = f"sed 's/authenticated-orcid/authenticated_orcid/g' {filepath_extract(url)} > " \
          f"{filepath_transform(url)}"

    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                         executable='/bin/bash')

    stdout, stderr = wait_for_process(p)
    if stdout:
        logging.info(stdout)
    if stderr:
        raise AirflowException(f"bash command failed for {url}: {stderr}")
    logging.info(f'Success transforming release: {url}')

    return filepath_transform(url)


def release_date(url: str) -> str:
    """
    Finds date in a given url.

    :param url: url of specific release
    :return: date in 'YYYY-MM-DD' format
    """
    date = re.search(r'\d{4}-\d{2}-\d{2}', url).group()

    return date


# def table_name(url: str) -> str:
#     """
#     Creates a table name that can be used in bigquery.
#
#     :param url: url of specific release
#     :return: table name
#     """
#     date = release_date(url)
#     table = f"{UnpaywallTelescope.DAG_ID}_{date}".replace('-', '_')
#
#     return table


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


class UnpaywallTelescope:
    DAG_ID = 'unpaywall'
    DESCRIPTION = 'Unpaywall'
    DATASET_ID = DAG_ID
    XCOM_MESSAGES_NAME = "messages"
    XCOM_CONFIG_NAME = "config"
    TELESCOPE_URL = 'https://unpaywall-data-snapshots.s3-us-west-2.amazonaws.com/'
    TELESCOPE_DEBUG_URL = 'https://unpaywall-data-snapshots.s3-us-west-2.amazonaws.com/unpaywall_snapshot_3000-01-27T153236.jsonl.gz'
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
        Depending on whether run from inside or outside a composer environment:
        If run from outside, checks if 'CONFIG_PATH' airflow variable is available and points to a valid config file.
        If run from inside, check if airflow variables for 'env', 'bucket' and 'project_id' are set.

        The corresponding values will be stored in a dict and pushed with xcom, so they can be accessed in consequent
        tasks.

        kwargs is required to access task instance
        :param kwargs: NA
        """
        invalid_list = []
        config_dict = {}

        if is_composer():
            environment = Variable.get('ENV', default_var=None)
            bucket = Variable.get('BUCKET', default_var=None)
            project_id = Variable.get('PROJECT_ID', default_var=None)

            for var in [(environment, 'ENV'), (bucket, 'BUCKET'), (project_id, 'PROJECT_ID')]:
                if var[0] is None:
                    invalid_list.append(f"Airflow variable '{var[1]}' not set with terraform.")

        else:
            default_config = ObservatoryConfig.CONTAINER_DEFAULT_PATH
            config_path = Variable.get('CONFIG_PATH', default_var=default_config)

            config = ObservatoryConfig.load(config_path)
            if config is not None:
                config_is_valid = config.is_valid
                if not config_is_valid:
                    invalid_list.append(f'Config file not valid: {config_is_valid}')
            if not config:
                invalid_list.append(f'Config file does not exist')

            environment = config.environment.value
            bucket = config.bucket_name
            project_id = config.project_id

        if invalid_list:
            for invalid_reason in invalid_list:
                logging.warning("-" + invalid_reason + "\n\n")
            raise AirflowException
        else:
            config_dict['environment'] = environment
            config_dict['bucket'] = bucket
            config_dict['project_id'] = project_id
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
        logging.info('Releases between current and next execution date:')
        for release_url in releases_list:
            logging.info(release_url)

            date = release_date(release_url)
            released_date: Pendulum = pendulum.parse(date)
            table_id = bigquery_partitioned_table_id(UnpaywallTelescope.DAG_ID, released_date)

            if execution_date <= released_date < next_execution_date:
                table_exists = bq_hook.table_exists(
                    project_id=project_id,
                    dataset_id=UnpaywallTelescope.DATASET_ID,
                    table_id=table_id
                )
                logging.info('Checking if bigquery table already exists:')
                if table_exists:
                    logging.info(f'- Table exists for {release_url}: {project_id}.{UnpaywallTelescope.DATASET_ID}.{table_id}')
                else:
                    logging.info(f"- Table doesn't exist yet, processing {release_url} in this workflow")
                    release_urls_out.append(release_url)

        # Push messages
        ti: TaskInstance = kwargs['ti']
        ti.xcom_push(UnpaywallTelescope.XCOM_MESSAGES_NAME, release_urls_out)
        return UnpaywallTelescope.TASK_ID_DOWNLOAD if release_urls_out else UnpaywallTelescope.TASK_ID_STOP

    @staticmethod
    def download(**kwargs):
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
    def decompress(**kwargs):
        """
        Unzip release to new file.

        kwargs is required to access task instance
        """
        # Pull messages
        release_urls, environment, bucket, project_id = xcom_pull_info(kwargs['ti'])

        for url in release_urls:
            decompress_release(url)

    @staticmethod
    def transform(**kwargs):
        """
        Transform release with sed command and save to new file.

        kwargs is required to access task instance
        """
        # Pull messages
        release_urls, environment, bucket, project_id = xcom_pull_info(kwargs['ti'])

        for url in release_urls:
            transform_release(url)

    @staticmethod
    def upload_to_gcs(**kwargs):
        """
        Upload transformed release to a google cloud storage bucket.

        kwargs is required to access task instance
        """
        # Pull messages
        release_urls, environment, bucket, project_id = xcom_pull_info(kwargs['ti'])

        for url in release_urls:
            blob_name = f'telescopes/unpaywall/transformed/{os.path.basename(filepath_transform(url))}'
            upload_file_to_cloud_storage(bucket, blob_name, file_path=filepath_transform(url))

            # Push messages
            ti: TaskInstance = kwargs['ti']
            ti.xcom_push('blob_name', blob_name)

    @staticmethod
    def load_to_bq(**kwargs):
        """
        Upload transformed release to a bigquery table.

        kwargs is required to access task instance
        """
        release_urls, environment, bucket, project_id = xcom_pull_info(kwargs['ti'])

        # Get bucket location
        storage_client = storage.Client()
        bucket_object = storage_client.get_bucket(bucket)
        location = bucket_object.location

        # Create dataset
        dataset_id = UnpaywallTelescope.DAG_ID
        create_bigquery_dataset(project_id, dataset_id, location, UnpaywallTelescope.DESCRIPTION)

        # Pull messages
        ti: TaskInstance = kwargs['ti']
        release_urls = ti.xcom_pull(key=UnpaywallTelescope.XCOM_MESSAGES_NAME, task_ids=UnpaywallTelescope.TASK_ID_LIST,
                                    include_prior_dates=False)
        blob_name = ti.xcom_pull(key='blob_name', task_ids=UnpaywallTelescope.TASK_ID_UPLOAD,
                                 include_prior_dates=False)
        for url in release_urls:
            # get release_date
            date = release_date(url)
            released_date: Pendulum = pendulum.parse(date)
            table_id = bigquery_partitioned_table_id(UnpaywallTelescope.DAG_ID, released_date)

            # Select schema file based on release date
            analysis_schema_path = schema_path('telescopes')
            schema_file_path = find_schema(analysis_schema_path, UnpaywallTelescope.DAG_ID, released_date)
            if schema_file_path is None:
                logging.error(f'No schema found with search parameters: analysis_schema_path={analysis_schema_path}, '
                              f'table_name={UnpaywallTelescope.DAG_ID}, release_date={release_date}')
                exit(os.EX_CONFIG)

            # Load BigQuery table
            uri = f"gs://{bucket}/{blob_name}"
            logging.info(f"URI: {uri}")
            load_bigquery_table(uri, dataset_id, location, table_id, schema_file_path,
                                SourceFormat.NEWLINE_DELIMITED_JSON)

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
