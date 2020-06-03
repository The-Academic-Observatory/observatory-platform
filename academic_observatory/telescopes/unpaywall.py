import os
import re
import json
import pendulum
import subprocess
import pathlib
import shutil
import logging
import xml.etree.ElementTree as ET
from pendulum import Pendulum

from airflow import settings
from airflow.exceptions import AirflowException
from airflow.models import Connection
from airflow.models import Variable
from airflow.models.taskinstance import TaskInstance
from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook

from academic_observatory.utils.url_utils import retry_session
from academic_observatory.utils.data_utils import get_file
from academic_observatory.utils.config_utils import ObservatoryConfig, SubFolder, telescope_path, bigquery_schema_path, debug_file_path


def list_releases(telescope_url):
    snapshot_list = []

    xml_string = retry_session().get(telescope_url).text
    if xml_string:
        # parse xml file and get list of snapshots
        root = ET.fromstring(xml_string)
        for release in root.findall('.//{http://s3.amazonaws.com/doc/2006-03-01/}Key'):
            snapshot_url = os.path.join(telescope_url, release.text)
            snapshot_list.append(snapshot_url)

    return snapshot_list


def release_date_from_url(url: str):
    date = re.search(r'\d{4}-\d{2}-\d{2}', url).group()

    return date


def table_name_from_url(url: str):
    release_date = release_date_from_url(url)
    table_name = f"{UnpaywallTelescope.DAG_ID}_{release_date}".replace('-', '_')

    return table_name


def filepath_download(url: str):
    release_date = release_date_from_url(url)
    compressed_file_name = f"{UnpaywallTelescope.DAG_ID}_{release_date}.jsonl.gz".replace('-', '_')
    download_dir = telescope_path(UnpaywallTelescope.DAG_ID, SubFolder.downloaded)
    path = os.path.join(download_dir, compressed_file_name)

    return path


def filepath_extract(url: str):
    release_date = release_date_from_url(url)
    decompressed_file_name = f"{UnpaywallTelescope.DAG_ID}_{release_date}.jsonl".replace('-', '_')
    extract_dir = telescope_path(UnpaywallTelescope.DAG_ID, SubFolder.extracted)
    path = os.path.join(extract_dir, decompressed_file_name)

    return path


def filepath_transform(url: str):
    release_date = release_date_from_url(url)
    decompressed_file_name = f"{UnpaywallTelescope.DAG_ID}_{release_date}.jsonl".replace('-', '_')
    transform_dir = telescope_path(UnpaywallTelescope.DAG_ID, SubFolder.transformed)
    path = os.path.join(transform_dir, decompressed_file_name)

    return path


def download_release(url: str):
    filename = filepath_download(url)
    download_dir = os.path.dirname(filename)
    get_file(fname=filename, origin=url, cache_dir=download_dir)


class UnpaywallTelescope:
    # example: https://unpaywall-data-snapshots.s3-us-west-2.amazonaws.com/unpaywall_snapshot_2020-04-27T153236.jsonl.gz
    TELESCOPE_URL = 'https://unpaywall-data-snapshots.s3-us-west-2.amazonaws.com/'
    TELESCOPE_DEBUG_URL = 'https://unpaywall-data-snapshots.s3-us-west-2.amazonaws.com/unpaywall_snapshot_3000-01-27T153236.jsonl.gz'
    SCHEMA_FILE_PATH = bigquery_schema_path('unpaywall_schema.json')
    DEBUG_FILE_PATH = debug_file_path('unpaywall_debug.jsonl.gz')

    DAG_ID = 'unpaywall'
    DATASET_ID = DAG_ID
    XCOM_MESSAGES_NAME = "messages"
    XCOM_CONFIG_NAME = "config"
    TASK_ID_CONFIG = f"get_config_variables"
    TASK_ID_LIST = f"list_{DAG_ID}_releases"
    TASK_ID_STOP = f"stop_{DAG_ID}_workflow"
    TASK_ID_DOWNLOAD = f"download_{DAG_ID}_releases"
    TASK_ID_DECOMPRESS = f"decompress_{DAG_ID}_releases"
    TASK_ID_TRANSFORM = f"transform_{DAG_ID}_releases"
    TASK_ID_UPLOAD = f"upload_{DAG_ID}_releases"
    TASK_ID_BQ_LOAD = f"bq_load_{DAG_ID}_releases"
    TASK_ID_CLEANUP = f"cleanup_{DAG_ID}_releases"

    @staticmethod
    def xcom_pull_messages(ti):
        # Pull messages
        msgs_in = ti.xcom_pull(key=UnpaywallTelescope.XCOM_MESSAGES_NAME, task_ids=UnpaywallTelescope.TASK_ID_LIST,
                               include_prior_dates=False)
        config_dict = ti.xcom_pull(key=UnpaywallTelescope.XCOM_CONFIG_NAME, task_ids=UnpaywallTelescope.TASK_ID_CONFIG,
                                   include_prior_dates=False)
        environment = config_dict['environment']
        bucket = config_dict['bucket']
        project_id = config_dict['project_id']
        return msgs_in, environment, bucket, project_id

    @staticmethod
    def get_config_variables(**kwargs):
        config_dict = {}

        config_path = Variable.get('CONFIG_PATH', default_var=None)
        if config_path is None:
            print("'CONFIG_FILE' airflow variable not set, please set in UI")

        config_valid, config_validator, config = ObservatoryConfig.load(config_path)
        if config_valid:
            config_dict['environment'] = config.environment.value
            config_dict['bucket'] = config.bucket_name
            config_dict['project_id'] = config.project_id
        else:
            print(f'config file not valid: ', config_validator)

        # Push messages
        ti: TaskInstance = kwargs['ti']
        ti.xcom_push(UnpaywallTelescope.XCOM_CONFIG_NAME, config_dict)
        return UnpaywallTelescope.TASK_ID_LIST if config_dict else UnpaywallTelescope.TASK_ID_STOP

    @staticmethod
    def list_releases_last_month(**kwargs):
        # Pull messages
        msgs_in, environment, bucket, project_id = UnpaywallTelescope.xcom_pull_messages(kwargs['ti'])

        if environment == 'dev':
            msgs_out = [UnpaywallTelescope.TELESCOPE_DEBUG_URL]
            # Push messages
            ti: TaskInstance = kwargs['ti']
            ti.xcom_push(UnpaywallTelescope.XCOM_MESSAGES_NAME, msgs_out)
            return UnpaywallTelescope.TASK_ID_DOWNLOAD if msgs_out else UnpaywallTelescope.TASK_ID_STOP

        execution_date = kwargs['execution_date']
        next_execution_date = kwargs['next_execution_date']
        releases_list = list_releases(UnpaywallTelescope.TELESCOPE_URL)

        bq_hook = BigQueryHook()
        # Select the releases that were published on or after the execution_date and before the next_execution_date
        msgs_out = []
        for release_url in releases_list:
            release_date = release_date_from_url(release_url)
            published_date: Pendulum = pendulum.parse(release_date)

            if execution_date <= published_date < next_execution_date:
                table_exists = bq_hook.table_exists(
                    project_id=project_id,
                    dataset_id=UnpaywallTelescope.DATASET_ID,
                    table_id=table_name_from_url(release_url)
                )
                print(f'bq table {project_id}.{UnpaywallTelescope.DATASET_ID}.{table_name_from_url(release_url)} already exists.')
                if not table_exists:
                    msgs_out.append(release_url)

        # Push messages
        ti: TaskInstance = kwargs['ti']
        ti.xcom_push(UnpaywallTelescope.XCOM_MESSAGES_NAME, msgs_out)
        return UnpaywallTelescope.TASK_ID_DOWNLOAD if msgs_out else UnpaywallTelescope.TASK_ID_STOP

    @staticmethod
    def download_releases_local(**kwargs):
        # Pull messages
        msgs_in, environment, bucket, project_id = UnpaywallTelescope.xcom_pull_messages(kwargs['ti'])

        for msg_in in msgs_in:
            if environment == 'dev':
                shutil.copy(UnpaywallTelescope.DEBUG_FILE_PATH, filepath_download(msg_in))

            else:
                download_release(msg_in)

    @staticmethod
    def decompress_release(**kwargs):
        # Pull messages
        msgs_in, environment, bucket, project_id = UnpaywallTelescope.xcom_pull_messages(kwargs['ti'])

        for msg_in in msgs_in:
            cmd = f"gunzip -c {filepath_download(msg_in)} > {filepath_extract(msg_in)}"

            p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                 executable='/bin/bash')
            stdout, stderr = p.communicate()
            if stdout:
                logging.info(stdout)
            if stderr:
                logging.warning(f"bash command failed for {msg_in}: {stderr}")
                return -1

    @staticmethod
    def transform_release(**kwargs):
        # Pull messages
        msgs_in, environment, bucket, project_id = UnpaywallTelescope.xcom_pull_messages(kwargs['ti'])

        for msg_in in msgs_in:
            cmd = f"sed 's/authenticated-orcid/authenticated_orcid/g' {filepath_extract(msg_in)} > " \
                  f"{filepath_transform(msg_in)}"

            p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                 executable='/bin/bash')
            stdout, stderr = p.communicate()
            if stdout:
                logging.info(stdout)
            if stderr:
                logging.warning(f"bash command failed for {msg_in}: {stderr}")
                return -1

    @staticmethod
    def upload_release_to_gcs(**kwargs):
        # Pull messages
        msgs_in, environment, bucket, project_id = UnpaywallTelescope.xcom_pull_messages(kwargs['ti'])

        gcs_hook = GoogleCloudStorageHook()

        for msg_in in msgs_in:
            gcs_hook.upload(
                bucket=bucket,
                object=os.path.basename(filepath_transform(msg_in)),
                filename=filepath_transform(msg_in)
            )

    @staticmethod
    def load_release_to_bq(**kwargs):
        # Add a project id to the bigquery connection, prevents error 'ValueError: INTERNAL: No default project is
        # specified'
        msgs_in, environment, bucket, project_id = UnpaywallTelescope.xcom_pull_messages(kwargs['ti'])

        if environment == 'dev':
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
        msgs_in = ti.xcom_pull(key=UnpaywallTelescope.XCOM_MESSAGES_NAME, task_ids=UnpaywallTelescope.TASK_ID_LIST, include_prior_dates=False)
        for msg_in in msgs_in:
            try:
                cursor.create_empty_dataset(
                    dataset_id=UnpaywallTelescope.DATASET_ID,
                    project_id=project_id
                )
            except AirflowException as e:
                print(f"Dataset already exists, continuing to create table. See error message:\n{e}")

            cursor.run_load(
                destination_project_dataset_table=f"{UnpaywallTelescope.DATASET_ID}.{table_name_from_url(msg_in)}",
                source_uris=f"gs://{bucket}/{os.path.basename(filepath_transform(msg_in))}",
                schema_fields=schema_fields,
                autodetect=False,
                source_format='NEWLINE_DELIMITED_JSON',
            )

    @staticmethod
    def cleanup_releases(**kwargs):
        # Pull messages
        msgs_in, environment, bucket, project_id = UnpaywallTelescope.xcom_pull_messages(kwargs['ti'])

        for msg_in in msgs_in:
            try:
                pathlib.Path(filepath_download(msg_in)).unlink()
            except FileNotFoundError as e:
                logging.warning(f"No such file or directory {filepath_download(msg_in)}: {e}")

            try:
                pathlib.Path(filepath_extract(msg_in)).unlink()
            except FileNotFoundError as e:
                logging.warning(f"No such file or directory {filepath_extract(msg_in)}: {e}")

            try:
                pathlib.Path(filepath_transform(msg_in)).unlink()
            except FileNotFoundError as e:
                logging.warning(f"No such file or directory {filepath_transform(msg_in)}: {e}")
