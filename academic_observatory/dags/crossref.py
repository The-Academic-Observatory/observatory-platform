"""
Parses response containing info on crossref releases.
Based on the returned info it checks whether there is an existing bigquery table with the same release.
If the table doesn't exist yet, it will trigger a separate dag 'crossref.py'.
"""
import os
import json
import pendulum
import subprocess
import pathlib
import logging
from shutil import rmtree
from pendulum import Pendulum
from datetime import datetime
from airflow import DAG
from airflow import settings
from airflow.models import Connection
from airflow.models.taskinstance import TaskInstance
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from academic_observatory.telescopes.crossref.crossref import CrossrefRelease, list_crossref_releases

default_args = {
    "owner": "Airflow",
    "start_date": datetime(2020, 5, 1)
}

data_source = 'crossref'
TOPIC_NAME = "messages"
TASK_ID_LIST = f"list_{data_source}_releases"
TASK_ID_DOWNLOAD = f"download_{data_source}_releases"
TASK_ID_DECOMPRESS = f"decompress_{data_source}_releases"
TASK_ID_TRANSFORM = f"transform_{data_source}_releases"
TASK_ID_UPLOAD = f"upload_{data_source}_releases"
TASK_ID_BQ_LOAD = f"bq_load_{data_source}_releases"
TASK_ID_CLEANUP = f"cleanup_{data_source}_releases"

# TODO get from environment, set e.g. to 'production' when using terraform.
testing_environment = 'dev'
# testing_environment='prod'
# TODO don't hardcode these variables, not sure if they should be coming from Terraform or a separate external file
PROJECT_ID = 'workflows-dev'
DATASET_ID = 'crossref'
BUCKET = 'workflows-dev-910922156021-test'


def list_releases_last_month(**kwargs):
    if testing_environment == 'dev':
        msgs_out = [CrossrefRelease.debug_url]
        # Push messages
        ti: TaskInstance = kwargs['ti']
        ti.xcom_push(TOPIC_NAME, msgs_out)
        return

    execution_date = kwargs['execution_date']
    next_execution_date = kwargs['next_execution_date']
    releases_list = list_crossref_releases()

    bq_hook = BigQueryHook()
    # Select the GRID releases that were published on or after the execution_date and before the next_execution_date
    msgs_out = []
    for release in releases_list:
        crossref_release = CrossrefRelease(release)
        published_date: Pendulum = pendulum.parse(crossref_release.release_date)

        if execution_date <= published_date < next_execution_date:
            table_exists = bq_hook.table_exists(
                project_id=PROJECT_ID,
                dataset_id=DATASET_ID,
                table_id=crossref_release.table_name
            )
            if not table_exists:
                msgs_out.append(crossref_release.url)

    # Push messages
    ti: TaskInstance = kwargs['ti']
    ti.xcom_push(TOPIC_NAME, msgs_out)


def download_releases_local(**kwargs):
    # Pull messages
    ti: TaskInstance = kwargs['ti']
    msgs_in = ti.xcom_pull(key=TOPIC_NAME, task_ids=TASK_ID_LIST, include_prior_dates=False)

    for msg_in in msgs_in:
        crossref_release = CrossrefRelease(msg_in)

        if testing_environment == 'dev':
            gcs_conn = GoogleCloudStorageHook()
            gcs_conn.download(
                bucket=BUCKET,
                object='crossref_debug.json.tar.gz',
                filename=os.path.join(crossref_release.download_path, crossref_release.compressed_file_name)
            )
        else:
            crossref_release.download_crossref_release()


def decompress_release(**kwargs):
    # Pull messages
    ti: TaskInstance = kwargs['ti']
    msgs_in = ti.xcom_pull(key=TOPIC_NAME, task_ids=TASK_ID_LIST, include_prior_dates=False)

    for msg_in in msgs_in:
        crossref_release = CrossrefRelease(msg_in)
        extract_dir = os.path.join(crossref_release.extracted_path, crossref_release.table_name)
        tar_file_path = os.path.join(crossref_release.download_path, crossref_release.compressed_file_name)
        cmd = f"mkdir -p {extract_dir}; tar -xOzf {tar_file_path} -C {extract_dir}"

        p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, executable='/bin/bash')
        stdout, stderr = p.communicate()
        if stdout:
            logging.info(stdout)
        if stderr:
            logging.warning(f"bash command:\n{cmd}\n failed for {crossref_release.compressed_file_name}: {stderr}")
            return -1


def transform_release(**kwargs):
    # Pull messages
    ti: TaskInstance = kwargs['ti']
    msgs_in = ti.xcom_pull(key=TOPIC_NAME, task_ids=TASK_ID_LIST, include_prior_dates=False)

    for msg_in in msgs_in:
        crossref_release = CrossrefRelease(msg_in)
        extract_dir = os.path.join(crossref_release.extracted_path, crossref_release.table_name)
        transformed_file_path = os.path.join(crossref_release.transformed_path, crossref_release.decompressed_file_name)
        cmd = f"for file in {extract_dir}/*json; do " \
              "sed -E -e 's/\]\]/\]/g' -e 's/\[\[/\[/g' -e 's/,[[:blank:]]*$//g'  -e 's/([[:alpha:]])-([[" \
              ":alpha:]])/\\1_\\2/g' -e 's/\"timestamp\":_/\"timestamp\":/g' -e 's/\"date_parts\":\[" \
              "null\]/\"date_parts\":\[\]/g' -e 's/^\{\"items\":\[//g' -e '/^\}$/d' -e '/^\]$/d' $file" \
              f" >> {transformed_file_path}; done"

        p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, executable='/bin/bash')
        stdout, stderr = p.communicate()
        if stdout:
            logging.info(stdout)
        if stderr:
            logging.warning(f"bash command:\n{cmd}\n failed for {crossref_release.decompressed_file_name}: {stderr}")
            return -1


def upload_release_to_gcs(**kwargs):
    gcs_hook = GoogleCloudStorageHook()

    # Pull messages
    ti: TaskInstance = kwargs['ti']
    msgs_in = ti.xcom_pull(key=TOPIC_NAME, task_ids=TASK_ID_LIST, include_prior_dates=False)
    for msg_in in msgs_in:
        crossref_release = CrossrefRelease(msg_in)
        gcs_hook.upload(
            bucket=BUCKET,
            object=crossref_release.decompressed_file_name,
            filename=os.path.join(crossref_release.transformed_path, crossref_release.decompressed_file_name)
        )


def load_release_to_bq(**kwargs):
    # Add a project id to the bigquery connection, prevents error 'ValueError: INTERNAL: No default project is
    # specified'
    if testing_environment == 'dev':
        session = settings.Session()
        bq_conn = Connection(
            conn_id='bigquery_custom',
            conn_type='google_cloud_platform',
        )

        conn_extra_json = json.dumps({'extra__google_cloud_platform__project': PROJECT_ID})
        bq_conn.set_extra(conn_extra_json)

        session.query(Connection).filter(Connection.conn_id == bq_conn.conn_id).delete()
        session.add(bq_conn)
        session.commit()

        bq_hook = BigQueryHook(bigquery_conn_id='bigquery_custom')
    else:
        bq_hook = BigQueryHook()

    gcs_hook = GoogleCloudStorageHook()
    schema_fields = json.loads(gcs_hook.download(BUCKET, CrossrefRelease.schema_gcs_object).decode("utf-8"))

    conn = bq_hook.get_conn()
    cursor = conn.cursor()

    # Pull messages
    ti: TaskInstance = kwargs['ti']
    msgs_in = ti.xcom_pull(key=TOPIC_NAME, task_ids=TASK_ID_LIST, include_prior_dates=False)
    for msg_in in msgs_in:
        crossref_release = CrossrefRelease(msg_in)
        cursor.run_load(
            destination_project_dataset_table=f"{DATASET_ID}.{crossref_release.table_name}",
            source_uris=f"gs://{BUCKET}/{crossref_release.decompressed_file_name}",
            schema_fields=schema_fields,
            source_format='NEWLINE_DELIMITED_JSON',
        )


def cleanup_releases(**kwargs):
    # Pull messages
    ti: TaskInstance = kwargs['ti']
    msgs_in = ti.xcom_pull(key=TOPIC_NAME, task_ids=TASK_ID_LIST, include_prior_dates=False)
    for msg_in in msgs_in:
        crossref_release = CrossrefRelease(msg_in)
        download_path_full = os.path.join(crossref_release.download_path, crossref_release.compressed_file_name)
        extracted_path_full = os.path.join(crossref_release.extracted_path, crossref_release.decompressed_file_name)
        transformed_path_full = os.path.join(crossref_release.transformed_path, crossref_release.decompressed_file_name)

        try:
            pathlib.Path(download_path_full).unlink()
        except FileNotFoundError as e:
            logging.warning(f"No such file or directory {download_path_full}: {e}")

        try:
            rmtree(extracted_path_full)
        except FileNotFoundError as e:
            logging.warning(f"No such file or directory {extracted_path_full}: {e}")

        try:
            pathlib.Path(transformed_path_full).unlink()
        except FileNotFoundError as e:
            logging.warning(f"No such file or directory {transformed_path_full}: {e}")


with DAG(dag_id="crossref", schedule_interval="@monthly", default_args=default_args) as dag:
    # List of all crossref releases for a given month
    list_releases = PythonOperator(
        task_id=TASK_ID_LIST,
        python_callable=list_releases_last_month,
        provide_context=True
    )

    # Downloads snapshot from url
    download_local = PythonOperator(
        task_id=TASK_ID_DOWNLOAD,
        python_callable=download_releases_local,
        provide_context=True
    )

    decompress = PythonOperator(
        task_id=TASK_ID_DECOMPRESS,
        python_callable=decompress_release,
        provide_context=True
    )

    transform = PythonOperator(
        task_id=TASK_ID_TRANSFORM,
        python_callable=transform_release,
        provide_context=True
    )

    upload_to_gcs = PythonOperator(
        task_id=TASK_ID_UPLOAD,
        python_callable=upload_release_to_gcs,
        provide_context=True

    )
    load_to_bq = PythonOperator(
        task_id=TASK_ID_BQ_LOAD,
        python_callable=load_release_to_bq,
        provide_context=True
    )

    cleanup_local = PythonOperator(
        task_id=TASK_ID_CLEANUP,
        python_callable=cleanup_releases,
        provide_context=True
    )

    list_releases >> download_local >> decompress >> transform >> upload_to_gcs >> load_to_bq >> cleanup_local