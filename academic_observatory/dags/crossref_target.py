"""
Parses response containing info on crossref releases.
Based on the returned info it checks whether there is an existing bigquery table with the same release.
If the table doesn't exist yet, it will trigger a separate dag 'crossref_target.py'.
"""
import os
import json
from datetime import datetime
from airflow import DAG
from airflow import settings
from airflow.models import Connection
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.file_to_gcs import FileToGoogleCloudStorageOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.gcs_download_operator import GoogleCloudStorageDownloadOperator

default_args = {
    "owner": "Airflow",
    "start_date": datetime(2020, 5, 1)
}

data_source='crossref'

#TODO get from environment, set e.g. to 'production' when using terraform.
testing_environment = 'prod'
#TODO don't hardcode these variables, not sure if they should be coming from Terraform or a separate external file
project_id = 'workflows-dev'
bucket = 'workflows-dev-910922156021-test'
local_data_dir = '/home/airflow/gcs/data'
# local_data_dir = '/usr/local/airflow/data'


def creating_bq_connection():
    # Adds a project id to the bigquery connection, prevents error 'ValueError: INTERNAL: No default project is
    # specified'
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


with DAG(dag_id="crossref_target", schedule_interval=None, default_args=default_args) as dag:
    if testing_environment == 'dev':
        # Downloads test file from gcs instead of from url
        download_snapshot_local = GoogleCloudStorageDownloadOperator(
            task_id='download_snapshot_local_debug',
            bucket="{{ dag_run.conf['bucket'] }}",
            object='crossref_debug.json.tar.gz',
            filename=os.path.join(local_data_dir, 'all.json.tar.gz'),
        )
        # Necessary to assign project id when running locally (instead of cloud composer)
        create_bq_connection = PythonOperator(
            task_id='create_bq_conn_debug',
            python_callable=creating_bq_connection
        )
        # Use custom conn ids with assigned project id
        load_to_bq = GoogleCloudStorageToBigQueryOperator(
            task_id='load_to_bq_debug',
            bucket="{{dag_run.conf['bucket']}}",
            source_objects=["{{dag_run.conf['snapshot_friendly_name_ext']}}"],
            destination_project_dataset_table="{{dag_run.conf['dataset']}}.{{dag_run.conf['snapshot_friendly_name']}}",
            schema_object="{{dag_run.conf['schema_gcs_object']}}",
            source_format='NEWLINE_DELIMITED_JSON',
            bigquery_conn_id='bigquery_custom',
            google_cloud_storage_conn_id='bigquery_custom'
        )

    else:
        # Download snapshot from url
        download_snapshot_local = BashOperator(
            task_id='download_snapshot_local',
            bash_command='url="{{dag_run.conf["snapshot_url"]}}"; api_token="{{ var.value.crossref_API_token }}"; wget '
                         f'-qc --header="$api_token" -P {local_data_dir} $url; '
        )
        # Load decompressed json of snapshot from gcs to bigquery
        load_to_bq = GoogleCloudStorageToBigQueryOperator(
            task_id='load_to_bq',
            bucket="{{dag_run.conf['bucket']}}",
            source_objects=["{{dag_run.conf['snapshot_friendly_name_ext']}}"],
            destination_project_dataset_table="{{dag_run.conf['dataset']}}.{{dag_run.conf['snapshot_friendly_name']}}",
            schema_object="{{dag_run.conf['schema_gcs_object']}}",
            source_format='NEWLINE_DELIMITED_JSON'
        )
    # Decompress downloaded snapshot and apply sed command.
    decompress_snapshot = BashOperator(
        task_id='decompress_snapshot',
        bash_command=f"tar -xOzf {os.path.join(local_data_dir, 'all.json.tar.gz')} | sed -E -e 's/\]\]/\]/g' -e 's/\[\[/\[/g' -e "
                     "'s/,[[:blank:]]*$//g'  -e 's/([[:alpha:]])-([[:alpha:]])/\\1_\\2/g' -e "
                     "'s/\"timestamp\":_/\"timestamp\":/g' -e 's/\"date_parts\":\[null\]/\"date_parts\":\[\]/g' -e "
                     "'s/^\{\"items\":\[//g' -e '/^\}$/d' -e '/^\]$/d' >> " + os.path.join(local_data_dir, "{{dag_run.conf['snapshot_friendly_name_ext']}}")
    )
    # Check if decompress command was succesful by checking that decompressed file is not empty,
    # delete compressed file locally if command was succesful.
    delete_compressed_local = BashOperator(
        task_id='delete_compressed_local',
        bash_command=f"if ! [ -s %s ]; then rm %s ; fi;" % (os.path.join(local_data_dir, "{{dag_run.conf['snapshot_friendly_name_ext']}}"),
                                                            os.path.join(local_data_dir, 'all.json.tar.gz'))
    )
    # Upload decompressed file to gcs for long-term storage
    #TODO fix openssl error with this operator. Now using bashoperator instead.
    # upload_to_gcs = FileToGoogleCloudStorageOperator(
    #     task_id='upload_to_gcs',
    #     src=os.path.join(local_data_dir, "{{dag_run.conf['snapshot_friendly_name_ext']}}"),
    #     dst="{{dag_run.conf['snapshot_friendly_name_ext']}}",
    #     bucket="{{dag_run.conf['bucket']}}"
    # )

    upload_to_gcs = BashOperator(
        task_id='upload_to_gcs',
        bash_command='gsutil cp %s gs://%s' % (os.path.join(local_data_dir, "{{dag_run.conf['snapshot_friendly_name_ext']}}"), "{{dag_run.conf['bucket']}}"),
    )

    # Delete decompressed file locally.
    delete_decompressed_local = BashOperator(
        task_id='delete_decompressed_local',
        bash_command="rm " + os.path.join(local_data_dir, "{{dag_run.conf['snapshot_friendly_name_ext']}}")
    )

    download_snapshot_local >> decompress_snapshot >> [delete_compressed_local, upload_to_gcs]
    if testing_environment == 'dev':
        upload_to_gcs >> [delete_decompressed_local, create_bq_connection]
        create_bq_connection >> load_to_bq
    else:
        upload_to_gcs >> [delete_decompressed_local, load_to_bq]
