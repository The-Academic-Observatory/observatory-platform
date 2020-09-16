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


from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.subdag_operator import SubDagOperator

from observatory_platform.telescopes.scopus import ScopusTelescope
# from observatory_platform.utils.config_utils import list_connections


default_args = {'owner': 'airflow',
                'start_date': datetime(2018, 1, 1),
                }

def download_subdag_factory(parent_dag_id, subdag_id, args):
    """ Factory for making the download subdag.

    :param parent_dag_id: parent dag's id.
    :param subdag_id: id of this subdag.
    :param args: default arguments to use for this subdag.
    :return: DAG object.
    """

    subdag = DAG(
        dag_id=f'{parent_dag_id}.{subdag_id}',
        default_args=args,
        catchup=False,
        schedule_interval=ScopusTelescope.SCHEDULE_INTERVAL
    )

    # conns = list_connections('wos')

    # with subdag:
    #     for conn in conns:
    #         institution = str(conn)[4:]
    #         PythonOperator(
    #             task_id=institution,
    #             python_callable=ScopusTelescope.download,
    #             op_kwargs={'conn' : conn, 'dag_start': '{{dag_run.start_date}}'},
    #             provide_context=True,
    #             queue=ScopusTelescope.QUEUE,
    #             retries=ScopusTelescope.RETRIES,
    #         )

        # Load tasks

    return subdag



with DAG(dag_id=ScopusTelescope.DAG_ID, schedule_interval=ScopusTelescope.SCHEDULE_INTERVAL, catchup=False, default_args=default_args) as dag:
    # Check that dependencies exist before starting

    check_dependencies = PythonOperator(
        task_id=ScopusTelescope.TASK_ID_CHECK_DEPENDENCIES,
        python_callable=ScopusTelescope.check_dependencies,
        provide_context=True,
        queue=ScopusTelescope.QUEUE
    )

    # Only process if the Web of Science API server is up.
    check_api_server = PythonOperator(
        task_id=ScopusTelescope.TASK_CHECK_API_SERVER,
        python_callable=ScopusTelescope.check_api_server,
        provide_context=True,
        queue=ScopusTelescope.QUEUE,
        retries=ScopusTelescope.RETRIES,
    )

    # Download the WoS snapshot for all configured institutions
    download = SubDagOperator(
        task_id=ScopusTelescope.TASK_ID_DOWNLOAD,
        subdag=download_subdag_factory(ScopusTelescope.DAG_ID, ScopusTelescope.SUBDAG_ID_DOWNLOAD, default_args),
        default_args=default_args,
        dag=dag
    )

    # Upload gzipped response (XML)
    upload_downloaded = PythonOperator(
        task_id=ScopusTelescope.TASK_ID_UPLOAD_DOWNLOADED,
        python_callable=ScopusTelescope.upload_downloaded,
        provide_context=True,
        queue=ScopusTelescope.QUEUE,
        retries=ScopusTelescope.RETRIES
    )

    # Transform XML data to json
    transform_xml_to_json = PythonOperator(
        task_id=ScopusTelescope.TASK_ID_TRANSFORM_XML,
        python_callable=ScopusTelescope.transform_xml,
        provide_context=True,
        queue=ScopusTelescope.QUEUE,
        retries=ScopusTelescope.RETRIES
    )

    # Upload gzipped JSON converted (from XML) responses.
    upload_json = PythonOperator(
        task_id=ScopusTelescope.TASK_ID_UPLOAD_JSON,
        python_callable=ScopusTelescope.upload_json,
        provide_context=True,
        queue=ScopusTelescope.QUEUE,
        retries=ScopusTelescope.RETRIES
    )

    # Transform into database schema format
    transform_db_format = PythonOperator(
        task_id=ScopusTelescope.TASK_ID_TRANSFORM_DB_FORMAT,
        python_callable=ScopusTelescope.transform_db_format,
        provide_context=True,
        queue=ScopusTelescope.QUEUE
    )

    # # Upload the transformed jsonline entries to Google Cloud Storage
    upload_transformed = PythonOperator(
        task_id=ScopusTelescope.TASK_ID_UPLOAD_TRANSFORMED,
        python_callable=ScopusTelescope.upload_transformed,
        provide_context=True,
        queue=ScopusTelescope.QUEUE,
        retries=ScopusTelescope.RETRIES
    )

    # Load the transformed WoS snapshot to BigQuery
    # Depends on past so that BigQuery load jobs are not all created at once
    bq_load = PythonOperator(
        task_id=ScopusTelescope.TASK_ID_BQ_LOAD,
        python_callable=ScopusTelescope.bq_load,
        provide_context=True,
        queue=ScopusTelescope.QUEUE
    )

    cleanup = PythonOperator(
        task_id=ScopusTelescope.TASK_ID_CLEANUP,
        python_callable=ScopusTelescope.cleanup,
        provide_context=True,
        queue=ScopusTelescope.QUEUE
    )

    # Task dependencies
    check_dependencies >> check_api_server >> download >> upload_downloaded >> transform_xml_to_json \
     >> upload_json >> transform_db_format >> upload_transformed >> bq_load >> cleanup
