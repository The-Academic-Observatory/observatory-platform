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


import logging

from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.subdag_operator import SubDagOperator

from observatory_platform.telescopes.wos import WosTelescope
from observatory_platform.utils.config_utils import list_connections

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
        schedule_interval=WosTelescope.SCHEDULE_INTERVAL
    )

    conns = list_connections('wos')

    with subdag:
        for conn in conns:
            institution = str(conn)[4:]

            logging.info(f'Subdag spawning download task for: {institution}')
            PythonOperator(
                task_id=institution,
                python_callable=WosTelescope.download,
                op_kwargs={'conn': conn},
                provide_context=True,
                queue=WosTelescope.QUEUE,
                retries=WosTelescope.RETRIES,
            )

        # Load tasks

    return subdag


with DAG(dag_id=WosTelescope.DAG_ID, schedule_interval=WosTelescope.SCHEDULE_INTERVAL, catchup=False,
         default_args=default_args) as dag:
    # Check that dependencies exist before starting

    check_dependencies = PythonOperator(
        task_id=WosTelescope.TASK_ID_CHECK_DEPENDENCIES,
        python_callable=WosTelescope.check_dependencies,
        op_kwargs={'dag_start': '{{dag_run.start_date}}'},
        provide_context=True,
        queue=WosTelescope.QUEUE
    )

    # Only process if the Web of Science API server is up.
    check_api_server = PythonOperator(
        task_id=WosTelescope.TASK_CHECK_API_SERVER,
        python_callable=WosTelescope.check_api_server,
        provide_context=False,
        queue=WosTelescope.QUEUE,
        retries=WosTelescope.RETRIES,
    )

    # Download the WoS snapshot for all configured institutions
    download = SubDagOperator(
        task_id=WosTelescope.TASK_ID_DOWNLOAD,
        subdag=download_subdag_factory(WosTelescope.DAG_ID, WosTelescope.SUBDAG_ID_DOWNLOAD, default_args),
        default_args=default_args,
        dag=dag
    )

    # Upload gzipped response (XML)
    upload_downloaded = PythonOperator(
        task_id=WosTelescope.TASK_ID_UPLOAD_DOWNLOADED,
        python_callable=WosTelescope.upload_downloaded,
        provide_context=True,
        queue=WosTelescope.QUEUE,
        retries=WosTelescope.RETRIES
    )

    # Transform XML data to json
    transform_xml_to_json = PythonOperator(
        task_id=WosTelescope.TASK_ID_TRANSFORM_XML,
        python_callable=WosTelescope.transform_xml,
        provide_context=True,
        queue=WosTelescope.QUEUE,
        retries=WosTelescope.RETRIES
    )

    # Transform into database schema format
    transform_db_format = PythonOperator(
        task_id=WosTelescope.TASK_ID_TRANSFORM_DB_FORMAT,
        python_callable=WosTelescope.transform_db_format,
        provide_context=True,
        queue=WosTelescope.QUEUE
    )

    # # Upload the transformed jsonline entries to Google Cloud Storage
    upload_transformed = PythonOperator(
        task_id=WosTelescope.TASK_ID_UPLOAD_TRANSFORMED,
        python_callable=WosTelescope.upload_transformed,
        provide_context=True,
        queue=WosTelescope.QUEUE,
        retries=WosTelescope.RETRIES
    )

    # Load the transformed WoS snapshot to BigQuery
    # Depends on past so that BigQuery load jobs are not all created at once
    bq_load = PythonOperator(
        task_id=WosTelescope.TASK_ID_BQ_LOAD,
        python_callable=WosTelescope.bq_load,
        provide_context=True,
        queue=WosTelescope.QUEUE
    )

    cleanup = PythonOperator(
        task_id=WosTelescope.TASK_ID_CLEANUP,
        python_callable=WosTelescope.cleanup,
        provide_context=True,
        queue=WosTelescope.QUEUE
    )

    # Task dependencies
    check_dependencies >> check_api_server >> download >> upload_downloaded >> transform_xml_to_json \
    >> transform_db_format >> upload_transformed >> bq_load >> cleanup
