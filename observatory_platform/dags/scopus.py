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

from observatory_platform.telescopes.scopus import ScopusTelescope
# from observatory_platform.utils.config_utils import list_connections

# Remove later when WoS branch merges.
from airflow.utils.db import create_session
from airflow.models import Connection


def list_connections(source):
    """Get a list of data source connections with name starting with <source>_, e.g., scopus_curtin.

    :param source: Data source (conforming to name convention) as a string, e.g., 'scopus'.
    :return: A list of connection id strings with the prefix <source>_, e.g., ['scopus_curtin', 'scopus_auckland'].
    """
    with create_session() as session:
        query = session.query(Connection)
        query = query.filter(Connection.conn_id.like(f'{source}_%'))
        return query.all()


default_args = {'owner': 'airflow',
                'start_date': datetime(2018, 1, 1),
                }


def subdag_factory(parent_dag_id, connection, args):
    """ Factory for making the ETL subdags.

    :param parent_dag_id: parent dag's id.
    :param connection: Airflow Connection object.
    :param args: default arguments to use for this subdag.
    :return: DAG object.
    """

    institution = str(connection)[4:]
    logging.info(f'Spawning ETL subdag for: {institution}')

    subdag = DAG(
        dag_id=f'{parent_dag_id}.{institution}',
        default_args=args,
        catchup=False,
        schedule_interval=ScopusTelescope.SCHEDULE_INTERVAL
    )

    with subdag:
        # Check that dependencies exist before starting
        check_dependencies = PythonOperator(
            task_id=ScopusTelescope.TASK_ID_CHECK_DEPENDENCIES,
            python_callable=ScopusTelescope.check_dependencies,
            op_kwargs={'dag_start': '{{dag_run.start_date}}', 'conn': connection, 'institution': institution},
            provide_context=True,
            queue=ScopusTelescope.QUEUE
        )

        download = PythonOperator(
            task_id=ScopusTelescope.TASK_ID_DOWNLOAD,
            python_callable=ScopusTelescope.download,
            op_kwargs={'conn': connection},
            provide_context=True,
            queue=ScopusTelescope.QUEUE,
            retries=ScopusTelescope.RETRIES
        )

        # Upload gzipped response (json)
        upload_downloaded = PythonOperator(
            task_id=ScopusTelescope.TASK_ID_UPLOAD_DOWNLOADED,
            python_callable=ScopusTelescope.upload_downloaded,
            provide_context=True,
            queue=ScopusTelescope.QUEUE,
            retries=ScopusTelescope.RETRIES
        )

        # Transform into database schema format (jsonline output)
        transform_db_format = PythonOperator(
            task_id=ScopusTelescope.TASK_ID_TRANSFORM_DB_FORMAT,
            python_callable=ScopusTelescope.transform_db_format,
            provide_context=True,
            queue=ScopusTelescope.QUEUE
        )

        # # Upload the gzipped transformed jsonline entries to Google Cloud Storage
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

        check_dependencies >> download >> upload_downloaded >> transform_db_format >> upload_transformed \
        >> bq_load >> cleanup

    return subdag


with DAG(dag_id=ScopusTelescope.DAG_ID, schedule_interval=ScopusTelescope.SCHEDULE_INTERVAL, catchup=False,
         default_args=default_args) as dag:
    # Only process if the Web of Science API server is up.
    check_api_server = PythonOperator(
        task_id=ScopusTelescope.TASK_CHECK_API_SERVER,
        python_callable=ScopusTelescope.check_api_server,
        provide_context=False,
        queue=ScopusTelescope.QUEUE,
        retries=ScopusTelescope.RETRIES,
    )

    # Spawn a subdag to handle ETL for each institution
    subdags = list()
    conns = list_connections('wos')
    for conn in conns:
        subdag = SubDagOperator(
            task_id=str(conn)[4:],
            subdag=subdag_factory(ScopusTelescope.DAG_ID, conn, default_args),
            default_args=default_args,
            dag=dag
        )

        # Task dependencies
        check_api_server >> subdag
        subdags.append(subdag)
