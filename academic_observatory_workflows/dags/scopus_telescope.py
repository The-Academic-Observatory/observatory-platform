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

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.subdag import SubDagOperator
from academic_observatory_workflows.workflows.scopus_telescope import ScopusTelescope
from observatory.platform.utils.airflow_utils import list_connections

default_args = {
    "owner": "airflow",
    "start_date": pendulum.datetime(2018, 1, 1),
}


def subdag_factory(parent_dag_id, connection, args):
    """Factory for making the ETL subdags.

    :param parent_dag_id: parent dag's id.
    :param connection: Airflow Connection object.
    :param args: default arguments to use for this subdag.
    :return: DAG object.
    """

    institution = str(connection)[ScopusTelescope.ID_STRING_OFFSET :]
    logging.info(f"Spawning ETL subdag for: {institution}")

    subdag = DAG(
        dag_id=f"{parent_dag_id}.{institution}",
        default_args=args,
        catchup=False,
        schedule_interval=ScopusTelescope.SCHEDULE_INTERVAL,
    )

    with subdag:
        # Check that dependencies exist before starting
        check_dependencies = PythonOperator(
            task_id=ScopusTelescope.TASK_ID_CHECK_DEPENDENCIES,
            python_callable=ScopusTelescope.check_dependencies,
            op_kwargs={"dag_start": "{{dag_run.start_date}}", "conn": connection, "institution": institution},
            queue=ScopusTelescope.QUEUE,
        )

        download = PythonOperator(
            task_id=ScopusTelescope.TASK_ID_DOWNLOAD,
            python_callable=ScopusTelescope.download,
            op_kwargs={"conn": connection},
            queue=ScopusTelescope.QUEUE,
            retries=ScopusTelescope.RETRIES,
        )

        # Upload gzipped response (json)
        upload_downloaded = PythonOperator(
            task_id=ScopusTelescope.TASK_ID_UPLOAD_DOWNLOADED,
            python_callable=ScopusTelescope.upload_downloaded,
            queue=ScopusTelescope.QUEUE,
            retries=ScopusTelescope.RETRIES,
        )

        # Transform into database schema format (jsonline output)
        transform_db_format = PythonOperator(
            task_id=ScopusTelescope.TASK_ID_TRANSFORM_DB_FORMAT,
            python_callable=ScopusTelescope.transform_db_format,
            queue=ScopusTelescope.QUEUE,
        )

        # # Upload the gzipped transformed jsonline entries to Google Cloud Storage
        upload_transformed = PythonOperator(
            task_id=ScopusTelescope.TASK_ID_UPLOAD_TRANSFORMED,
            python_callable=ScopusTelescope.upload_transformed,
            queue=ScopusTelescope.QUEUE,
            retries=ScopusTelescope.RETRIES,
        )

        # Load the transformed SCOPUS snapshot to BigQuery
        # Depends on past so that BigQuery load jobs are not all created at once
        bq_load = PythonOperator(
            task_id=ScopusTelescope.TASK_ID_BQ_LOAD,
            python_callable=ScopusTelescope.bq_load,
            queue=ScopusTelescope.QUEUE,
        )

        cleanup = PythonOperator(
            task_id=ScopusTelescope.TASK_ID_CLEANUP,
            python_callable=ScopusTelescope.cleanup,
            queue=ScopusTelescope.QUEUE,
        )

        (
            check_dependencies
            >> download
            >> upload_downloaded
            >> transform_db_format
            >> upload_transformed
            >> bq_load
            >> cleanup
        )

    return subdag


with DAG(
    dag_id=ScopusTelescope.DAG_ID,
    schedule_interval=ScopusTelescope.SCHEDULE_INTERVAL,
    catchup=False,
    default_args=default_args,
) as dag:
    # Only process if the SCOPUS API server is up.
    check_api_server = PythonOperator(
        task_id=ScopusTelescope.TASK_CHECK_API_SERVER,
        python_callable=ScopusTelescope.check_api_server,
        queue=ScopusTelescope.QUEUE,
        retries=ScopusTelescope.RETRIES,
    )

    # Spawn a subdag to handle ETL for each institution
    subdags = list()
    conns = list_connections(ScopusTelescope.DAG_ID)
    for conn in conns:
        subdag = SubDagOperator(
            task_id=str(conn)[ScopusTelescope.ID_STRING_OFFSET :],
            subdag=subdag_factory(ScopusTelescope.DAG_ID, conn, default_args),
            default_args=default_args,
            dag=dag,
        )

        # Task dependencies
        check_api_server >> subdag
        subdags.append(subdag)
