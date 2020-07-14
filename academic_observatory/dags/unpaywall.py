from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import ShortCircuitOperator

from academic_observatory.telescopes.unpaywall import UnpaywallTelescope

default_args = {
    "owner": "Airflow",
    "start_date": datetime(2020, 4, 1)
}

with DAG(dag_id="unpaywall", schedule_interval="@weekly", default_args=default_args) as dag:
    # Check that variables exist
    check = PythonOperator(
        task_id=UnpaywallTelescope.TASK_ID_CHECK_DEPENDENCIES,
        python_callable=UnpaywallTelescope.check_dependencies,
        provide_context=True,
        queue=UnpaywallTelescope.QUEUE
    )

    # List releases and skip all subsequent tasks if there is no release to process
    list_releases = ShortCircuitOperator(
        task_id=UnpaywallTelescope.TASK_ID_LIST,
        python_callable=UnpaywallTelescope.list_releases,
        provide_context=True,
        queue=UnpaywallTelescope.QUEUE
    )

    # Downloads snapshot from url
    download = PythonOperator(
        task_id=UnpaywallTelescope.TASK_ID_DOWNLOAD,
        python_callable=UnpaywallTelescope.download,
        provide_context=True,
        queue=UnpaywallTelescope.QUEUE
    )

    # Upload downloaded files for safekeeping
    upload_downloaded = PythonOperator(
        task_id=UnpaywallTelescope.TASK_ID_UPLOAD_DOWNLOADED,
        python_callable=UnpaywallTelescope.upload_downloaded,
        provide_context=True,
        queue=UnpaywallTelescope.QUEUE
    )

    # Decompresses download
    extract = PythonOperator(
        task_id=UnpaywallTelescope.TASK_ID_EXTRACT,
        python_callable=UnpaywallTelescope.extract,
        provide_context=True,
        queue=UnpaywallTelescope.QUEUE
    )

    # Transforms download
    transform = PythonOperator(
        task_id=UnpaywallTelescope.TASK_ID_TRANSFORM,
        python_callable=UnpaywallTelescope.transform,
        provide_context=True,
        queue=UnpaywallTelescope.QUEUE
    )

    # Upload download to gcs bucket
    upload_transformed = PythonOperator(
        task_id=UnpaywallTelescope.TASK_ID_UPLOAD_TRANSFORMED,
        python_callable=UnpaywallTelescope.upload_transformed,
        provide_context=True,
        queue=UnpaywallTelescope.QUEUE
    )

    # Upload download to bigquery table
    bq_load = PythonOperator(
        task_id=UnpaywallTelescope.TASK_ID_BQ_LOAD,
        python_callable=UnpaywallTelescope.load_to_bq,
        provide_context=True,
        queue=UnpaywallTelescope.QUEUE
    )

    # Delete locally stored files
    cleanup = PythonOperator(
        task_id=UnpaywallTelescope.TASK_ID_CLEANUP,
        python_callable=UnpaywallTelescope.cleanup,
        provide_context=True,
        queue=UnpaywallTelescope.QUEUE
    )

    check >> list_releases >> download >> upload_downloaded >> extract >> transform >> upload_transformed >> bq_load >> cleanup
