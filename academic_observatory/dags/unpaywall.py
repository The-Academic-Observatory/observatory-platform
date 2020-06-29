from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.python_operator import PythonOperator

from academic_observatory.telescopes.unpaywall import UnpaywallTelescope

default_args = {
    "owner": "Airflow",
    "start_date": datetime(2020, 3, 1)
}

with DAG(dag_id="unpaywall", schedule_interval="@monthly", default_args=default_args) as dag:
    # Get config variables
    check_setup = PythonOperator(
        task_id=UnpaywallTelescope.TASK_ID_SETUP,
        python_callable=UnpaywallTelescope.check_setup_requirements,
        provide_context=True
    )

    # List of all releases for last month
    list_releases = BranchPythonOperator(
        task_id=UnpaywallTelescope.TASK_ID_LIST,
        python_callable=UnpaywallTelescope.list_releases_last_month,
        provide_context=True
    )

    stop_workflow = DummyOperator(task_id=UnpaywallTelescope.TASK_ID_STOP)

    # Downloads snapshot from url
    download_local = PythonOperator(
        task_id=UnpaywallTelescope.TASK_ID_DOWNLOAD,
        python_callable=UnpaywallTelescope.download,
        provide_context=True
    )

    # Decompresses download
    decompress = PythonOperator(
        task_id=UnpaywallTelescope.TASK_ID_DECOMPRESS,
        python_callable=UnpaywallTelescope.decompress,
        provide_context=True
    )

    # Transforms download
    transform = PythonOperator(
        task_id=UnpaywallTelescope.TASK_ID_TRANSFORM,
        python_callable=UnpaywallTelescope.transform,
        provide_context=True
    )

    # Upload download to gcs bucket
    upload_to_gcs = PythonOperator(
        task_id=UnpaywallTelescope.TASK_ID_UPLOAD,
        python_callable=UnpaywallTelescope.upload_to_gcs,
        provide_context=True

    )

    # Upload download to bigquery table
    load_to_bq = PythonOperator(
        task_id=UnpaywallTelescope.TASK_ID_BQ_LOAD,
        python_callable=UnpaywallTelescope.load_to_bq,
        provide_context=True
    )

    # Delete locally stored files
    cleanup_local = PythonOperator(
        task_id=UnpaywallTelescope.TASK_ID_CLEANUP,
        python_callable=UnpaywallTelescope.cleanup_releases,
        provide_context=True
    )

    check_setup >> [list_releases, stop_workflow]
    list_releases >> [download_local, stop_workflow]
    download_local >> decompress >> transform >> upload_to_gcs >> load_to_bq >> cleanup_local
