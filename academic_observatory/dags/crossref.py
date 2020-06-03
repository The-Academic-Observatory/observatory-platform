from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

from academic_observatory.telescopes.crossref import CrossrefTelescope

default_args = {
    "owner": "Airflow",
    "start_date": datetime(2020, 3, 1)
}


with DAG(dag_id=CrossrefTelescope.DAG_ID, schedule_interval="@monthly", default_args=default_args) as dag:
    # Get config variables
    get_config = PythonOperator(
        task_id=CrossrefTelescope.TASK_ID_CONFIG,
        python_callable=CrossrefTelescope.get_config_variables,
        provide_context=True
    )

    # List of all unpaywall releases for a given month
    list_releases = BranchPythonOperator(
        task_id=CrossrefTelescope.TASK_ID_LIST,
        python_callable=CrossrefTelescope.list_releases_last_month,
        provide_context=True
    )

    stop_workflow = DummyOperator(task_id=CrossrefTelescope.TASK_ID_STOP)

    # Downloads snapshot from url
    download_local = PythonOperator(
        task_id=CrossrefTelescope.TASK_ID_DOWNLOAD,
        python_callable=CrossrefTelescope.download_releases_local,
        provide_context=True
    )

    decompress = PythonOperator(
        task_id=CrossrefTelescope.TASK_ID_DECOMPRESS,
        python_callable=CrossrefTelescope.decompress_release,
        provide_context=True
    )

    transform = PythonOperator(
        task_id=CrossrefTelescope.TASK_ID_TRANSFORM,
        python_callable=CrossrefTelescope.transform_release,
        provide_context=True
    )

    upload_to_gcs = PythonOperator(
        task_id=CrossrefTelescope.TASK_ID_UPLOAD,
        python_callable=CrossrefTelescope.upload_release_to_gcs,
        provide_context=True

    )
    load_to_bq = PythonOperator(
        task_id=CrossrefTelescope.TASK_ID_BQ_LOAD,
        python_callable=CrossrefTelescope.load_release_to_bq,
        provide_context=True
    )

    cleanup_local = PythonOperator(
        task_id=CrossrefTelescope.TASK_ID_CLEANUP,
        python_callable=CrossrefTelescope.cleanup_releases,
        provide_context=True
    )

    get_config >> list_releases >> [download_local, stop_workflow]
    download_local >> decompress >> transform >> upload_to_gcs >> load_to_bq >> cleanup_local