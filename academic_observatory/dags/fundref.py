from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

from academic_observatory.telescopes.fundref import FundrefTelescope

default_args = {
    "owner": "Airflow",
    "start_date": datetime(2020, 1, 1)
}


with DAG(dag_id="fundref", schedule_interval="@monthly", default_args=default_args) as dag:
    # Get config variables
    check_setup = PythonOperator(
        task_id=FundrefTelescope.TASK_ID_SETUP,
        python_callable=FundrefTelescope.setup_requirements,
        provide_context=True
    )

    # List of all unpaywall releases for a given month
    list_releases = BranchPythonOperator(
        task_id=FundrefTelescope.TASK_ID_LIST,
        python_callable=FundrefTelescope.list_releases_last_month,
        provide_context=True
    )

    stop_workflow = DummyOperator(task_id=FundrefTelescope.TASK_ID_STOP)

    # Downloads snapshot from url
    download_local = PythonOperator(
        task_id=FundrefTelescope.TASK_ID_DOWNLOAD,
        python_callable=FundrefTelescope.download_releases_local,
        provide_context=True
    )

    decompress = PythonOperator(
        task_id=FundrefTelescope.TASK_ID_DECOMPRESS,
        python_callable=FundrefTelescope.decompress_release,
        provide_context=True
    )

    geonames = PythonOperator(
        task_id=FundrefTelescope.TASK_ID_GEONAMES,
        python_callable=FundrefTelescope.geonames_dump,
        provide_context=True
    )

    transform = PythonOperator(
        task_id=FundrefTelescope.TASK_ID_TRANSFORM,
        python_callable=FundrefTelescope.transform_release,
        provide_context=True
    )

    upload_to_gcs = PythonOperator(
        task_id=FundrefTelescope.TASK_ID_UPLOAD,
        python_callable=FundrefTelescope.upload_release_to_gcs,
        provide_context=True

    )
    load_to_bq = PythonOperator(
        task_id=FundrefTelescope.TASK_ID_BQ_LOAD,
        python_callable=FundrefTelescope.load_release_to_bq,
        provide_context=True
    )

    cleanup_local = PythonOperator(
        task_id=FundrefTelescope.TASK_ID_CLEANUP,
        python_callable=FundrefTelescope.cleanup_releases,
        provide_context=True
    )

    check_setup >> list_releases >> [download_local, stop_workflow]
    download_local >> decompress >> geonames >> transform >> upload_to_gcs >> load_to_bq >> cleanup_local