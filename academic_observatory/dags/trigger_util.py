import logging
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.python_operator import PythonOperator


def conditional_trigger_dag(context, dag_run_obj):
    # used to pass through the params to triggered dag
    dag_run_obj.payload = context['params']
    print(dag_run_obj.payload)
    return dag_run_obj


def resetting_airflow_variables(data_source):
    # reset airflow variables, especially useful for max_no_snapshots,
    # so later runs are not influenced by this.
    variable_no_snapshots = f"{data_source}_no_snapshots"
    variable_max_no_snapshots = f"{data_source}_max_no_snapshots"
    variable_snapshot_url_list = f"{data_source}_snapshot_url_list"

    Variable.set(variable_snapshot_url_list, '')
    Variable.set(variable_no_snapshots, 0)
    Variable.set(variable_max_no_snapshots, 0)


def create_trigger_dag(bucket, data_source, default_args, create_url_debug, create_url_list, ReleaseUtil, testing_environment):
    """
    Returns a dag that triggers external dags, one per snapshot.
    :param bucket: Bucket to store/retrieve data
    :param data_source: Which data_source is used, e.g. unpaywall/crossref
    :param default_args: The default_args of the dag
    :param create_url_debug: Method to create an url list of available snapshots when debugging
    :param create_url_list: Method to create an url list of available snapshots
    :param ReleaseUtil: Class used to retrieve info on snapshot per data source
    :param testing_environment: Testing or in production
    :return: dag object
    """
    variable_no_snapshots = f"{data_source}_no_snapshots"
    variable_max_no_snapshots = f"{data_source}_max_no_snapshots"
    variable_snapshot_url_list = f"{data_source}_snapshot_url_list"
    dag_id = f"{data_source}_trigger"
    target_dag_id = f"{data_source}_target"

    with DAG(dag_id=dag_id, schedule_interval="@monthly", default_args=default_args, catchup=True) as dag:
        if testing_environment == 'dev':
            get_snapshot_urls = PythonOperator(
                task_id='get_snapshot_url_list_debug',
                dag=dag,
                python_callable=create_url_debug
            )
        else:
            get_snapshot_urls = PythonOperator(
                task_id='get_snapshot_url_list',
                dag=dag,
                python_callable=create_url_list,
            )

        no_snapshots = int(Variable.get(variable_no_snapshots, default_var=0))

        if no_snapshots > 0:
            # convert airflow variable which is a string back to list
            snapshot_url_list = Variable.get(variable_snapshot_url_list).strip(', ').split(', ')
            # possibly limit number of snapshots that is downloaded
            max_no_snapshots = int(Variable.get(variable_max_no_snapshots, default_var=0))
            if max_no_snapshots > 0:
                no_snapshots = min(no_snapshots, max_no_snapshots)

        task_list = []
        for index in range(no_snapshots):
            snapshot_url = snapshot_url_list[index]
            snapshot_release = ReleaseUtil(snapshot_url)

            logging.info("Idx and url is: " + str(index) + "- " + snapshot_url)

            # add task to list, params are necessary for target dag
            task_list.append(TriggerDagRunOperator(
                task_id="trigger_dag_" + snapshot_release.friendly_name,
                trigger_dag_id=target_dag_id,
                provide_context=True,
                python_callable=conditional_trigger_dag,
                params={
                    'snapshot_url': snapshot_url,
                    'snapshot_friendly_name_ext': snapshot_release.friendly_name_ext,
                    'snapshot_friendly_name': snapshot_release.friendly_name,
                    'bucket': bucket,
                    'dataset': data_source,
                    'schema_gcs_object': ReleaseUtil.schema_gcs_object,
                },
            ))
            # set new tasks downstream of previous task
            if index == 0:
                get_snapshot_urls >> task_list[0]
            else:
                task_list[index-1] >> task_list[index]
            # reset airflow variables as a last task
            if index == (no_snapshots-1):
                task_list.append(PythonOperator(
                    task_id='reset_airflow_variables',
                    dag=dag,
                    python_callable=resetting_airflow_variables,
                    op_args=[data_source]
                ))
                task_list[index] >> task_list[index+1]

    return dag