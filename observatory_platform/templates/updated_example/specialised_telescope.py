from abc import ABC, abstractmethod
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, ShortCircuitOperator
from airflow.models.taskinstance import TaskInstance
import pendulum
from observatory_platform.templates.updated_example.telescope import TelescopeRelease, Telescope
import logging
from datetime import timedelta


class SpecialisedRelease(TelescopeRelease):
    """ Used to store info on a given release """
    def __init__(self, dag_id: str, start_date: pendulum.Pendulum, end_date: pendulum.Pendulum,
                 extensions: dict, first_release: bool = False):
        super().__init__(dag_id, start_date, end_date, extensions, first_release)


def pull_release(ti: TaskInstance) -> SpecialisedRelease:
    """ Pulls xcom with release.

    :param ti: Task instance
    :return: One release
    """
    return ti.xcom_pull(key=SpecialisedTelescope.RELEASES_XCOM, task_ids=SpecialisedTelescope.TASK_ID_CREATE_RELEASE,
                        include_prior_dates=False)


def pull_last_start_date(ti: TaskInstance) -> pendulum.Pendulum:
    """ Pulls xcom with last start date of this run.

    :param ti: Task instance
    :return: Last start date
    """
    return ti.xcom_pull(key=SpecialisedTelescope.LAST_START_XCOM, task_ids=SpecialisedTelescope.TASK_ID_CREATE_RELEASE,
                        include_prior_dates=True)


class SpecialisedTelescope(Telescope, ABC):
    RELEASES_XCOM = "release"
    LAST_START_XCOM = "last_start"

    TASK_ID_CREATE_RELEASE = "create_release"
    TASK_ID_DOWNLOAD = "download"
    TASK_ID_BQ_LOAD_PARTITION = "bq_load_partition"

    def __init__(self, dag_id, start_date, schedule_interval, extensions, partition_table_id):
        super().__init__(dag_id, start_date, schedule_interval, extensions)
        self.partition_table_id = partition_table_id

    def create_airflow_DAG(self) -> DAG:
        default_args = {
            "owner": "airflow",
            "start_date": self.start_date,
            # 'on_failure_callback': on_failure_callback
        }

        with DAG(dag_id=self.dag_id, schedule_interval=self.schedule_interval,
                 default_args=default_args, catchup=False, max_active_runs=1, doc_md=self.__doc__) as dag:
            task1 = PythonOperator(task_id=self.TASK_ID_CREATE_RELEASE, provide_context=True,
                                   python_callable=self.create_release_task)
            task2 = ShortCircuitOperator(task_id=self.TASK_ID_DOWNLOAD, provide_context=True,
                                         python_callable=self.download_task)
            task3 = PythonOperator(task_id=self.TASK_ID_BQ_LOAD_PARTITION, provide_context=True,
                                   python_callable=self.bq_load_partition_task)

            task1 >> task2 >> task3
        return dag

    # not an abstractmethod, but can optionally be overwritten
    def create_release(self, start_date: pendulum.Pendulum, end_date: pendulum.Pendulum, first_release: bool):
        release = SpecialisedRelease(self.dag_id, start_date, end_date, self.extensions, first_release)
        return release

    def create_release_task(self, **kwargs):
        """ Create a release instance and update the xcom value with the last start date.

        :param kwargs: The context passed from the PythonOperator.
        :return: None.
        """
        ti: TaskInstance = kwargs['ti']

        first_release = False
        start_date = pull_last_start_date(ti)
        if not start_date:
            first_release = True
            start_date = pendulum.instance(kwargs['dag'].default_args['start_date']).start_of('day')
        start_date = start_date - timedelta(days=1)
        end_date = pendulum.utcnow()
        logging.info(f'Start date: {start_date}, end date: {end_date}, first release: {first_release}')

        release = self.create_release(start_date, end_date, first_release)

        ti.xcom_push(self.RELEASES_XCOM, release)
        ti.xcom_push(self.LAST_START_XCOM, end_date)

    @abstractmethod
    def download(self, release):
        """
        Download code.
        :param release:
        """
        raise NotImplementedError

    # method that calls an abstractmethod that needs to be customised by the user
    def download_task(self, **kwargs) -> bool:
        """ Download release data to disk.

        :param kwargs: The context passed from the PythonOperator.
        :return: None.
        """
        ti: TaskInstance = kwargs['ti']
        release = pull_release(ti)

        success = self.download(release)
        return True if success else False

    # a method that is specific to the specialised telescope, should not be overwritten in principle
    def bq_load_partition_task(self, **kwargs):
        """ Load the data from the transformed release to a BigQuery partition (partitioned by ingestion date).
        This is skipped for the first release, since the partition is the same as the main table.

        :param kwargs: The context passed from the PythonOperator.
        :return: None.
        """
        ti: TaskInstance = kwargs['ti']
        release = pull_release(ti)

        if release.first_release:
            print(self.partition_table_id)
            # execute bq load partition function