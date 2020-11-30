from abc import ABC, abstractmethod
from airflow import DAG
from airflow.exceptions import AirflowSkipException, AirflowException
from airflow.operators.python_operator import PythonOperator, ShortCircuitOperator
from airflow.models.taskinstance import TaskInstance
from airflow.utils.helpers import chain
import pendulum
import logging
from datetime import timedelta
import datetime
from typing import Callable
from functools import partial
from observatory_platform.utils.config_utils import check_variables, check_connections
from observatory_platform.utils.telescope_utils import bq_load_partition, upload_transformed, bq_delete_old, \
    bq_append_from_partition, bq_append_from_file, cleanup, normalize_schedule_interval
from croniter import croniter
from observatory_platform.templates.updated_example.telescope import TelescopeRelease


class AbstractStreamTelescope(ABC):
    @abstractmethod
    def make_release(self, start_date: pendulum.Pendulum, end_date: pendulum.Pendulum,
                     first_release: bool) -> TelescopeRelease:
        pass

    @abstractmethod
    def download_pull(self, release: TelescopeRelease, **kwargs) -> bool:
        pass

    @abstractmethod
    def download_push(self, release: TelescopeRelease, **kwargs):
        pass

    @abstractmethod
    def extract(self, release: TelescopeRelease, **kwargs):
        pass

    @abstractmethod
    def transform(self, release: TelescopeRelease, **kwargs):
        pass

    @abstractmethod
    def task_callable(self, func: Callable, **kwargs):
        pass

    @abstractmethod
    def make_dag(self) -> DAG:
        pass

    @abstractmethod
    def check_dependencies(self, **kwargs):
        pass

    @abstractmethod
    def get_release_info(self, **kwargs):
        pass

    @abstractmethod
    def upload_transformed(self, release: TelescopeRelease, **kwargs):
        pass

    @abstractmethod
    def bq_load_partition(self, release: TelescopeRelease, **kwargs):
        pass

    @abstractmethod
    def bq_delete_old(self, release: TelescopeRelease, **kwargs):
        pass

    @abstractmethod
    def bq_append_new(self, release: TelescopeRelease, **kwargs):
        pass

    @abstractmethod
    def cleanup(self, release: TelescopeRelease, **kwargs):
        pass


class StreamTelescope(AbstractStreamTelescope):
    RELEASE_INFO = 'releases'

    def __init__(self, dag_id, queue: str, schedule_interval: str, start_date: datetime, max_retries: int,
                 description: str, download_pull_first: bool, dataset_id: str, schema_version: str, airflow_vars: list,
                 airflow_conns: list,
                 download_ext: str, extract_ext: str, transform_ext: str, main_table_id: str, partition_table_id: str,
                 merge_partition_field: str, updated_date_field: str, bq_merge_days: int):

        if not croniter.is_valid(normalize_schedule_interval(schedule_interval)):
            raise AirflowException(f"Not a valid cron expression: {schedule_interval}")
        self.dag_id = dag_id
        self.queue = queue
        self.schedule_interval = schedule_interval
        self.start_date = start_date
        self.max_retries = max_retries
        self.description = description
        self.download_pull_first = download_pull_first
        self.dataset_id = dataset_id
        self.schema_version = schema_version
        self.airflow_vars = airflow_vars
        self.airflow_conns = airflow_conns
        self.extensions = {'download': download_ext, 'extract': extract_ext, 'transform': transform_ext}

        self.main_table_id = main_table_id
        self.partition_table_id = partition_table_id
        self.merge_partition_field = merge_partition_field
        self.updated_date_field = updated_date_field
        self.bq_merge_days = bq_merge_days

    def task_callable(self, func: Callable, **kwargs):
        """ Invoke a task callable. Creates a Release instance and calls the given task method.
        :param func: the task method.
        :param kwargs: the context passed from the PythonOperator.
        See https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are passed
        to this argument.
        :return: None.
        """

        # Make Release instance
        ti: TaskInstance = kwargs['ti']
        start_date, end_date, first_release = ti.xcom_pull(key=self.RELEASE_INFO,
                                                           task_ids=self.get_release_info.__name__,
                                                           include_prior_dates=True)
        release = self.make_release(start_date, end_date, first_release)
        result = func(release, **kwargs)
        return result

    def make_dag(self) -> DAG:
        default_args = {
            "owner": "airflow",
            "start_date": self.start_date,
            # 'on_failure_callback': on_failure_callback
        }

        task_funcs = [self.check_dependencies, self.get_release_info, self.download_push, self.download_pull,
                      self.extract,  self.transform, self.upload_transformed,
                      self.bq_load_partition, self.bq_delete_old, self.bq_append_new, self.cleanup]
        if self.download_pull_first:
            pull, push = task_funcs.index(self.download_pull), task_funcs.index(self.download_push)
            task_funcs[pull], task_funcs[push] = task_funcs[push], task_funcs[pull]

        with DAG(dag_id=self.dag_id, schedule_interval=self.schedule_interval, default_args=default_args, catchup=False,
                 max_active_runs=1, doc_md=self.__doc__) as dag:
            tasks = []
            for func in task_funcs:
                if func == self.check_dependencies or func == self.get_release_info:
                    task = PythonOperator(task_id=func.__name__,
                                          provide_context=True,
                                          python_callable=func)
                elif func == self.download_pull:
                    task = ShortCircuitOperator(task_id=func.__name__,
                                                provide_context=True,
                                                python_callable=partial(self.task_callable, func))
                else:
                    task = PythonOperator(task_id=func.__name__,
                                          provide_context=True,
                                          python_callable=partial(self.task_callable, func))
                tasks.append(task)

            chain(*tasks)
        return dag

    def check_dependencies(self, **kwargs):
        """ Checks the 'telescope' attributes, airflow variables & connections and possibly additional custom checks.

        :param kwargs: The context passed from the PythonOperator.
        :return: None.
        """
        # check that vars and connections are available
        vars_valid = True
        conns_valid = True
        if self.airflow_vars:
            vars_valid = check_variables(self.airflow_vars)
        if self.airflow_conns:
            conns_valid = check_connections(self.airflow_conns)

        if not vars_valid or not conns_valid:
            raise AirflowException('Required variables or connections are missing')

    def get_release_info(self, **kwargs):
        """ Create a release instance and update the xcom value with the last start date.

        :param kwargs: The context passed from the PythonOperator.
        :return: None.
        """
        ti: TaskInstance = kwargs['ti']

        first_release = False
        release_info = ti.xcom_pull(key=self.RELEASE_INFO, include_prior_dates=True)
        if not release_info:
            first_release = True
            start_date = pendulum.instance(kwargs['dag'].default_args['start_date']).start_of('day')
        else:
            start_date = release_info[1]
        start_date = start_date - timedelta(days=1)
        end_date = pendulum.utcnow()
        logging.info(f'Start date: {start_date}, end date: {end_date}, first release: {first_release}')

        ti.xcom_push(self.RELEASE_INFO, (start_date, end_date, first_release))

    def upload_transformed(self, release: TelescopeRelease, **kwargs):
        upload_transformed(release.transform_path, release.transform_blob)

    def bq_load_partition(self, release: TelescopeRelease, **kwargs):
        if release.first_release:
            raise AirflowSkipException('Skipped, because first release')

        bq_load_partition(release.end_date, release.transform_blob, self.dataset_id, self.main_table_id,
                          self.partition_table_id, self.schema_version)

    def bq_delete_old(self, release: TelescopeRelease, **kwargs):
        ti: TaskInstance = kwargs['ti']
        if release.first_release:
            # don't use AirflowSkipException, to ensure that task is in 'success' state
            logging.info('Skipped, because first release')
            return

        start_date = pendulum.instance(ti.previous_start_date_success)
        end_date = pendulum.instance(ti.start_date)
        if (end_date - start_date).days >= self.bq_merge_days:
            bq_delete_old(start_date, end_date, self.dataset_id, self.main_table_id,
                          self.partition_table_id, self.merge_partition_field,
                          self.updated_date_field)
        else:
            raise AirflowSkipException(f'Skipped, only delete old records every {cls.telescope.bq_merge_days} days. '
                                       f'Last append was {(end_date - start_date).days} days ago')

    def bq_append_new(self, release: TelescopeRelease, **kwargs):
        ti: TaskInstance = kwargs['ti']

        if release.first_release:
            bq_append_from_file(release.end_date, release.transform_blob, self.dataset_id,
                                self.main_table_id, self.schema_version, self.description)
            return

        start_date = pendulum.instance(ti.previous_start_date_success)
        end_date = pendulum.instance(ti.start_date)
        if (end_date - start_date).days >= self.bq_merge_days:
            bq_append_from_partition(start_date, end_date, self.dataset_id, self.main_table_id,
                                     self.partition_table_id, self.schema_version, self.description)
        else:
            raise AirflowSkipException(f'Skipped, not first release and only append new records every '
                                       f'{self.bq_merge_days} days. Last append was'
                                       f' {(end_date - start_date).days} days ago')

    def cleanup(self, release: TelescopeRelease, **kwargs):
        cleanup(release.download_dir, release.extract_dir, release.transform_dir)