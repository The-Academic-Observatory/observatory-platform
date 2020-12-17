from airflow.models.variable import Variable
from airflow.exceptions import AirflowSkipException
from airflow.operators.python_operator import PythonOperator
from airflow.models.taskinstance import TaskInstance
import pendulum
import logging
from datetime import timedelta
import datetime
from typing import Callable, Type, Any, List, Tuple
from functools import partial
from observatory_platform.utils.config_utils import AirflowVar
from observatory_platform.utils.telescope_utils import bq_load_partition, bq_delete_old, \
    bq_append_from_partition, bq_append_from_file, cleanup, upload_files_to_cloud_storage
from observatory_platform.templates.updated_example.telescope import TelescopeRelease, Telescope, AbstractTelescopeRelease
import os
from typing_extensions import Protocol


class UserFunction(Protocol):
    """ Attempt to define function signature"""
    def __call__(self, release: 'AbstractTelescopeRelease', **kwargs: Any) -> Any: ...


class StreamRelease(TelescopeRelease):
    def __init__(self, dag_id: str, start_date: pendulum.Pendulum, end_date: pendulum.Pendulum,
                 first_release: bool = False):
        super().__init__(dag_id)
        self.start_date = start_date
        self.end_date = end_date
        self.first_release = first_release

    @property
    def date_str(self) -> str:
        return self.start_date.strftime("%Y_%m_%d") + "-" + self.end_date.strftime("%Y_%m_%d")


class StreamTelescope(Telescope):
    def __init__(self, release_cls: Type['StreamRelease'], dag_id: str, subdag_ids: list, queue: str,
                 schedule_interval: str, catchup: bool,
                 start_date: datetime, max_retries: int, description: str, dataset_id: str,
                 schema_version: str, airflow_vars: list, airflow_conns: list, transform_filenames: list,
                 merge_partition_fields: list, updated_date_fields: list, bq_merge_days: int):

        super().__init__(release_cls, start_date, dag_id, subdag_ids, schedule_interval, catchup, queue, max_retries,
                         description, airflow_vars, airflow_conns)
        self.dataset_id = dataset_id
        self.schema_version = schema_version
        # Set transform_bucket_name as required airflow variable
        self.airflow_vars = list(set([AirflowVar.transform_bucket_name.get()] + airflow_vars))
        self.airflow_conns = airflow_conns

        self.transform_filenames = transform_filenames
        self.merge_partition_fields = merge_partition_fields
        self.updated_date_fields = updated_date_fields
        if not (len(self.transform_filenames) == len(self.merge_partition_fields) == len(self.updated_date_fields)):
            #TODO raise some kind of error
            pass
        self.bq_merge_days = bq_merge_days

    @staticmethod
    def create_table_ids_from_blob(transform_blob) -> Tuple[str, str]:
        main_table_id = os.path.splitext(os.path.basename(transform_blob))[0]
        partition_table_id = main_table_id + '_partitions'
        return main_table_id, partition_table_id

    def make_operators(self, funcs: List[Callable]) -> List[Callable]:
        operators = []
        for func in funcs:
            operator = partial(PythonOperator, task_id=func.__name__, provide_context=True,
                               python_callable=partial(self.task_callable, func),
                               queue=self.queue,
                               trigger_rule='none_failed'
                               )
            operators.append(operator)
        return operators

    def task_callable(self, func: UserFunction, **kwargs):
        """ Invoke a task callable. Creates a Release instance and calls the given task method.
        :param func: the task method.
        :param kwargs: the context passed from the PythonOperator.
        See https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are passed
        to this argument.
        :return: None.
        """
        release = self.release_cls.make_release(**kwargs)
        result = func(release, **kwargs)
        return result

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

    def upload_transformed_task(self, release: StreamRelease, **kwargs):
        bucket_name = Variable.get(AirflowVar.transform_bucket_name.get())
        upload_blobs = []
        upload_paths = []
        for transform_path in release.transform_paths:
            if release.is_file_required(transform_path, self.transform_filenames):
                upload_blobs.append(release.get_blob_name(transform_path))
                upload_paths.append(transform_path)
        upload_files_to_cloud_storage(bucket_name, upload_blobs, upload_paths)

    def bq_load_partition_task(self, release: StreamRelease, **kwargs):
        if release.first_release:
            raise AirflowSkipException('Skipped, because first release')

        for transform_path in release.transform_paths:
            if release.is_file_required(transform_path, self.transform_filenames):
                transform_blob = release.get_blob_name(transform_path)
                main_table_id, partition_table_id = self.create_table_ids_from_blob(transform_blob)
                bq_load_partition(release.end_date, transform_blob, self.dataset_id, main_table_id,
                                  partition_table_id, self.schema_version)

    def bq_delete_old_task(self, release: StreamRelease, **kwargs):
        ti: TaskInstance = kwargs['ti']
        if release.first_release:
            # don't use AirflowSkipException, to ensure that task is in 'success' state
            logging.info('Skipped, because first release')
            return

        start_date = pendulum.instance(ti.previous_start_date_success)
        end_date = pendulum.instance(ti.start_date)
        if (end_date - start_date).days >= self.bq_merge_days:
            for transform_path in release.transform_paths:
                if release.is_file_required(transform_path, self.transform_filenames):
                    file_name, ext = os.path.splitext(os.path.basename(transform_path))
                    transform_blob = release.get_blob_name(transform_path)
                    idx = self.transform_filenames.index(file_name)

                    main_table_id, partition_table_id = self.create_table_ids_from_blob(transform_blob)
                    bq_delete_old(start_date, end_date, self.dataset_id, main_table_id, partition_table_id,
                                  self.merge_partition_fields[idx], self.updated_date_fields[idx])
        else:
            raise AirflowSkipException(f'Skipped, only delete old records every {self.bq_merge_days} days. '
                                       f'Last append was {(end_date - start_date).days} days ago')

    def bq_append_new_task(self, release: StreamRelease, **kwargs):
        ti: TaskInstance = kwargs['ti']

        if release.first_release:
            for transform_path in release.transform_paths:
                if release.is_file_required(transform_path, self.transform_filenames):
                    transform_blob = release.get_blob_name(transform_path)
                    main_table_id, partition_table_id = self.create_table_ids_from_blob(transform_blob)
                    bq_append_from_file(release.end_date, transform_blob, self.dataset_id, main_table_id,
                                        self.schema_version, self.description)
            return

        start_date = pendulum.instance(ti.previous_start_date_success)
        end_date = pendulum.instance(ti.start_date)
        if (end_date - start_date).days >= self.bq_merge_days:
            for transform_path in release.transform_paths:
                if release.is_file_required(transform_path, self.transform_filenames):
                    transform_blob = release.get_blob_name(transform_path)
                    main_table_id, partition_table_id = self.create_table_ids_from_blob(transform_blob)
                    bq_append_from_partition(start_date, end_date, self.dataset_id, main_table_id, partition_table_id,
                                             self.schema_version, self.description)
        else:
            raise AirflowSkipException(f'Skipped, not first release and only append new records every '
                                       f'{self.bq_merge_days} days. Last append was'
                                       f' {(end_date - start_date).days} days ago')

    def cleanup(self, release: StreamRelease, **kwargs):
        cleanup(release.download_dir, release.extract_dir, release.transform_dir)
