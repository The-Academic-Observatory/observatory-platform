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

# Author: Aniek Roelofs, James Diprose, Tuan Chien

import datetime
import logging
from datetime import timedelta
from typing import Dict

import pendulum
from airflow.exceptions import AirflowSkipException
from airflow.models.taskinstance import TaskInstance
from observatory.platform.telescopes.telescope import Release, Telescope
from observatory.platform.utils.airflow_utils import AirflowVars
from observatory.platform.utils.template_utils import blob_name, \
    bq_append_from_file, \
    bq_append_from_partition, \
    bq_delete_old, \
    bq_load_ingestion_partition, \
    table_ids_from_path, \
    upload_files_from_list


class StreamRelease(Release):
    def __init__(self, dag_id: str, start_date: pendulum.Pendulum, end_date: pendulum.Pendulum,
                 first_release: bool, download_files_regex: str = None, extract_files_regex: str = None,
                 transform_files_regex: str = None):
        """ Construct a StreamRelease instance
        :param dag_id: the id of the DAG.
        :param start_date: the start_date of the release.
        :param end_date: the end_date of the release.
        :param first_release: whether this is the first release that is processed for this DAG
        :param download_files_regex: regex pattern that is used to find files in download folder
        :param extract_files_regex: regex pattern that is used to find files in extract folder
        :param transform_files_regex: regex pattern that is used to find files in transform folder
        """
        self.start_date = start_date
        self.end_date = end_date
        self.first_release = first_release
        release_id = self.start_date.strftime("%Y_%m_%d") + "-" + self.end_date.strftime("%Y_%m_%d")
        super().__init__(dag_id, release_id, download_files_regex, extract_files_regex, transform_files_regex)


class StreamTelescope(Telescope):
    def __init__(self, dag_id: str, start_date: datetime, schedule_interval: str, dataset_id: str,
                 merge_partition_field: str, updated_date_field: str, bq_merge_days: int, catchup: bool = False,
                 queue: str = 'default', max_retries: int = 3, max_active_runs: int = 1, schema_prefix: str = '',
                 schema_version: str = None, load_bigquery_table_kwargs: Dict = None,
                 dataset_description: str = '', table_descriptions: Dict[str, str] = None,
                 airflow_vars: list = None, airflow_conns: list = None):
        """ Construct a StreamTelescope instance.

        :param dag_id: the id of the DAG.
        :param start_date: the start date of the DAG.
        :param schedule_interval: the schedule interval of the DAG.
        :param dataset_id: the dataset id.
        :param merge_partition_field: the BigQuery field used to match partitions for a merge
        :param updated_date_field: the BigQuery field used to determine newest entry for a merge
        :param bq_merge_days: how often partitions should be merged (every x days)
        :param catchup: whether to catchup the DAG or not.
        :param queue: the Airflow queue name.
        :param max_retries: the number of times to retry each task.
        :param max_active_runs: the maximum number of DAG runs that can be run at once.
        :param schema_prefix: the prefix used to find the schema path
        :param schema_version: the version used to find the schema path
        :param load_bigquery_table_kwargs: the customisation parameters for loading data into a BigQuery table.
        :param dataset_description: description for the BigQuery dataset.
        :param table_descriptions: a dictionary with table ids and corresponding table descriptions
        :param airflow_vars: list of airflow variable keys, for each variable it is checked if it exists in airflow
        :param airflow_conns: list of airflow connection keys, for each connection it is checked if it exists in airflow
        """

        # Set transform_bucket_name as required airflow variable
        if not airflow_vars:
            airflow_vars = []
        airflow_vars = list(set([AirflowVars.TRANSFORM_BUCKET] + airflow_vars))

        super().__init__(dag_id, start_date, schedule_interval, catchup=catchup, queue=queue, max_retries=max_retries,
                         max_active_runs=max_active_runs, airflow_vars=airflow_vars, airflow_conns=airflow_conns)

        self.schema_prefix = schema_prefix
        self.schema_version = schema_version
        self.dataset_id = dataset_id
        self.merge_partition_field = merge_partition_field
        self.updated_date_field = updated_date_field
        self.bq_merge_days = bq_merge_days
        self.load_bigquery_table_kwargs = load_bigquery_table_kwargs if load_bigquery_table_kwargs else dict()
        self.dataset_description = dataset_description
        self.table_descriptions = table_descriptions if table_descriptions else dict()

    def get_release_info(self, **kwargs) -> bool:
        """ Create a release instance and update the xcom value with the last start date.
        :param kwargs: The context passed from the PythonOperator.
        :return: None.
        """
        ti: TaskInstance = kwargs['ti']

        first_release = False
        release_info = ti.xcom_pull(key=self.RELEASE_INFO, include_prior_dates=True)
        if not release_info:
            first_release = True
            # set start date to the start of the DAG
            start_date = pendulum.instance(kwargs['dag'].default_args['start_date']).start_of('day')
        else:
            # set start date to end date of previous DAG run
            start_date = release_info[1]
        end_date = pendulum.today('UTC') - timedelta(days=1)
        logging.info(f'Start date: {start_date}, end date: {end_date}, first release: {first_release}')

        ti.xcom_push(self.RELEASE_INFO, (start_date, end_date, first_release))
        return True

    def upload_transformed(self, release: StreamRelease, **kwargs):
        """ Upload the transformed files for the given release.
        :param release: a StreamRelease instance
        :param kwargs: The context passed from the PythonOperator.
        :return: None.
        """
        upload_files_from_list(release.transform_files, release.transform_bucket)

    def bq_load_partition(self, release: StreamRelease, **kwargs):
        """ For each file listed in transform_files, create a time based partition in BigQuery
        :param release: a StreamRelease instance
        :param kwargs: The context passed from the PythonOperator.
        :return: None.
        """
        if release.first_release:
            # The main table is the same as the partition, so no need to upload the partition as well. Especially
            # because a first release can be relatively big in size.
            raise AirflowSkipException('Skipped, because first release')

        for transform_path in release.transform_files:
            transform_blob = blob_name(transform_path)
            main_table_id, partition_table_id = table_ids_from_path(transform_path)
            table_description = self.table_descriptions.get(main_table_id, '')
            bq_load_ingestion_partition(release.end_date, transform_blob, self.dataset_id, main_table_id,
                                        partition_table_id, self.schema_prefix, self.schema_version,
                                        self.dataset_description, table_description=table_description,
                                        **self.load_bigquery_table_kwargs)

    def bq_delete_old(self, release: StreamRelease, **kwargs):
        """ Delete old rows from the 'main' table, based on rows that are in a partition of the 'partitions' table.
        :param release: a StreamRelease instance
        :param kwargs: The context passed from the PythonOperator.
        :return: None.
        """
        ti: TaskInstance = kwargs['ti']
        if release.first_release:
            # don't use AirflowSkipException, to ensure that task is in 'success' state, this is used below in
            # ti.previous_start_date_success
            logging.info('Skipped, because first release')
            return

        start_date = pendulum.instance(ti.previous_start_date_success)
        end_date = pendulum.instance(ti.start_date)
        if (end_date - start_date).days >= self.bq_merge_days:
            logging.info(f'Deleting old data from main table from partitions between dates: {start_date}, {end_date}')
            for transform_path in release.transform_files:
                main_table_id, partition_table_id = table_ids_from_path(transform_path)
                bq_delete_old(start_date, end_date, self.dataset_id, main_table_id, partition_table_id,
                              self.merge_partition_field, self.updated_date_field)
        else:
            raise AirflowSkipException(f'Skipped, only delete old records every {self.bq_merge_days} days. '
                                       f'Last delete was {(end_date - start_date).days} days ago')

    def bq_append_new(self, release: StreamRelease, **kwargs):
        """ Append rows to the 'main' table, based on rows that are in a partition of the 'partitions' table.
        :param release: a StreamRelease instance
        :param kwargs: The context passed from the PythonOperator.
        :return: None.
        """
        ti: TaskInstance = kwargs['ti']

        if release.first_release:
            for transform_path in release.transform_files:
                transform_blob = blob_name(transform_path)
                main_table_id, partition_table_id = table_ids_from_path(transform_path)
                table_description = self.table_descriptions.get(main_table_id, '')
                bq_append_from_file(release.end_date, transform_blob, self.dataset_id, main_table_id,
                                    self.schema_prefix, self.schema_version, self.dataset_description,
                                    table_description=table_description, **self.load_bigquery_table_kwargs)
            return

        start_date = pendulum.instance(ti.previous_start_date_success)
        end_date = pendulum.instance(ti.start_date)
        if (end_date - start_date).days >= self.bq_merge_days:
            logging.info(f'Appending data to main table from partitions between dates: {start_date}, {end_date}')
            for transform_path in release.transform_files:
                main_table_id, partition_table_id = table_ids_from_path(transform_path)
                bq_append_from_partition(start_date, end_date, self.dataset_id, main_table_id, partition_table_id,
                                         self.schema_prefix, self.schema_version)
        else:
            raise AirflowSkipException(f'Skipped, not first release and only append new records every '
                                       f'{self.bq_merge_days} days. Last append was'
                                       f' {(end_date - start_date).days} days ago')

    def cleanup(self, release: StreamRelease, **kwargs):
        """ Delete downloaded, extracted and transformed files of the release.
        :param release: a StreamRelease instance
        :param kwargs: The context passed from the PythonOperator.
        :return: None.
        """
        release.cleanup()
