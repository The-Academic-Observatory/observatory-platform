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

# Author: Aniek Roelofs

import logging
import pendulum
from cerberus import rules_set_registry
from cerberus import Validator
from datetime import timedelta
from types import SimpleNamespace
from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.models.taskinstance import TaskInstance

from observatory_platform.utils.config_utils import (check_connections,
                                                     check_variables)
from observatory_platform.utils.telescope_utils import (check_dependencies_example,
                                                        transfer_example,
                                                        download_transferred_example,
                                                        download_example,
                                                        upload_downloaded,
                                                        extract_example,
                                                        transform_example,
                                                        upload_transformed,
                                                        bq_load_partition,
                                                        bq_delete_old,
                                                        bq_append_from_file,
                                                        bq_append_from_partition,
                                                        cleanup,
                                                        valid_cron_expression
                                                        )
from observatory_platform.utils.telescope_utils import TelescopeRelease


class StreamRelease(TelescopeRelease):
    """ Used to store info on a given release """
    def __init__(self, start_date: pendulum.Pendulum, end_date: pendulum.Pendulum, telescope: SimpleNamespace,
                 first_release: bool = False):
        super().__init__(start_date, end_date, telescope, first_release)


def create_release_example(start_date: pendulum.Pendulum, end_date: pendulum.Pendulum, telescope: SimpleNamespace,
                           first_release: bool) -> StreamRelease:
    """ Create a release instance.

    :param start_date: Start date of this run
    :param end_date: End date of this run
    :param telescope: Contains telescope properties
    :param first_release: Whether this is the first release to be obtained
    :return: Release instance
    """
    release = StreamRelease(start_date, end_date, telescope, first_release)
    return release


def pull_release(ti: TaskInstance) -> StreamRelease:
    """ Pulls xcom with release.

    :param ti: Task instance
    :return: One release
    """
    return ti.xcom_pull(key=StreamTelescope.RELEASES_XCOM, task_ids=StreamTelescope.TASK_ID_CREATE_RELEASE,
                        include_prior_dates=False)


def pull_last_start_date(ti: TaskInstance) -> pendulum.Pendulum:
    """ Pulls xcom with last start date of this run.

    :param ti: Task instance
    :return: Last start date
    """
    return ti.xcom_pull(key=StreamTelescope.LAST_START_XCOM, task_ids=StreamTelescope.TASK_ID_CREATE_RELEASE,
                        include_prior_dates=True)


class StreamTelescope:
    """ A container for holding the constants and static functions of this telescope. """
    RELEASES_XCOM = "release"
    LAST_START_XCOM = "last_start"

    TASK_ID_CHECK_DEPENDENCIES = "check_dependencies"
    TASK_ID_CREATE_RELEASE = "create_release"
    TASK_ID_TRANSFER = "transfer"
    TASK_ID_DOWNLOAD = "download"
    TASK_ID_UPLOAD_DOWNLOADED = "upload_downloaded"
    TASK_ID_DOWNLOAD_TRANSFERRED = "download_transferred"
    TASK_ID_EXTRACT = "extract"
    TASK_ID_TRANSFORM = "transform"
    TASK_ID_UPLOAD_TRANSFORMED = "upload_transformed"
    TASK_ID_BQ_LOAD_PARTITION = "bq_load_partition"
    TASK_ID_BQ_DELETE_OLD = "bq_delete_old"
    TASK_ID_BQ_APPEND_NEW = "bq_append_new"
    TASK_ID_CLEANUP = "cleanup"

    rules_set_registry.extend((('string', {'required': True, 'type': 'string', 'empty': False}),
                               ('strings', {'valuesrules': 'string'}),
                               ('list', {'required': True, 'type': 'list'}),
                               ('lists', {'valuesrules': 'list'}),
                               ('int', {'required': True, 'type': 'integer'}),
                               ('ints', {'valuesrules': 'int'})))

    telescope_schema = {'dag_id': 'string',
                        'queue': 'string',
                        'schedule_interval': {'required': True, 'type': 'string', 'check_with': valid_cron_expression},
                        'start_date': {'required': True, 'type': 'datetime'},
                        'max_retries': 'int',
                        'description': 'string',
                        'dataset_id': 'string',
                        'main_table_id': 'string',
                        'partition_table_id': 'string',
                        'merge_partition_field': 'string',
                        'updated_date_field': 'string',
                        'bq_merge_days': 'int',
                        'schema_version': {'required': True, 'type': 'string'},
                        'download_ext': 'string',
                        'extract_ext': 'string',
                        'transform_ext': 'string',
                        'airflow_vars': 'list',
                        'airflow_conns': 'list'
                        }

    telescope = SimpleNamespace(**telescope_schema)

    # optional functions to overwrite
    check_dependencies_custom = check_dependencies_example
    create_release_custom = create_release_example

    # required functions to overwrite
    transfer_custom = transfer_example
    download_custom = download_example
    download_transferred_custom = download_transferred_example
    extract_custom = extract_example
    transform_custom = transform_example

    @classmethod
    def check_dependencies(cls, **kwargs):
        """ Checks the 'telescope' attributes, airflow variables & connections and possibly additional custom checks.

        :param kwargs: The context passed from the PythonOperator.
        :return: None.
        """
        # check that all required attributes of simplenamespace are present, not empty and of the correct type
        v = Validator(cls.telescope_schema)
        # allow additional unknown attributes
        v.allow_unknown = True
        if not v.validate(cls.telescope.__dict__):
            raise AirflowException(f'Telescope properties dict not valid, errors: {v.errors}')

        # check that vars and connections are available
        vars_valid = check_variables(*cls.telescope.airflow_vars)
        conns_valid = check_connections(*cls.telescope.airflow_conns)
        if not vars_valid or not conns_valid:
            raise AirflowException('Required variables or connections are missing')

        cls.check_dependencies_custom(kwargs)

    @classmethod
    def create_release(cls, **kwargs):
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

        release = cls.create_release_custom(start_date, end_date, cls.telescope, first_release)

        ti.xcom_push(cls.RELEASES_XCOM, release)
        ti.xcom_push(cls.LAST_START_XCOM, end_date)

    @classmethod
    def transfer(cls, **kwargs) -> bool:
        """ Transfer the release data from one bucket to a Google Cloud bucket.

        :param kwargs: The context passed from the PythonOperator.
        :return: True if data is available and successfully transferred, else False.
        """
        ti: TaskInstance = kwargs['ti']
        release = pull_release(ti)

        success = cls.transfer_custom(release)
        return True if success else False

    @classmethod
    def download_transferred(cls, **kwargs):
        """ Download the transferred release data to disk.

        :param kwargs: The context passed from the PythonOperator.
        :return: None.
        """
        ti: TaskInstance = kwargs['ti']
        release = pull_release(ti)

        cls.download_transferred_custom(release)

    @classmethod
    def download(cls, **kwargs):
        """ Download release data to disk.

        :param kwargs: The context passed from the PythonOperator.
        :return: None.
        """
        ti: TaskInstance = kwargs['ti']
        release = pull_release(ti)

        success = cls.download_custom(release)
        return True if success else False

    @classmethod
    def upload_downloaded(cls, **kwargs):
        """ Upload downloaded release data to a Goocle Cloud bucket.

        :param kwargs: The context passed from the PythonOperator.
        :return: None.
        """
        ti: TaskInstance = kwargs['ti']
        release = pull_release(ti)

        upload_downloaded(release.download_dir, release.blob_dir)

    @classmethod
    def extract(cls, **kwargs):
        """ Extract release.

        :param kwargs: The context passed from the PythonOperator.
        :return: None.
        """
        ti: TaskInstance = kwargs['ti']
        release = pull_release(ti)

        cls.extract_custom(release)

    @classmethod
    def transform(cls, **kwargs):
        """ Transform release.

        :param kwargs: The context passed from the PythonOperator.
        :return: None.
        """
        ti: TaskInstance = kwargs['ti']
        release = pull_release(ti)

        cls.transform_custom(release)

    @classmethod
    def upload_transformed(cls, **kwargs):
        """ Upload transformed release to a Google Cloud bucket.

        :param kwargs: The context passed from the PythonOperator.
        :return: None.
        """
        ti: TaskInstance = kwargs['ti']
        release = pull_release(ti)

        upload_transformed(release.transform_path, release.transform_blob)

    @classmethod
    def bq_load_partition(cls, **kwargs):
        """ Load the data from the transformed release to a BigQuery partition (partitioned by ingestion date).
        This is skipped for the first release, since the partition is the same as the main table.

        :param kwargs: The context passed from the PythonOperator.
        :return: None.
        """
        ti: TaskInstance = kwargs['ti']
        release = pull_release(ti)

        if release.first_release:
            raise AirflowSkipException('Skipped, because first release')

        bq_load_partition(release.end_date, release.transform_blob, cls.telescope.dataset_id,
                          cls.telescope.main_table_id, cls.telescope.partition_table_id, cls.telescope.schema_version)

    @classmethod
    def bq_delete_old(cls, **kwargs):
        """ Every x days, delete the rows from the main table that are in the releases created since this task last ran.
        The frequency is determined by the 'bq_merge_days' attribute.
        Nothing is done for the first release, but the task is not skipped to ensure that the 'start_date' is updated
        for the next run.

        :param kwargs: The context passed from the PythonOperator.
        :return: None.
        """
        ti: TaskInstance = kwargs['ti']
        release = pull_release(ti)

        if release.first_release:
            # don't use AirflowSkipException, to ensure that task is in 'success' state
            logging.info('Skipped, because first release')
            return

        start_date = pendulum.instance(ti.previous_start_date_success)
        end_date = pendulum.instance(ti.start_date)
        if (end_date - start_date).days >= cls.telescope.bq_merge_days:
            bq_delete_old(start_date, end_date, cls.telescope.dataset_id,  cls.telescope.main_table_id,
                          cls.telescope.partition_table_id,  cls.telescope.merge_partition_field,
                          cls.telescope.updated_date_field)
        else:
            raise AirflowSkipException(f'Skipped, only delete old records every {cls.telescope.bq_merge_days} days. '
                                       f'Last append was {(end_date - start_date).days} days ago')

    @classmethod
    def bq_append_new(cls, **kwargs):
        """ Every x days, append the rows to the main table that are in the releases created since this task last ran.
        These are obtained from the corresponding BigQuery partitions that were created earlier.
        For the first release, the transformed file is simply used to create an initial main table.

        :param kwargs: The context passed from the PythonOperator.
        :return: None.
        """
        ti: TaskInstance = kwargs['ti']
        release = pull_release(ti)

        if release.first_release:
            bq_append_from_file(release.end_date, release.transform_blob, cls.telescope.dataset_id,
                                cls.telescope.main_table_id, cls.telescope.schema_version, cls.telescope.description)
            return

        start_date = pendulum.instance(ti.previous_start_date_success)
        end_date = pendulum.instance(ti.start_date)
        if (end_date - start_date).days >= cls.telescope.bq_merge_days:
            bq_append_from_partition(start_date, end_date, cls.telescope.dataset_id, cls.telescope.main_table_id,
                                     cls.telescope.partition_table_id, cls.telescope.schema_version,
                                     cls.telescope.description)
        else:
            raise AirflowSkipException(f'Skipped, not first release and only append new records every '
                                       f'{cls.telescope.bq_merge_days} days. Last append was'
                                       f' {(end_date - start_date).days} days ago')

    @classmethod
    def cleanup(cls, **kwargs):
        """ Delete release directories from disk.

        :param kwargs: The context passed from the PythonOperator.
        :return: None.
        """
        ti: TaskInstance = kwargs['ti']
        release = pull_release(ti)

        cleanup(release.download_dir, release.extract_dir, release.transform_dir)
