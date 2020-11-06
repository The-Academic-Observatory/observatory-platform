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
from types import SimpleNamespace
from typing import Optional
from airflow.exceptions import AirflowException
from airflow.models.taskinstance import TaskInstance
from cerberus import Validator, rules_set_registry

from observatory_platform.utils.config_utils import check_connections, check_variables
from observatory_platform.utils.telescope_utils import (TelescopeRelease,
                                                        bq_load_shard,
                                                        check_dependencies_example,
                                                        cleanup,
                                                        download_example,
                                                        upload_downloaded,
                                                        extract_example,
                                                        transform_example,
                                                        upload_transformed)


def list_releases_example(start_date: pendulum.Pendulum, end_date: pendulum.Pendulum, telescope: SimpleNamespace) -> \
        list:
    """ List all available releases between start and end date.

    :param start_date: Start date of this run
    :param end_date: End date of this run
    :param telescope: Contains telescope properties
    :return: List of releases
    """
    releases = []
    release_date = pendulum.parse('2020-02-01')
    if start_date <= release_date < end_date:
        release = SnapshotRelease(None, release_date, telescope, False)
        releases.append(release)
    return releases


def pull_releases(ti: TaskInstance) -> list:
    """ Pulls xcom with list of releases.

    :param ti: Task instance
    :return: List of releases
    """
    return ti.xcom_pull(key=SnapshotTelescope.RELEASES_XCOM, task_ids=SnapshotTelescope.TASK_ID_LIST_RELEASES,
                        include_prior_dates=False)


class SnapshotRelease(TelescopeRelease):
    """ Used to store info on a given release"""
    def __init__(self, start_date: Optional[pendulum.Pendulum], end_date: pendulum.Pendulum,
                 telescope: SimpleNamespace, first_release: bool = False):
        super().__init__(start_date, end_date, telescope, first_release)
        self.date_str = self.release_date.strftime("%Y_%m_%d")
        self.telescope = telescope


class SnapshotTelescope:
    """ A container for holding the constants and static functions of this telescope. """
    RELEASES_XCOM = "releases"

    TASK_ID_CHECK_DEPENDENCIES = "check_dependencies"
    TASK_ID_LIST_RELEASES = "list_releases"
    TASK_ID_DOWNLOAD = "download"
    TASK_ID_UPLOAD_DOWNLOADED = "upload_downloaded"
    TASK_ID_EXTRACT = "extract"
    TASK_ID_TRANSFORM = "transform"
    TASK_ID_UPLOAD_TRANSFORMED = "upload_transformed"
    TASK_ID_BQ_LOAD = "bq_load"
    TASK_ID_CLEANUP = "cleanup"

    rules_set_registry.extend((('string', {'required': True, 'type': 'string', 'empty': False}),
                               ('strings', {'valuesrules': 'string'}),
                               ('list', {'required': True, 'type': 'list'}),
                               ('lists', {'valuesrules': 'list'}),
                               ('int', {'required': True, 'type': 'integer'}),
                               ('ints', {'valuesrules': 'int'})))

    telescope_schema = {'dag_id': 'string',
                        'queue': 'string',
                        'max_retries': 'int',
                        'description': 'string',
                        'dataset_id': 'string',
                        'table_id': 'string',
                        'schema_version': 'string',
                        'download_ext': 'string',
                        'extract_ext': 'string',
                        'transform_ext': 'string',
                        'airflow_vars': 'list',
                        'airflow_conns': 'list'}

    telescope = SimpleNamespace(**telescope_schema)

    # optional functions to overwrite
    check_dependencies_custom = check_dependencies_example

    # required functions to overwrite
    list_releases_custom = list_releases_example
    download_custom = download_example
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
    def list_releases(cls, **kwargs) -> bool:
        """ List all available releases between this execution date and the next execution date.

        :param kwargs: The context passed from the PythonOperator.
        :return: True if any releases are available, else False.
        """
        ti: TaskInstance = kwargs['ti']
        start_date = kwargs['execution_date']
        end_date = kwargs['next_execution_date']

        releases = cls.list_releases_custom(start_date, end_date, cls.telescope)
        for release in releases:
            logging.info(f'Release date: {release.date_str}')

        ti.xcom_push(SnapshotTelescope.RELEASES_XCOM, releases)
        return True if releases else False

    @classmethod
    def download(cls, **kwargs):
        """ Download all release data to disk.

        :param kwargs: The context passed from the PythonOperator.
        :return: None.
        """
        ti: TaskInstance = kwargs['ti']
        releases = pull_releases(ti)

        for release in releases:
            cls.download_custom(release)

    @classmethod
    def upload_downloaded(cls, **kwargs):
        """ Upload all downloaded releases to a Google Cloud bucket.

        :param kwargs: The context passed from the PythonOperator.
        :return: None.
        """
        ti: TaskInstance = kwargs['ti']
        releases = pull_releases(ti)

        for release in releases:
            upload_downloaded(release.download_dir, release.blob_dir)

    @classmethod
    def extract(cls, **kwargs):
        """ Extract all releases.

        :param kwargs: The context passed from the PythonOperator.
        :return: None.
        """
        ti: TaskInstance = kwargs['ti']
        releases = pull_releases(ti)

        for release in releases:
            cls.extract_custom(release)

    @classmethod
    def transform(cls, **kwargs):
        """ Transform all releases

        :param kwargs: The context passed from the PythonOperator.
        :return: None.
        """
        ti: TaskInstance = kwargs['ti']
        releases = pull_releases(ti)

        for release in releases:
            cls.transform_custom(release)

    @classmethod
    def upload_transformed(cls, **kwargs):
        """ Upload all transformed releases to a Google Cloud bucket.

        :param kwargs: The context passed from the PythonOperator.
        :return: None.
        """
        ti: TaskInstance = kwargs['ti']
        releases = pull_releases(ti)

        for release in releases:
            upload_transformed(release.transform_path, release.transform_blob)

    @classmethod
    def bq_load(cls, **kwargs):
        """ Load all transformed releases to an individual BigQuery table shard.

        :param kwargs: The context passed from the PythonOperator.
        :return: None.
        """
        ti: TaskInstance = kwargs['ti']
        releases = pull_releases(ti)

        for release in releases:
            bq_load_shard(release.end_date, release.transform_blob, cls.telescope.dataset_id,
                          cls.telescope.table_id, cls.telescope.schema_version)

    @classmethod
    def cleanup(cls, **kwargs):
        """ Delete release directories from disk.

        :param kwargs: The context passed from the PythonOperator.
        :return: None.
        """
        ti: TaskInstance = kwargs['ti']
        releases = pull_releases(ti)

        for release in releases:
            cleanup(release.download_dir, release.extract_dir, release.transform_dir)
