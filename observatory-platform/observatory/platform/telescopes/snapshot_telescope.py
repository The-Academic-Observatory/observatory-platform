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
from typing import List

import pendulum
from observatory.platform.telescopes.telescope import Release, Telescope
from observatory.platform.utils.airflow_utils import AirflowVars
from observatory.platform.utils.template_utils import blob_name, \
    bq_load_shard, \
    table_ids_from_path, \
    upload_files_from_list


class SnapshotRelease(Release):
    def __init__(self, dag_id: str, release_date: pendulum.Pendulum, download_files_regex: str = None,
                 extract_files_regex: str = None, transform_files_regex: str = None):
        self.release_date = release_date
        release_id = f'{self.dag_id}_{self.release_date.strftime("%Y_%m_%d")}'
        super().__init__(dag_id, release_id, download_files_regex, extract_files_regex, transform_files_regex)


class SnapshotTelescope(Telescope):
    def __init__(self, dag_id: str, start_date: datetime, schedule_interval: str, dataset_id: str,
                 queue: str = 'default', catchup: bool = True, max_retries: int = 3, max_active_runs: int = 1,
                 schema_prefix: str = '', schema_version: str = None, airflow_vars: list = None,
                 airflow_conns: list = None):

        super().__init__(dag_id, start_date, schedule_interval, catchup, queue, max_retries, max_active_runs,
                         schema_prefix, schema_version, airflow_vars, airflow_conns)
        self.dataset_id = dataset_id
        # Set transform_bucket_name as required airflow variable
        if not airflow_vars:
            airflow_vars = []
        self.airflow_vars = list(set([AirflowVars.TRANSFORM_BUCKET] + airflow_vars))
        self.airflow_conns = airflow_conns

    def upload_transformed(self, releases: List[SnapshotRelease], **kwargs):
        """ Task to upload each transformed release to a google cloud bucket
        :param releases:
        :param kwargs:
        :return:
        """
        for release in releases:
            upload_files_from_list(release.transform_files, release.transform_bucket)

    def bq_load(self, releases: List[SnapshotRelease], **kwargs):
        """ Task to load each transformed release to BigQuery.
        :param releases: a list of GRID releases.
        :return: None.
        """

        # Load each transformed release
        for release in releases:
            for transform_path in release.transform_files:
                transform_blob = blob_name(transform_path)
                table_id, _ = table_ids_from_path(transform_path)
                bq_load_shard(release.release_date, transform_blob, self.dataset_id, table_id, self.schema_prefix,
                              self.schema_version, self.description)

    def cleanup(self, releases: List[SnapshotRelease], **kwargs):
        """ Delete files of downloaded, extracted and transformed release.
        :param releases: the oapen metadata release
        :return: None.
        """
        # Delete files from each release
        for release in releases:
            release.cleanup()
