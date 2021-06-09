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

# Author: Aniek Roelofs, James Diprose


from __future__ import annotations, annotations

import datetime
import functools
import logging
import os
import shutil
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from subprocess import Popen
from typing import Dict, List

import requests
from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook
from natsort import natsorted
from observatory.platform.telescopes.snapshot_telescope import SnapshotRelease, SnapshotTelescope
from observatory.platform.utils.airflow_utils import AirflowConns, AirflowVars
from observatory.platform.utils.proc_utils import wait_for_process
from observatory.platform.utils.template_utils import upload_files_from_list, bq_load_shard, table_ids_from_path, blob_name
from observatory.platform.utils.url_utils import retry_session
from pendulum import Pendulum


class CrossrefMetadataRelease(SnapshotRelease):
    def __init__(self, dag_id: str, release_date: Pendulum):
        """ Create a CrossrefMetadataRelease instance.

        :param dag_id: the DAG id.
        :param release_date: the date of the release.
        """

        download_files_regex = '.*.json.tar.gz$'
        extract_files_regex = f'.*.json$'
        transform_files_regex = f'.*.jsonl$'
        super().__init__(dag_id, release_date, download_files_regex, extract_files_regex, transform_files_regex)

        self.url = CrossrefMetadataTelescope.TELESCOPE_URL.format(year=release_date.year, month=release_date.month)

    @property
    def download_path(self) -> str:
        """ Get the path to the downloaded file.

        :return: the file path.
        """

        return os.path.join(self.download_folder, 'crossref_metadata.json.tar.gz')

    def download(self):
        """ Downloads release

        :return:  None.
        """

        logging.info(f"Downloading from url: {self.url}")

        # Set API token header
        connection = BaseHook.get_connection(AirflowConns.CROSSREF)
        api_token = connection.password
        header = {
            'Crossref-Plus-API-Token': f'Bearer {api_token}'
        }

        # Download release
        with requests.get(self.url, headers=header, stream=True) as response:
            # Check if authorisation with the api token was successful or not, raise error if not successful
            if response.status_code != 200:
                raise ConnectionError(f"Error downloading file {self.url}, status_code={response.status_code}")

            # Open file for saving
            with open(self.download_path, 'wb') as file:
                response.raw.read = functools.partial(response.raw.read, decode_content=True)
                shutil.copyfileobj(response.raw, file)

        logging.info(f"Successfully download url to {self.download_path}")

    def extract(self):
        """ Extract release. Decompress and unzip file to multiple json files.

        """
        logging.info(f"extract_release: {self.download_path}")

        # Run command using GNUtar, bsdtar (on e.g. OS x) might give error: 'Error inclusion pattern: Failed to open
        # 'pigz -d'
        cmd = f'tar -xv -I "pigz -d" -f {self.download_path} -C {self.extract_folder}'
        p: Popen = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                    executable='/bin/bash')
        stdout, stderr = wait_for_process(p)
        logging.debug(stdout)
        success = p.returncode == 0 and 'error' not in stderr.lower()

        if success:
            logging.info(f"extract_release success: {self.download_path}")
        else:
            logging.error(stdout)
            logging.error(stderr)
            raise AirflowException(f"extract_release error: {self.download_path}")

    def transform(self, max_workers: int):
        """ Transform the Crossref Metadata release.
        Each extracted file is transformed. This is done in parallel using the ThreadPoolExecutor.
        Once all individual files are transformed they are concatenated together and gzipped.

        :param max_workers: the number of processes to use when transforming files (one process per file).
        :return: whether the transformation was successful or not.
        """
        logging.info(f'Transform input folder: {self.extract_folder}, output folder: {self.transform_folder}')
        finished = 0
        # Transform each file in parallel
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = []

            # List files and sort so that they are processed in ascending order
            input_file_paths = natsorted(self.extract_files)

            # Create tasks for each file
            for input_file in input_file_paths:
                # The output file will be a json lines file, hence adding the 'l' to the file extension
                output_file = os.path.join(self.transform_folder, os.path.basename(input_file) + 'l')
                future = executor.submit(transform_file, input_file, output_file)
                futures.append(future)

            # Wait for completed tasks
            for future in as_completed(futures):
                future.result()
                finished += 1
                if finished % 1000 == 0:
                    logging.info(f'Transformed {finished} files')


class CrossrefMetadataTelescope(SnapshotTelescope):
    """
    The Crossref Metadata Telescope

    Saved to the BigQuery table: <project_id>.crossref.crossref_metadataYYYYMMDD
    """

    DAG_ID = 'crossref_metadata'
    DATASET_ID = 'crossref'
    SCHEDULE_INTERVAL = '@monthly'
    TELESCOPE_URL = 'https://api.crossref.org/snapshots/monthly/{year}/{month:02d}/all.json.tar.gz'

    def __init__(self, dag_id: str = DAG_ID, start_date: datetime = datetime(2020, 6, 7),
                 schedule_interval: str = SCHEDULE_INTERVAL, dataset_id: str = 'crossref',
                 dataset_description: str = 'The Crossref Metadata Plus dataset: '
                                            'https://www.crossref.org/services/metadata-retrieval/metadata-plus/',
                 load_bigquery_table_kwargs: Dict = None, table_descriptions: Dict = None,
                 airflow_vars: List = None, airflow_conns: List = None, max_active_runs: int = 1,
                 max_processes: int = min(32, os.cpu_count() + 4)):

        """ The Crossref Metadata telescope

        :param dag_id: the id of the DAG.
        :param start_date: the start date of the DAG.
        :param schedule_interval: the schedule interval of the DAG.
        :param dataset_id: the BigQuery dataset id.
        :param dataset_description: description for the BigQuery dataset.
        :param load_bigquery_table_kwargs: the customisation parameters for loading data into a BigQuery table.
        :param table_descriptions: a dictionary with table ids and corresponding table descriptions.
        :param airflow_vars: list of airflow variable keys, for each variable it is checked if it exists in airflow.
        :param airflow_conns: list of airflow connection keys, for each connection it is checked if it exists in airflow
        :param max_active_runs: the maximum number of DAG runs that can be run at once.
        :param max_processes: the number of max workers used in ThreadPoolExecutor to transform files in parallel.
        """

        if table_descriptions is None:
            table_descriptions = {dag_id: 'A single Crossref Metadata snapshot.'}

        if airflow_vars is None:
            airflow_vars = [AirflowVars.DATA_PATH, AirflowVars.PROJECT_ID, AirflowVars.DATA_LOCATION,
                            AirflowVars.DOWNLOAD_BUCKET, AirflowVars.TRANSFORM_BUCKET]
        if airflow_conns is None:
            airflow_conns = [AirflowConns.CROSSREF]
        super().__init__(dag_id, start_date, schedule_interval, dataset_id,
                         load_bigquery_table_kwargs=load_bigquery_table_kwargs,
                         dataset_description=dataset_description,
                         table_descriptions=table_descriptions,
                         airflow_vars=airflow_vars,
                         airflow_conns=airflow_conns,
                         max_active_runs=max_active_runs)
        self.max_processes = max_processes

        self.add_setup_task(self.check_dependencies)
        self.add_setup_task(self.check_release_exists)
        self.add_task(self.download)
        self.add_task(self.upload_downloaded)
        self.add_task(self.extract)
        self.add_task(self.transform)
        self.add_task(self.upload_transformed)
        self.add_task(self.bq_load)
        self.add_task(self.cleanup)

    def make_release(self, **kwargs) -> List[CrossrefMetadataRelease]:
        """ Make release instances. The release is passed as an argument to the function (TelescopeFunction) that is
        called in 'task_callable'.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are
        passed to this argument.
        :return: a list of CrossrefMetadataRelease instances.
        """

        release_date = kwargs['execution_date']
        return [CrossrefMetadataRelease(self.dag_id, release_date)]

    @staticmethod
    def check_release_exists(**kwargs):
        """ Check that the release for this month exists.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        # Construct the release for the execution date and check if it exists.
        # The release release for a given execution_date is added on the 5th day of the following month.
        # E.g. the 2020-05 release is added to the website on 2020-06-05.
        execution_date = kwargs['execution_date']

        url = CrossrefMetadataTelescope.TELESCOPE_URL.format(year=execution_date.year, month=execution_date.month)
        logging.info('Checking if release exists')

        response = retry_session().head(url)
        if response.status_code == 302:
            logging.info(f'Snapshot exists at url: {url}, response code: {response.status_code}')
            return True
        elif response.reason == 'Not Found':
            logging.info(f'Snapshot does not exist at url: {url}, response code: {response.status_code}, '
                         f'reason: {response.reason}')
            return False
        else:
            raise AirflowException(f"Could not get head of url: {url}, response code: {response.status_code},"
                                   f"reason: {response.reason}")

    def download(self, releases: List[CrossrefMetadataRelease], **kwargs):
        """ Task to download the CrossrefMetadataRelease release for a given month.

        :param releases: the list of CrossrefMetadataRelease instances.
        :return: None.
        """

        # Download each release
        for release in releases:
            release.download()

    def upload_downloaded(self, releases: List[CrossrefMetadataRelease], **kwargs):
        """ Task to upload the downloaded CrossrefMetadataRelease release for a given month.

        :param releases: the list of CrossrefMetadataRelease instances.
        :return: None.
        """

        for release in releases:
            upload_files_from_list(release.download_files, release.download_bucket)

    def extract(self, releases: List[CrossrefMetadataRelease], **kwargs):
        """ Task to extract the CrossrefMetadataRelease release for a given month.

        :param releases: the list of CrossrefMetadataRelease instances.
        :return: None.
        """

        for release in releases:
            release.extract()

    def transform(self, releases: List[CrossrefMetadataRelease], **kwargs):
        """ Task to transform the CrossrefMetadataRelease release for a given month.

        :param releases: the list of CrossrefMetadataRelease instances.
        :return: None.
        """

        for release in releases:
            release.transform(max_workers=self.max_processes)

    def bq_load(self, releases: List[SnapshotRelease], **kwargs):
        """ Task to load each transformed release to BigQuery.

        The table_id is set to the file name without the extension.

        :param releases: a list of releases.
        :return: None.
        """

        # Load each transformed release
        for release in releases:
            transform_blob = f'{blob_name(release.transform_folder)}/*'
            table_description = self.table_descriptions.get(self.dag_id, '')
            bq_load_shard(release.release_date, transform_blob, self.dataset_id, self.dag_id, self.source_format,
                          prefix=self.schema_prefix, schema_version=self.schema_version,
                          dataset_description=self.dataset_description, table_description=table_description,
                          **self.load_bigquery_table_kwargs)


def transform_file(input_file_path: str, output_file_path: str):
    """ Transform a single crossref metadata json file.
    The json file is converted to a jsonl file and field names are transformed so they are accepted by BigQuery.

    :param input_file_path: the path of the file to transform.
    :param output_file_path: where to save the transformed file.
    :return: None.
    """

    cmd = 'mawk \'BEGIN {FS="\\":";RS=",\\"";OFS=FS;ORS=RS} {for (i=1; i<=NF;i++) if(i != NF) gsub("-", "_", $i)}1\'' \
          f' {input_file_path} | ' \
          'mawk \'!/^\}$|^\]$|,\\"$/{gsub("\[\[", "[");gsub("]]", "]");gsub(/,[ \\t]*$/,"");' \
          'gsub("\\"timestamp\\":_", "\\"timestamp\\":");gsub("\\"date_parts\\":\[null]", "\\"date_parts\\":[]");' \
          'gsub(/^\{\\"items\\":\[/,"");print}\' > ' \
          f'{output_file_path}'
    p: Popen = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, executable='/bin/bash')
    stdout, stderr = wait_for_process(p)
    if p.returncode != 0:
        raise AirflowException(f"transform unsuccessful for file: {input_file_path}. "
                               f"stdout: {stdout}, stderr: {stderr}")
