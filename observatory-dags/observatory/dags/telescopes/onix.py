# Copyright 2021 Curtin University
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

# Author: James Diprose

import logging
import os
import re
import shutil
import subprocess
from datetime import datetime
from typing import Optional, Dict, List

import pendulum
from airflow.exceptions import AirflowException
from airflow.models.taskinstance import TaskInstance
from google.cloud.bigquery import SourceFormat
from pendulum import Pendulum

from observatory.api.client.model.organisation import Organisation
from observatory.platform.telescopes.snapshot_telescope import SnapshotRelease, SnapshotTelescope
from observatory.platform.utils.airflow_utils import AirflowVars, AirflowConns
from observatory.platform.utils.config_utils import observatory_home
from observatory.platform.utils.data_utils import get_file
from observatory.platform.utils.proc_utils import wait_for_process
from observatory.platform.utils.telescope_utils import make_dag_id, make_sftp_connection
from observatory.platform.utils.template_utils import upload_files_from_list


class OnixRelease(SnapshotRelease):
    RELEASE_INFO = 'releases'
    DOWNLOAD_FILES_REGEX = r"\d{8}[_A-Z]+\.xml"
    TRANSFORM_FILES_REGEX = "onix.jsonl"
    ONIX_PARSER_NAME = 'coki-onix-parser.jar'
    ONIX_PARSER_URL = 'https://github.com/The-Academic-Observatory/onix-parser/releases/download/v1.1/coki-onix-parser-1.1-SNAPSHOT-shaded.jar'
    ONIX_PARSER_MD5 = 'e752ffdeb015e7a269cad6a6f9abdfed'

    def __init__(self, dag_id: str, release_date: Pendulum, file_name: str, organisation: Organisation):
        """ Construct an OnixRelease.

        :param release_date: the release date.
        :param file_name: the ONIX file name.
        :param organisation: the Organisation.
        """

        super().__init__(dag_id, release_date, self.DOWNLOAD_FILES_REGEX, None, self.TRANSFORM_FILES_REGEX)
        self.organisation = organisation
        self.file_name = file_name

    @property
    def sftp_home(self):
        """ The organisation's SFTP home folder.

        :return: path to folder.
        """

        return sftp_home(self.organisation.name)

    @property
    def sftp_in_progress_folder(self):
        """ The organisation's SFTP in_progress folder.

        :return: path to folder.
        """

        return os.path.join(self.sftp_home, 'in_progress')

    @property
    def sftp_finished_folder(self):
        """ The organisation's SFTP finished folder.

        :return: path to folder.
        """

        return os.path.join(self.sftp_home, 'finished')

    @property
    def sftp_upload_file(self):
        """ The organisation's SFTP upload file.

        :return: path to folder.
        """

        return os.path.join(self.sftp_home, 'upload', self.file_name)

    @property
    def sftp_in_progress_file(self):
        """ The organisation's SFTP in_progress file.

        :return: path to folder.
        """

        return os.path.join(self.sftp_in_progress_folder, self.file_name)

    @property
    def sftp_finished_file(self):
        """ The organisation's SFTP finished file.

        :return: path to folder.
        """

        return os.path.join(self.sftp_finished_folder, self.file_name)

    @property
    def download_file(self) -> str:
        """ Get the path to the downloaded file.

        :return: the file path.
        """

        return os.path.join(self.download_folder, self.file_name)

    @property
    def download_bucket(self):
        """ The download bucket name.

        :return: the download bucket name.
        """
        return self.organisation.gcp_download_bucket

    @property
    def transform_bucket(self):
        """ The transform bucket name.

        :return: the transform bucket name.
        """
        return self.organisation.gcp_transform_bucket

    def move_files_to_in_progress(self):
        """ Move ONIX file to in-progress folder

        :return: None.
        """

        with make_sftp_connection() as sftp:
            sftp.makedirs(self.sftp_in_progress_folder)
            sftp.rename(self.sftp_upload_file, self.sftp_in_progress_file)

    def download(self):
        """ Downloads an individual ONIX release from the SFTP server.

        :return: None.
        """

        with make_sftp_connection() as sftp:
            sftp.get(self.sftp_in_progress_file, localpath=self.download_file)

    def transform(self):
        """ Transform ONIX release.

        :return: None.
        """

        # Download ONIX Parser
        bin_path = observatory_home('bin')
        get_file(self.ONIX_PARSER_NAME, self.ONIX_PARSER_URL,
                 cache_subdir='',
                 cache_dir=bin_path,
                 md5_hash=self.ONIX_PARSER_MD5)

        # Transform release
        cmd = f"java -jar {self.ONIX_PARSER_NAME} {self.download_folder} {self.transform_folder} " \
              f"{org_id(self.organisation.name)}"
        p = subprocess.Popen(cmd,
                             shell=True,
                             stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE,
                             executable='/bin/bash',
                             cwd=bin_path)
        stdout, stderr = wait_for_process(p)

        if stdout:
            logging.info(stdout)

        if p.returncode != 0:
            raise AirflowException(f"bash command failed `{cmd}`: {stderr}")

        # Rename file to onix.jsonl
        shutil.move(os.path.join(self.transform_folder, 'full.jsonl'),
                    os.path.join(self.transform_folder, self.TRANSFORM_FILES_REGEX))

    def move_files_to_finished(self):
        """ Move ONIX file to finished folder

        :return: None.
        """

        with make_sftp_connection() as sftp:
            sftp.makedirs(self.sftp_finished_folder)
            sftp.rename(self.sftp_in_progress_file, self.sftp_finished_file)


def org_id(organisation_name: str):
    return organisation_name.strip().replace(' ', '_').lower()


def sftp_home(organisation_name: str) -> str:
    """ Make the SFTP home folder for an organisation.

    :param organisation_name: the organisation name.
    :return: the path to the folder.
    """

    return os.path.join('/telescopes', 'onix', org_id(organisation_name))


def list_release_info(sftp_upload_folder: str) -> List[Dict]:
    """ List the ONIX release info, a release date and a file name for each release.

    :param sftp_upload_folder: the SFTP upload folder.
    :return: the release information.
    """

    results = []
    sftp = make_sftp_connection()
    sftp.makedirs(sftp_upload_folder)
    files = sftp.listdir(sftp_upload_folder)
    for file_name in files:
        if re.match(OnixRelease.DOWNLOAD_FILES_REGEX, file_name):
            date_str = file_name[:8]
            release_date = pendulum.strptime(date_str, '%Y%m%d')
            results.append({
                'release_date': release_date,
                'file_name': file_name
            })
    sftp.close()
    return results


class OnixTelescope(SnapshotTelescope):
    DAG_ID_PREFIX = 'onix'

    def __init__(self, organisation: Organisation, dag_id: Optional[str] = None,
                 start_date: datetime = datetime(2021, 3, 28), schedule_interval: str = '@weekly',
                 dataset_id: str = 'onix', source_format: str = SourceFormat.NEWLINE_DELIMITED_JSON,
                 catchup: bool = False, airflow_vars: List = None, airflow_conns: List = None):
        """ Construct an OnixTelescope instance.

        :param organisation: the Organisation the ONIX DAG will process.
        :param dag_id: the id of the DAG, by default this is automatically generated based on the DAG_ID_PREFIX
        and the organisation name.
        :param start_date: the start date of the DAG.
        :param schedule_interval: the schedule interval of the DAG.
        :param dataset_id: the BigQuery dataset id.
        :param source_format: the format of the data to load into BigQuery.
        :param catchup: whether to catchup the DAG or not.
        :param airflow_vars: list of airflow variable keys, for each variable it is checked if it exists in airflow
        """

        if airflow_vars is None:
            airflow_vars = [AirflowVars.DATA_PATH, AirflowVars.PROJECT_ID, AirflowVars.DATA_LOCATION,
                            AirflowVars.DOWNLOAD_BUCKET, AirflowVars.TRANSFORM_BUCKET]

        if airflow_conns is None:
            airflow_conns = [AirflowConns.SFTP_SERVICE]

        if dag_id is None:
            dag_id = make_dag_id(self.DAG_ID_PREFIX, organisation.name)

        dataset_description = f'{organisation.name} ONIX feeds'

        super().__init__(dag_id, start_date, schedule_interval, dataset_id,
                         source_format=source_format,
                         dataset_description=dataset_description,
                         catchup=catchup,
                         airflow_vars=airflow_vars,
                         airflow_conns=airflow_conns)

        self.organisation = organisation
        self.add_setup_task(self.check_dependencies)
        self.add_setup_task(self.list_release_info)
        self.add_task(self.move_files_to_in_progress)
        self.add_task(self.download)
        self.add_task(self.upload_downloaded)
        self.add_task(self.transform)
        self.add_task(self.upload_transformed)
        self.add_task(self.bq_load)
        self.add_task(self.move_files_to_finished)
        self.add_task(self.cleanup)

    def list_release_info(self, **kwargs):
        """ Lists all ONIX releases and publishes their file names as an XCom.

        :param kwargs: the context passed from the BranchPythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: the identifier of the task to execute next.
        """

        # List release dates
        sftp_upload_folder = os.path.join(sftp_home(self.organisation.name), 'upload')
        release_info = list_release_info(sftp_upload_folder)

        # Publish XCom
        continue_dag = len(release_info)
        if continue_dag:
            ti: TaskInstance = kwargs['ti']
            ti.xcom_push(OnixTelescope.RELEASE_INFO, release_info, kwargs['execution_date'])

        return continue_dag

    def make_release(self, **kwargs) -> List[OnixRelease]:
        """ Make release instances. The release is passed as an argument to the function (TelescopeFunction) that is
        called in 'task_callable'.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are
        passed to this argument.
        :return: a list of GeonamesRelease instances.
        """

        ti: TaskInstance = kwargs['ti']
        records = ti.xcom_pull(key=OnixRelease.RELEASE_INFO,
                               task_ids=self.list_release_info.__name__,
                               include_prior_dates=False)
        releases = []
        for record in records:
            release_date = record['release_date']
            file_name = record['file_name']
            releases.append(OnixRelease(self.dag_id, release_date, file_name, self.organisation))
        return releases

    def move_files_to_in_progress(self, releases: List[OnixRelease], **kwargs):
        """ Move ONIX files to SFTP in-progress folder.

        :param releases: a list of ONIX releases.
        :return: None.
        """

        for release in releases:
            release.move_files_to_in_progress()

    def download(self, releases: List[OnixRelease], **kwargs):
        """ Task to download the ONIX releases.

        :param releases: a list of ONIX releases.
        :return: None.
        """

        for release in releases:
            release.download()

    def upload_downloaded(self, releases: List[OnixRelease], **kwargs):
        """ Task to upload the downloaded ONIX releases.

        :param releases: a list of ONIX releases.
        :return: None.
        """

        for release in releases:
            upload_files_from_list(release.download_files, release.download_bucket)

    def transform(self, releases: List[OnixRelease], **kwargs):
        """ Task to transform the ONIX releases.

        :param releases: a list of ONIX releases.
        :return: None.
        """

        # Transform each release
        for release in releases:
            release.transform()

    def move_files_to_finished(self, releases: List[OnixRelease], **kwargs):
        """ Move ONIX files to SFTP finished folder.

        :param releases: a list of ONIX releases.
        :return: None.
        """

        for release in releases:
            release.move_files_to_finished()
