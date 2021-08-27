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
from typing import Dict, List, Optional

import pendulum
from airflow.exceptions import AirflowException
from airflow.models.taskinstance import TaskInstance
from google.cloud.bigquery import SourceFormat

from oaebu_workflows.config import schema_folder as default_schema_folder
from observatory.platform.utils.airflow_utils import AirflowConns, AirflowVars
from observatory.platform.utils.config_utils import observatory_home
from observatory.platform.utils.data_utils import get_file
from observatory.platform.utils.proc_utils import wait_for_process
from observatory.platform.utils.workflow_utils import (
    SftpFolders,
    make_dag_id,
    make_org_id,
    make_sftp_connection,
)
from observatory.platform.utils.workflow_utils import (
    blob_name,
    bq_load_shard_v2,
    table_ids_from_path,
    upload_files_from_list,
)
from observatory.platform.workflows.snapshot_telescope import (
    SnapshotRelease,
    SnapshotTelescope,
)


class OnixRelease(SnapshotRelease):
    DOWNLOAD_FILES_REGEX = r".*\.xml"
    TRANSFORM_FILES_REGEX = "onix.jsonl"
    ONIX_PARSER_NAME = "coki-onix-parser.jar"
    ONIX_PARSER_URL = "https://github.com/The-Academic-Observatory/onix-parser/releases/download/v1.1/coki-onix-parser-1.1-SNAPSHOT-shaded.jar"
    ONIX_PARSER_MD5 = "e752ffdeb015e7a269cad6a6f9abdfed"

    def __init__(
        self,
        *,
        dag_id: str,
        release_date: pendulum.DateTime,
        file_name: str,
        organisation_name: str,
        download_bucket: str,
        transform_bucket: str,
    ):
        """Construct an OnixRelease.

        :param dag_id: the DAG id.
        :param release_date: the release date.
        :param file_name: the ONIX file name.
        :param organisation_name: the organisation name.
        :param download_bucket: the download bucket name.
        :param transform_bucket: the transform bucket name.
        """

        self.organisation_name = organisation_name
        self._download_bucket = download_bucket
        self._transform_bucket = transform_bucket

        super().__init__(dag_id, release_date, self.DOWNLOAD_FILES_REGEX, None, self.TRANSFORM_FILES_REGEX)
        self.organisation_name = organisation_name
        self.file_name = file_name
        self.sftp_folders = SftpFolders(dag_id, organisation_name)

    @property
    def download_file(self) -> str:
        """Get the path to the downloaded file.

        :return: the file path.
        """

        return os.path.join(self.download_folder, self.file_name)

    @property
    def download_bucket(self):
        """The download bucket name.

        :return: the download bucket name.
        """
        return self._download_bucket

    @property
    def transform_bucket(self):
        """The transform bucket name.

        :return: the transform bucket name.
        """
        return self._transform_bucket

    def move_files_to_in_progress(self):
        """Move ONIX file to in-progress folder
        :return: None.
        """

        self.sftp_folders.move_files_to_in_progress(self.file_name)

    def download(self):
        """Downloads an individual ONIX release from the SFTP server.

        :return: None.
        """

        with make_sftp_connection() as sftp:
            in_progress_file = os.path.join(self.sftp_folders.in_progress, self.file_name)
            sftp.get(in_progress_file, localpath=self.download_file)

    def transform(self):
        """Transform ONIX release.

        :return: None.
        """

        # Download ONIX Parser
        bin_path = observatory_home("bin")
        get_file(
            self.ONIX_PARSER_NAME,
            self.ONIX_PARSER_URL,
            cache_subdir="",
            cache_dir=bin_path,
            md5_hash=self.ONIX_PARSER_MD5,
        )

        # Transform release
        cmd = (
            f"java -jar {self.ONIX_PARSER_NAME} {self.download_folder} {self.transform_folder} "
            f"{make_org_id(self.organisation_name)}"
        )
        p = subprocess.Popen(
            cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, executable="/bin/bash", cwd=bin_path
        )
        stdout, stderr = wait_for_process(p)

        if stdout:
            logging.info(stdout)

        if p.returncode != 0:
            raise AirflowException(f"bash command failed `{cmd}`: {stderr}")

        # Rename file to onix.jsonl
        shutil.move(
            os.path.join(self.transform_folder, "full.jsonl"),
            os.path.join(self.transform_folder, self.TRANSFORM_FILES_REGEX),
        )

    def move_files_to_finished(self):
        """Move ONIX file to finished folder
        :return: None.
        """

        self.sftp_folders.move_files_to_finished(self.file_name)


def list_release_info(*, sftp_upload_folder: str, date_regex: str, date_format: str,) -> List[Dict]:
    """List the ONIX release info, a release date and a file name for each release.

    :param sftp_upload_folder: the SFTP upload folder.
    :param date_regex: the regex for extracting the date from the filename.
    :param date_format: the strptime date format string for converting the date into a pendulum datetime object.
    :return: the release information.
    """

    results = []
    with make_sftp_connection() as sftp:
        sftp.makedirs(sftp_upload_folder)
        files = sftp.listdir(sftp_upload_folder)
        for file_name in files:
            if re.match(OnixRelease.DOWNLOAD_FILES_REGEX, file_name):
                try:
                    date_str = re.search(date_regex, file_name).group(0)
                except AttributeError:
                    msg = f"Could not find date with pattern `{date_regex}` in file name {file_name}"
                    logging.error(msg)
                    raise AirflowException(msg)
                results.append({"release_date": date_str, "file_name": file_name})
    return results


class OnixTelescope(SnapshotTelescope):
    DAG_ID_PREFIX = "onix"

    def __init__(
        self,
        *,
        organisation_name: str,
        project_id: str,
        download_bucket: str,
        transform_bucket: str,
        dataset_location: str,
        date_regex: str,
        date_format: str,
        dag_id: Optional[str] = None,
        start_date: pendulum.DateTime = pendulum.datetime(2021, 3, 28),
        schedule_interval: str = "@weekly",
        dataset_id: str = "onix",
        schema_folder: str = default_schema_folder(),
        source_format: str = SourceFormat.NEWLINE_DELIMITED_JSON,
        catchup: bool = False,
        airflow_vars: List = None,
        airflow_conns: List = None,
    ):
        """Construct an OnixTelescope instance.

        :param organisation_name: the organisation name.
        :param project_id: the Google Cloud project id.
        :param download_bucket: the Google Cloud download bucket.
        :param transform_bucket: the Google Cloud transform bucket.
        :param dataset_location: the location for the BigQuery dataset.
        :param date_regex: a regular expression for extracting a date string from an ONIX file name.
        :param date_format: the Python strptime date format string for transforming the string extracted with
        `date_regex` into a date object.
        :param dag_id: the id of the DAG, by default this is automatically generated based on the DAG_ID_PREFIX
        and the organisation name.
        :param start_date: the start date of the DAG.
        :param schedule_interval: the schedule interval of the DAG.
        :param dataset_id: the BigQuery dataset id.
        :param schema_folder: the SQL schema path.
        :param source_format: the format of the data to load into BigQuery.
        :param catchup: whether to catchup the DAG or not.
        :param airflow_vars: list of airflow variable keys, for each variable, it is checked if it exists in airflow.
        :param airflow_conns: list of airflow connection keys, for each connection, it is checked if it exists in airflow.
        """

        if airflow_vars is None:
            airflow_vars = [
                AirflowVars.DATA_PATH,
                AirflowVars.PROJECT_ID,
                AirflowVars.DATA_LOCATION,
                AirflowVars.DOWNLOAD_BUCKET,
                AirflowVars.TRANSFORM_BUCKET,
            ]

        if airflow_conns is None:
            airflow_conns = [AirflowConns.SFTP_SERVICE]

        if dag_id is None:
            dag_id = make_dag_id(self.DAG_ID_PREFIX, organisation_name)

        dataset_description = f"{organisation_name} ONIX feeds"
        self.organisation_name = organisation_name
        self.project_id = project_id
        self.download_bucket = download_bucket
        self.transform_bucket = transform_bucket
        self.dataset_location = dataset_location
        self.date_regex = date_regex
        self.date_format = date_format

        super().__init__(
            dag_id,
            start_date,
            schedule_interval,
            dataset_id,
            schema_folder,
            source_format=source_format,
            dataset_description=dataset_description,
            catchup=catchup,
            airflow_vars=airflow_vars,
            airflow_conns=airflow_conns,
        )

        # self.organisation = organisation
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
        """Lists all ONIX releases and publishes their file names as an XCom.

        :param kwargs: the context passed from the BranchPythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: the identifier of the task to execute next.
        """

        # List release dates
        sftp_upload_folder = SftpFolders(self.dag_id, self.organisation_name).upload
        release_info = list_release_info(
            sftp_upload_folder=sftp_upload_folder, date_regex=self.date_regex, date_format=self.date_format
        )

        # Publish XCom
        continue_dag = len(release_info)
        if continue_dag:
            ti: TaskInstance = kwargs["ti"]
            execution_date = kwargs["execution_date"]
            ti.xcom_push(OnixTelescope.RELEASE_INFO, release_info, execution_date)

        return continue_dag

    def make_release(self, **kwargs) -> List[OnixRelease]:
        """Make release instances. The release is passed as an argument to the function (TelescopeFunction) that is
        called in 'task_callable'.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are
        passed to this argument.
        :return: a list of GeonamesRelease instances.
        """

        ti: TaskInstance = kwargs["ti"]
        records = ti.xcom_pull(
            key=OnixTelescope.RELEASE_INFO, task_ids=self.list_release_info.__name__, include_prior_dates=False
        )
        releases = []
        for record in records:
            release_date = pendulum.parse(record["release_date"])
            file_name = record["file_name"]
            releases.append(
                OnixRelease(
                    dag_id=self.dag_id,
                    release_date=release_date,
                    file_name=file_name,
                    organisation_name=self.organisation_name,
                    download_bucket=self.download_bucket,
                    transform_bucket=self.transform_bucket,
                )
            )
        return releases

    def move_files_to_in_progress(self, releases: List[OnixRelease], **kwargs):
        """Move ONIX files to SFTP in-progress folder.

        :param releases: a list of ONIX releases.
        :return: None.
        """

        for release in releases:
            release.move_files_to_in_progress()

    def download(self, releases: List[OnixRelease], **kwargs):
        """Task to download the ONIX releases.

        :param releases: a list of ONIX releases.
        :return: None.
        """

        for release in releases:
            release.download()

    def upload_downloaded(self, releases: List[OnixRelease], **kwargs):
        """Task to upload the downloaded ONIX releases.

        :param releases: a list of ONIX releases.
        :return: None.
        """

        for release in releases:
            print(f"release download files: {release.download_files}, download bucket: {release.download_bucket}")
            upload_files_from_list(release.download_files, release.download_bucket)

    def transform(self, releases: List[OnixRelease], **kwargs):
        """Task to transform the ONIX releases.

        :param releases: a list of ONIX releases.
        :return: None.
        """

        # Transform each release
        for release in releases:
            release.transform()

    def bq_load(self, releases: List[SnapshotRelease], **kwargs):
        """Task to load each transformed release to BigQuery.

        The table_id is set to the file name without the extension.

        :param releases: a list of releases.
        :return: None.
        """

        # Load each transformed release
        for release in releases:
            for transform_path in release.transform_files:
                transform_blob = blob_name(transform_path)
                table_id, _ = table_ids_from_path(transform_path)

                bq_load_shard_v2(
                    self.schema_folder,
                    self.project_id,
                    self.transform_bucket,
                    transform_blob,
                    self.dataset_id,
                    self.dataset_location,
                    table_id,
                    release.release_date,
                    self.source_format,
                    prefix=self.schema_prefix,
                    schema_version=self.schema_version,
                    dataset_description=self.dataset_description,
                    **self.load_bigquery_table_kwargs,
                )

    def move_files_to_finished(self, releases: List[OnixRelease], **kwargs):
        """Move ONIX files to SFTP finished folder.

        :param releases: a list of ONIX releases.
        :return: None.
        """

        for release in releases:
            release.move_files_to_finished()
