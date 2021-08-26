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

# Author: James Diprose

import glob
import json
import logging
import os
import shutil
import subprocess
from dataclasses import dataclass
from typing import List

import pendulum
from airflow.exceptions import AirflowException
from airflow.models.taskinstance import TaskInstance
from google.cloud.bigquery import SourceFormat

from academic_observatory_workflows.config import schema_folder
from observatory.platform.utils.airflow_utils import AirflowVariable as Variable
from observatory.platform.utils.airflow_utils import AirflowVars, check_variables
from observatory.platform.utils.config_utils import find_schema
from observatory.platform.utils.data_utils import get_file
from observatory.platform.utils.gc_utils import (
    bigquery_sharded_table_id,
    bigquery_table_exists,
    create_bigquery_dataset,
    load_bigquery_table,
    upload_files_to_cloud_storage,
)
from observatory.platform.utils.proc_utils import wait_for_process
from observatory.platform.utils.url_utils import retry_session
from observatory.platform.utils.workflow_utils import SubFolder, workflow_path

OPEN_CITATIONS_ARTICLE_ID = 6741422
OPEN_CITATIONS_VERSION_URL = "https://api.figshare.com/v2/articles/{article_id}/versions"


@dataclass
class File:
    name: str
    download_url: str
    md5hash: str
    parent: "OpenCitationsRelease" = None

    @property
    def download_blob_name(self):
        return f"telescopes/{OpenCitationsTelescope.DAG_ID}/{self.parent.release_name}/{self.name}"


class OpenCitationsRelease:
    release_date: pendulum.DateTime
    files: List[File]

    def __init__(self, release_date: pendulum.DateTime, files: List[File]):
        self.release_date = release_date
        self.files = files

        # Add parent release to files
        for file in self.files:
            file.parent = self

    @property
    def release_name(self) -> str:
        return f'{OpenCitationsTelescope.DAG_ID}_{self.release_date.strftime("%Y_%m_%d")}'

    @property
    def download_path(self) -> str:
        return os.path.join(workflow_path(SubFolder.downloaded, OpenCitationsTelescope.DAG_ID), self.release_name)

    @property
    def extract_path(self) -> str:
        return os.path.join(workflow_path(SubFolder.extracted, OpenCitationsTelescope.DAG_ID), self.release_name)

    @property
    def transformed_blob_path(self) -> str:
        return f"telescopes/{OpenCitationsTelescope.DAG_ID}/{self.release_name}/*.csv"


def list_open_citations_releases(
    start_date: pendulum.DateTime = None, end_date: pendulum.DateTime = None, timeout: float = 30.0
) -> List[OpenCitationsRelease]:
    """List the Open Citations releases for a given time period.

    :param start_date: the start date.
    :param end_date: the end date.
    :param timeout: timeout in seconds.
    :return: a list of Open Citations releases found within the given time period (if the time period was specified).
    """

    versions = fetch_open_citations_versions()
    releases = []
    for version in versions:
        version_url = version["url"]
        response = retry_session().get(version_url, timeout=timeout, headers={"Accept-encoding": "gzip"})
        article = json.loads(response.text)
        release_date = pendulum.parse(article["created_date"])

        if (start_date is None or start_date <= release_date) and (end_date is None or release_date <= end_date):
            files = []
            for file in article["files"]:
                name = file["name"]
                download_url = file["download_url"]
                md5hash = file["computed_md5"]
                files.append(File(name, download_url, md5hash))
            releases.append(OpenCitationsRelease(release_date, files))
    return releases


def fetch_open_citations_versions():
    """Fetch the Open Citation versions that are available.

    :return: the open citations versions.
    """

    url = OPEN_CITATIONS_VERSION_URL.format(article_id=OPEN_CITATIONS_ARTICLE_ID)
    response = retry_session().get(url, timeout=30, headers={"Accept-encoding": "gzip"})
    return json.loads(response.text)


def pull_releases(ti: TaskInstance) -> List[OpenCitationsRelease]:
    return ti.xcom_pull(
        key=OpenCitationsTelescope.RELEASES_TOPIC_NAME,
        task_ids=OpenCitationsTelescope.TASK_ID_LIST_RELEASES,
        include_prior_dates=False,
    )


class OpenCitationsTelescope:
    """A container for holding the constants and static functions for the Open Citations telescope."""

    DAG_ID = "open_citations"
    DESCRIPTION = "The OpenCitations Indexes: http://opencitations.net/"
    DATASET_ID = DAG_ID
    QUEUE = "remote_queue"
    RETRIES = 3
    RELEASES_TOPIC_NAME = "releases"

    TASK_ID_CHECK_DEPENDENCIES = "check_dependencies"
    TASK_ID_LIST_RELEASES = f"list_releases"
    TASK_ID_DOWNLOAD = f"download"
    TASK_ID_UPLOAD_DOWNLOADED = f"upload_downloaded"
    TASK_ID_EXTRACT = f"extract"
    TASK_ID_UPLOAD_EXTRACTED = f"upload_extracted"
    TASK_ID_BQ_LOAD = f"bq_load"
    TASK_ID_CLEANUP = f"cleanup"

    @staticmethod
    def check_dependencies(**kwargs):
        """Check that all variables exist that are required to run the DAG.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        """

        vars_valid = check_variables(
            AirflowVars.DATA_PATH,
            AirflowVars.PROJECT_ID,
            AirflowVars.DATA_LOCATION,
            AirflowVars.DOWNLOAD_BUCKET,
            AirflowVars.TRANSFORM_BUCKET,
        )
        if not vars_valid:
            raise AirflowException("Required variables are missing")

    @staticmethod
    def list_releases(**kwargs):
        """Task to lists all Open Citations releases for a given time period.

        :param kwargs: the context passed from the BranchPythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: the identifier of the task to execute next.
        """

        start_date = kwargs["execution_date"]
        end_date = kwargs["next_execution_date"].subtract(microseconds=1)
        releases = list_open_citations_releases(start_date=start_date, end_date=end_date)
        project_id = Variable.get(AirflowVars.PROJECT_ID)

        # Check if we can skip any releases
        releases_out = []
        for release in releases:
            table_id = bigquery_sharded_table_id(OpenCitationsTelescope.DAG_ID, release.release_date)

            if bigquery_table_exists(project_id, OpenCitationsTelescope.DATASET_ID, table_id):
                logging.info(
                    f"Skipping as table exists for {release.release_name} release: "
                    f"{project_id}.{OpenCitationsTelescope.DATASET_ID}.{table_id}"
                )
            else:
                logging.info(
                    f"Table doesn't exist yet, processing Open Citations {release.release_date} "
                    f"release in this workflow"
                )
                releases_out.append(release)

        continue_dag = len(releases_out)
        if continue_dag:
            # Push messages
            ti: TaskInstance = kwargs["ti"]
            ti.xcom_push(OpenCitationsTelescope.RELEASES_TOPIC_NAME, releases_out, start_date)
        return continue_dag

    @staticmethod
    def download(**kwargs):
        """Task to download the Open Citations releases for a given time period.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        # Pull messages
        ti: TaskInstance = kwargs["ti"]
        releases = pull_releases(ti)

        # Download and extract each release posted this month
        for release in releases:
            os.makedirs(release.download_path, exist_ok=True)
            for file in release.files:
                file_path, updated = get_file(
                    file.name,
                    file.download_url,
                    cache_subdir="",
                    cache_dir=release.download_path,
                    md5_hash=file.md5hash,
                )

    @staticmethod
    def upload_downloaded(**kwargs):
        """Task to upload the downloaded Open Citations releases for a given month.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        # Get bucket name
        bucket_name = Variable.get(AirflowVars.DOWNLOAD_BUCKET)

        # Pull messages
        ti: TaskInstance = kwargs["ti"]
        releases = pull_releases(ti)

        # Upload each release
        file_paths = []
        blob_names = []
        for release in releases:
            for file in release.files:
                file_path = os.path.join(release.download_path, file.name)
                blob_name = file.download_blob_name
                file_paths.append(file_path)
                blob_names.append(blob_name)

        success = upload_files_to_cloud_storage(bucket_name, blob_names, file_paths)
        if not success:
            raise AirflowException("Problem uploading files")

    @staticmethod
    def extract(**kwargs):
        """Task to extract the downloaded Open Citations releases for a given month.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        # Pull messages
        ti: TaskInstance = kwargs["ti"]
        releases = pull_releases(ti)

        for release in releases:
            os.makedirs(release.extract_path, exist_ok=True)

            for file in release.files:
                file_path = os.path.join(release.download_path, file.name)
                cmd = f"unzip {file_path} -d {release.extract_path}"
                logging.info(f"Running command: {cmd}")
                p = subprocess.Popen(
                    cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, executable="/bin/bash"
                )
                stdout, stderr = wait_for_process(p)

                if stdout:
                    logging.info(stdout)

                if stderr:
                    raise AirflowException(f"bash command failed for {file_path}, {release.release_date}: {stderr}")

                logging.info(f"File extracted to: {release.extract_path}")

    @staticmethod
    def upload_extracted(**kwargs):
        """Task to upload the extracted Open Citations releases for a given month.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        # Get bucket name
        bucket_name = Variable.get(AirflowVars.TRANSFORM_BUCKET)

        # Pull messages
        ti: TaskInstance = kwargs["ti"]
        releases = pull_releases(ti)

        # Upload each release
        file_paths = []
        blob_names = []
        for release in releases:
            release_file_paths = glob.glob(f"{release.extract_path}/*.csv")
            for file_path in release_file_paths:
                file_name = os.path.basename(file_path)
                blob_name = f"telescopes/{OpenCitationsTelescope.DAG_ID}/{release.release_name}/{file_name}"
                file_paths.append(file_path)
                blob_names.append(blob_name)

        logging.info(f"file_paths: {file_paths}")
        logging.info(f"blob_names: {blob_names}")

        success = upload_files_to_cloud_storage(bucket_name, blob_names, file_paths)
        if not success:
            raise AirflowException("Problem uploading files")

    @staticmethod
    def load_to_bq(**kwargs):
        """Load transformed Open Citations release into BigQuery.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        # Pull messages
        ti: TaskInstance = kwargs["ti"]
        releases = pull_releases(ti)

        # Get variables
        project_id = Variable.get(AirflowVars.PROJECT_ID)
        data_location = Variable.get(AirflowVars.DATA_LOCATION)
        bucket_name = Variable.get(AirflowVars.TRANSFORM_BUCKET)

        # Create dataset
        dataset_id = OpenCitationsTelescope.DATASET_ID
        create_bigquery_dataset(project_id, dataset_id, data_location, OpenCitationsTelescope.DESCRIPTION)
        table_name = OpenCitationsTelescope.DAG_ID

        # Load each release
        for release in releases:
            table_id = bigquery_sharded_table_id(table_name, release.release_date)

            # Select schema file based on release date
            analysis_schema_path = schema_folder()
            schema_file_path = find_schema(analysis_schema_path, table_name, release.release_date)
            if schema_file_path is None:
                logging.error(
                    f"No schema found with search parameters: analysis_schema_path={analysis_schema_path}, "
                    f"table_name={table_name}, release_date={release.release_date}"
                )
                exit(os.EX_CONFIG)

            # Load BigQuery table
            uri = f"gs://{bucket_name}/{release.transformed_blob_path}"
            logging.info(f"URI: {uri}")
            success = load_bigquery_table(
                uri,
                dataset_id,
                data_location,
                table_id,
                schema_file_path,
                SourceFormat.CSV,
                csv_field_delimiter=",",
                csv_quote_character='"',
                csv_skip_leading_rows=1,
                csv_allow_quoted_newlines=True,
            )
            if not success:
                raise AirflowException("bq_load task: data failed to load data into BigQuery")

    @staticmethod
    def cleanup(**kwargs):
        """Delete files of downloaded and extracted Open Citations releases.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        # Pull messages
        ti: TaskInstance = kwargs["ti"]
        releases = pull_releases(ti)

        for release in releases:
            logging.info(f"Removing downloaded files for release: {release.release_name}")
            download_path = release.download_path
            try:
                shutil.rmtree(download_path)
            except FileNotFoundError as e:
                logging.warning(f"No such file or directory {download_path}: {e}")

            logging.info(f"Removing extracted files for release: {release.release_name}")
            extract_path = release.extract_path
            try:
                shutil.rmtree(extract_path)
            except FileNotFoundError as e:
                logging.warning(f"No such file or directory {extract_path}: {e}")
