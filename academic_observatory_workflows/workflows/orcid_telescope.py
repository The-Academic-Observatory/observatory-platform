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

import gzip
import logging
import os
import shutil
import subprocess
import tarfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from io import BytesIO
from subprocess import Popen
from typing import List

import boto3
import jsonlines
import pendulum
import xmltodict
from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable
from airflow.models.taskinstance import TaskInstance

from observatory.dags.config import schema_folder as default_schema_folder
from observatory.platform.utils.airflow_utils import AirflowConns
from observatory.platform.utils.airflow_utils import AirflowVariable as Variable
from observatory.platform.utils.airflow_utils import AirflowVars
from observatory.platform.utils.gc_utils import (
    aws_to_google_cloud_storage_transfer,
    storage_bucket_exists,
)
from observatory.platform.utils.proc_utils import wait_for_process
from observatory.platform.workflows.stream_telescope import (
    StreamRelease,
    StreamTelescope,
)


class OrcidRelease(StreamRelease):
    def __init__(
        self,
        dag_id: str,
        start_date: pendulum.DateTime,
        end_date: pendulum.DateTime,
        first_release: bool,
        max_processes: int,
    ):
        """Construct an OrcidRelease instance

        :param dag_id: the id of the DAG.
        :param start_date: the start_date of the release.
        :param end_date: the end_date of the release.
        :param first_release: whether this is the first release that is processed for this DAG
        :param max_processes: Max processes used for parallel downloading
        """
        download_files_regex = r".*.xml$"
        transform_files_regex = r".*.jsonl.gz"
        super().__init__(
            dag_id,
            start_date,
            end_date,
            first_release,
            download_files_regex=download_files_regex,
            transform_files_regex=transform_files_regex,
        )
        self.max_processes = max_processes

    @property
    def modified_records_path(self) -> str:
        """Get the path to the file with ids of modified records.

        :return: the file path.
        """
        return os.path.join(self.download_folder, "modified_records.txt")

    def transfer(self, max_retries):
        """Sync files from AWS bucket to Google Cloud bucket

        :param max_retries: Number of max retries to try the transfer
        :return: None.
        """
        aws_access_key_id, aws_secret_access_key = get_aws_conn_info()

        gc_download_bucket = Variable.get(AirflowVars.ORCID_BUCKET)
        gc_project_id = Variable.get(AirflowVars.PROJECT_ID)
        last_modified_since = None if self.first_release else self.start_date

        success = False
        total_count = 0

        for i in range(max_retries):
            if success:
                break
            success, objects_count = aws_to_google_cloud_storage_transfer(
                aws_access_key_id,
                aws_secret_access_key,
                aws_bucket=OrcidTelescope.SUMMARIES_BUCKET,
                include_prefixes=[],
                gc_project_id=gc_project_id,
                gc_bucket=gc_download_bucket,
                description="Transfer ORCID data from " "airflow telescope",
                last_modified_since=last_modified_since,
            )
            total_count += objects_count

        if not success:
            raise AirflowException(f"Google Storage Transfer unsuccessful, status: {success}")

        logging.info(f"Total number of objects transferred: {total_count}")
        if total_count == 0:
            raise AirflowSkipException("No objects to transfer")

    def download_transferred(self):
        """Download the updated records from the Google Cloud bucket to a local directory using gsutil.
        If the run processes the first release it will download all files. If it is a later release, it will check
        the ORCID lambda file which tracks which records are modified. Only the modified records will be downloaded.

        :return: None.
        """
        aws_access_key_id, aws_secret_access_key = get_aws_conn_info()

        gc_download_bucket = Variable.get(AirflowVars.ORCID_BUCKET)

        # Authenticate gcloud with service account
        args = [
            "gcloud",
            "auth",
            "activate-service-account",
            f"--key-file" f"={os.environ['GOOGLE_APPLICATION_CREDENTIALS']}",
        ]
        proc: Popen = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        run_subprocess_cmd(proc, args)

        logging.info(f"Downloading transferred files from Google Cloud bucket: {gc_download_bucket}")
        log_path = os.path.join(self.download_folder, "cp.log")
        if self.first_release:
            # Download all records from bucket
            args = [
                "gsutil",
                "-m",
                "-q",
                "cp",
                "-L",
                log_path,
                "-r",
                f"gs://{gc_download_bucket}",
                self.download_folder,
            ]
            proc: Popen = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        else:
            # Download only modified records from bucket
            write_modified_record_blobs(
                self.start_date,
                self.end_date,
                aws_access_key_id,
                aws_secret_access_key,
                gc_download_bucket,
                self.modified_records_path,
            )
            args = ["gsutil", "-m", "-q", "cp", "-L", log_path, "-I", self.download_folder]
            proc: Popen = subprocess.Popen(
                args, stdin=open(self.modified_records_path), stdout=subprocess.PIPE, stderr=subprocess.PIPE
            )
        run_subprocess_cmd(proc, args)

    def transform(self):
        """Transform the ORCID records in parallel.
        Each file is 1 record, after the single file is transformed the data is appended to a .jsonl.gz file

        :return: None.
        """
        logging.info(f"Using {self.max_processes} workers for multithreading")
        count = 0
        with ThreadPoolExecutor(max_workers=self.max_processes) as executor:
            futures = [executor.submit(self.transform_single_file, file) for file in self.download_files]
            for future in as_completed(futures):
                future.result()
                count += 1
                if count % 1000 == 0:
                    logging.info(f"Transformed {count} files")

        # Loop through directories with individual files, concatenate files in each directory into 1 gzipped file.
        logging.info("Finished transforming individual files, concatenating & compressing files")
        for root, dirs, files in os.walk(self.transform_folder):
            if root == self.transform_folder:
                continue
            file_dir = os.path.basename(root)
            transform_path = os.path.join(self.transform_folder, file_dir + ".jsonl.gz")
            with gzip.GzipFile(transform_path, mode="wb") as f_out:
                for name in files:
                    with open(os.path.join(root, name), "rb") as f_in:
                        shutil.copyfileobj(f_in, f_out)

    def transform_single_file(self, download_path: str):
        """Transform a single ORCID file/record.
        The xml file is turned into a dictionary, a record should have either a valid 'record' section or an 'error'
        section. The keys of the dictionary are slightly changed so they are valid BigQuery fields.
        The dictionary is appended to a jsonl file

        :param download_path: The path to the file with the ORCID record.
        :return: None.
        """
        file_name = os.path.basename(download_path)
        file_dir = os.path.join(self.transform_folder, file_name[-7:-4])  # last three digits are used for subdir

        # Create subdirectory if it does not exist yet, even with if statement it will still raise FileExistsError
        # sometimes
        if not os.path.exists(file_dir):
            try:
                os.mkdir(file_dir)
            except FileExistsError:
                pass

        transform_path = os.path.join(file_dir, os.path.splitext(file_name)[0] + ".jsonl")
        # Skip if file already exists
        if os.path.exists(transform_path):
            return

        # Create dict of data from summary xml file
        with open(download_path, "r") as f:
            orcid_dict = xmltodict.parse(f.read())

        # Get record
        orcid_record = orcid_dict.get("record:record")

        # Some records do not have a 'record', but only 'error', this will be stored in the BQ table.
        if not orcid_record:
            orcid_record = orcid_dict.get("error:error")
        if not orcid_record:
            raise AirflowException(f"Key error for file: {download_path}")

        orcid_record = change_keys(orcid_record, convert)

        with jsonlines.open(transform_path, "w") as writer:
            writer.write(orcid_record)
        return


class OrcidTelescope(StreamTelescope):
    """ORCID telescope"""

    DAG_ID = "orcid"

    SUMMARIES_BUCKET = "v2.0-summaries"
    LAMBDA_BUCKET = "orcid-lambda-file"
    LAMBDA_OBJECT = "last_modified.csv.tar"
    S3_HOST = "s3.eu-west-1.amazonaws.com"

    def __init__(
        self,
        dag_id: str = DAG_ID,
        start_date: pendulum.DateTime = pendulum.datetime(2018, 5, 14),
        schedule_interval: str = "@weekly",
        dataset_id: str = "orcid",
        dataset_description: str = "",
        table_descriptions: dict = None,
        merge_partition_field: str = "orcid_identifier.uri",
        bq_merge_days: int = 7,
        schema_folder: str = default_schema_folder(),
        batch_load: bool = True,
        airflow_vars: List = None,
        airflow_conns: List = None,
        max_processes: int = min(32, os.cpu_count() + 4),
    ):
        """Construct an OrcidTelescope instance.

        :param dag_id: the id of the DAG.
        :param start_date: the start date of the DAG.
        :param schedule_interval: the schedule interval of the DAG.
        :param dataset_id: the dataset id.
        :param dataset_description: the dataset description.
        :param table_descriptions: a dictionary with table ids and corresponding table descriptions.
        :param merge_partition_field: the BigQuery field used to match partitions for a merge
        :param bq_merge_days: how often partitions should be merged (every x days)
        :param schema_folder: the SQL schema path.
        :param batch_load: whether all files in the transform folder are loaded into 1 table at once
        :param airflow_vars: list of airflow variable keys, for each variable it is checked if it exists in airflow
        :param airflow_conns: list of airflow connection keys, for each connection it is checked if it exists in airflow
        :param max_processes: Max processes used for parallel downloading
        """
        if table_descriptions is None:
            table_descriptions = {
                dag_id: "The ORCID (Open Researcher and Contributor ID) is a nonproprietary "
                "alphanumeric code to uniquely identify authors and contributors of "
                "scholarly communication, see: https://orcid.org/."
            }

        if airflow_vars is None:
            airflow_vars = [
                AirflowVars.DATA_PATH,
                AirflowVars.PROJECT_ID,
                AirflowVars.DATA_LOCATION,
                AirflowVars.DOWNLOAD_BUCKET,
                AirflowVars.TRANSFORM_BUCKET,
                AirflowVars.ORCID_BUCKET,
            ]
        if airflow_conns is None:
            airflow_conns = [AirflowConns.ORCID]
        super().__init__(
            dag_id,
            start_date,
            schedule_interval,
            dataset_id,
            merge_partition_field,
            bq_merge_days,
            schema_folder,
            dataset_description=dataset_description,
            table_descriptions=table_descriptions,
            airflow_vars=airflow_vars,
            airflow_conns=airflow_conns,
            batch_load=batch_load,
        )
        self.max_processes = max_processes

        self.add_setup_task_chain([self.check_dependencies, self.get_release_info])
        self.add_task_chain(
            [self.transfer, self.download_transferred, self.transform, self.upload_transformed, self.bq_load_partition]
        )
        self.add_task_chain([self.bq_delete_old, self.bq_append_new, self.cleanup], trigger_rule="none_failed")

    def make_release(self, **kwargs) -> OrcidRelease:
        """Make a release instance. The release is passed as an argument to the function (TelescopeFunction) that is
        called in 'task_callable'.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are
        passed to this argument.
        :return: an OrcidRelease instance.
        """
        ti: TaskInstance = kwargs["ti"]
        start_date, end_date, first_release = ti.xcom_pull(key=OrcidTelescope.RELEASE_INFO, include_prior_dates=True)

        release = OrcidRelease(
            self.dag_id, pendulum.parse(start_date), pendulum.parse(end_date), first_release, self.max_processes
        )
        return release

    def check_dependencies(self, **kwargs) -> bool:
        """Check dependencies of DAG. Add to parent method to additionally check whether the Google Cloud bucket
        that is used to sync ORCID data exists.

        :return: True if dependencies are valid.
        """
        super().check_dependencies()

        orcid_bucket_name = Variable.get(AirflowVars.ORCID_BUCKET)
        if not storage_bucket_exists(orcid_bucket_name):
            raise AirflowException(f"Bucket to store ORCID download data does not exist ({orcid_bucket_name})")
        return True

    def transfer(self, release: OrcidRelease, **kwargs):
        """Task to transfer data of the ORCID release.

        :param release: an OrcidRelease instance.
        :return: None.
        """
        release.transfer(self.max_retries)

    def download_transferred(self, release: OrcidRelease, **kwargs):
        """Task to download the transferred data of the ORCID release.

        :param release: an OrcidRelease instance.
        :return: None.
        """
        release.download_transferred()

    def transform(self, release: OrcidRelease, **kwargs):
        """Task to transform data of the ORCID release.

        :param release: an OrcidRelease instance.
        :return: None.
        """
        release.transform()


def get_aws_conn_info() -> (str, str):
    """Get the AWS access key id and secret access key from the ORCID airflow connection.

    :return: access key id and secret access key
    """
    conn = BaseHook.get_connection(AirflowConns.ORCID)
    access_key_id = conn.login
    secret_access_key = conn.password

    return access_key_id, secret_access_key


def run_subprocess_cmd(proc: Popen, args: list):
    """Execute and wait for subprocess to finish, also handle stdout & stderr from process.

    :param proc: subprocess proc
    :param args: args list that was passed on to subprocess
    :return: None.
    """
    logging.info(f"Executing bash command: {subprocess.list2cmdline(args)}")
    out, err = wait_for_process(proc)
    if out:
        logging.info(out)
    if err:
        logging.info(err)
    if proc.returncode != 0:
        # Don't raise exception if the only error is because blobs could not be found in bucket
        err_lines = err.split("\n")
        for line in err_lines[:]:
            if not line or "CommandException: No URLs matched:" in line or "could not be transferred." in line:
                err_lines.remove(line)
        if err_lines:
            raise AirflowException("bash command failed")
    logging.info("Finished cmd successfully")


def write_modified_record_blobs(
    start_date: pendulum.DateTime,
    end_date: pendulum.DateTime,
    aws_access_key_id: str,
    aws_secret_access_key: str,
    gc_download_bucket: str,
    modified_records_path: str,
) -> int:
    """Download the ORCID lambda file (last_modified.csv.tar) from AWS and use file to write the full Google Cloud
    blob names of modified records.
    The tar file is opened in memory and contains the ORCID record IDs, sorted by last modified date.

    :param start_date: Start date of the release
    :param end_date: End date of the release
    :param aws_access_key_id: AWS access key id
    :param aws_secret_access_key: AWS secret access key
    :param gc_download_bucket: Name of Google Cloud bucket with ORCID records
    :param modified_records_path: Path to file with the blob names of modified records
    :return: The number of modified records.
    """
    logging.info(f"Writing modified records to {modified_records_path}")

    # orcid lambda file, containing info on last_modified dates of records
    aws_lambda_bucket = OrcidTelescope.LAMBDA_BUCKET
    aws_lambda_object = OrcidTelescope.LAMBDA_OBJECT

    s3client = boto3.client("s3", aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
    lambda_obj = s3client.get_object(Bucket=aws_lambda_bucket, Key=aws_lambda_object)
    lambda_content = lambda_obj["Body"].read()

    modified_records_count = 0
    # open tar file in memory
    with tarfile.open(fileobj=BytesIO(lambda_content)) as tar, open(modified_records_path, "w") as f:
        for tar_resource in tar:
            if tar_resource.isfile():
                # extract last modified file in memory
                inner_file_bytes = tar.extractfile(tar_resource).read().decode().split("\n")
                for line in inner_file_bytes[1:]:
                    elements = line.split(",")
                    orcid_record = elements[0]

                    # parse through line by line, check if last_modified timestamp is between start/end date
                    last_modified_date = pendulum.parse(elements[3])

                    # skip records that are too new, not included in this release
                    if last_modified_date > end_date:
                        continue
                    # use records between start date and end date
                    elif last_modified_date >= start_date:
                        directory = orcid_record[-3:]
                        f.write(f"gs://{gc_download_bucket}/{directory}/{orcid_record}.xml" + "\n")
                        modified_records_count += 1
                    # stop when reached records before start date, not included in this release
                    else:
                        break

    return modified_records_count


def convert(k: str) -> str:
    """Convert key of dictionary to valid BQ key.

    :param k: Key
    :return: The converted key
    """
    if len(k.split(":")) > 1:
        k = k.split(":")[1]
    if k.startswith("@") or k.startswith("#"):
        k = k[1:]
    k = k.replace("-", "_")
    return k


def change_keys(obj, convert):
    """Recursively goes through the dictionary obj and replaces keys with the convert function.

    :param obj: The dictionary value, can be object of any type
    :param convert: The convert function.
    :return: The transformed object.
    """
    if isinstance(obj, (str, int, float)):
        return obj
    if isinstance(obj, dict):
        new = obj.__class__()
        for k, v in list(obj.items()):
            if k.startswith("@xmlns"):
                pass
            else:
                new[convert(k)] = change_keys(v, convert)
    elif isinstance(obj, (list, set, tuple)):
        new = obj.__class__(change_keys(v, convert) for v in obj)
    else:
        return obj
    return new
