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
import json
import logging
import os
import time
from typing import Dict, List, Optional, Tuple
from urllib.parse import quote

import pendulum
import requests
from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.hooks.base import BaseHook
from google.auth import environment_vars
from google.auth.transport.requests import AuthorizedSession
from google.cloud import bigquery
from google.oauth2.service_account import IDTokenCredentials
from googleapiclient.discovery import Resource, build
from oauth2client.service_account import ServiceAccountCredentials

from observatory.api.client.model.organisation import Organisation
from oaebu_workflows.config import schema_folder as default_schema_folder
from observatory.platform.utils.airflow_utils import AirflowConns, AirflowVars
from observatory.platform.utils.data_utils import get_file
from observatory.platform.utils.file_utils import _hash_file, list_to_jsonl_gz
from observatory.platform.utils.gc_utils import (
    copy_blob_from_cloud_storage,
    create_cloud_storage_bucket,
    download_blob_from_cloud_storage,
    upload_file_to_cloud_storage,
)
from observatory.platform.utils.workflow_utils import (
    SubFolder,
    blob_name,
    bq_load_partition,
    table_ids_from_path,
    workflow_path,
)
from observatory.platform.utils.workflow_utils import (
    add_partition_date,
    make_dag_id,
    make_org_id,
)
from observatory.platform.workflows.snapshot_telescope import (
    SnapshotRelease,
    SnapshotTelescope,
)


class OapenIrusUkRelease(SnapshotRelease):
    def __init__(self, dag_id: str, release_date: pendulum.DateTime, organisation: Organisation):
        """Create a OapenIrusUkReleaes instance.

        :param dag_id: the DAG id.
        :param release_date: the date of the release.
        :param organisation: the Organisation of which data is processed.
        """

        transform_files_regex = f"{OapenIrusUkTelescope.DAG_ID_PREFIX}.jsonl.gz"
        super().__init__(dag_id, release_date, transform_files_regex=transform_files_regex)
        self.organisation = organisation
        self.organisation_id = make_org_id(organisation.name)

    @property
    def blob_name(self) -> str:
        """Blob name for the access stats data, includes telescope dir

        :return: Blob name
        """
        return blob_name(
            os.path.join(self.download_folder, f'{self.release_date.strftime("%Y_%m")}_{self.organisation_id}.jsonl.gz')
        )

    @property
    def download_bucket(self):
        """The download bucket name.
        :return: the download bucket name.
        """
        return self.organisation.gcp_download_bucket

    @property
    def transform_bucket(self):
        """The transform bucket name.
        :return: the transform bucket name.
        """
        return self.organisation.gcp_transform_bucket

    @property
    def download_path(self) -> str:
        """Creates path to store the downloaded oapen irus uk data

        :return: Full path to the download file
        """
        return os.path.join(self.download_folder, f"{OapenIrusUkTelescope.DAG_ID_PREFIX}.jsonl.gz")

    @property
    def transform_path(self) -> str:
        """Creates path to store the transformed oapen irus uk data

        :return: Full path to the transform file
        """
        return os.path.join(self.transform_folder, f"{OapenIrusUkTelescope.DAG_ID_PREFIX}.jsonl.gz")

    def create_cloud_function(self, max_instances: int):
        """Create/update the google cloud function that is inside the oapen project id if the source code has changed.

        :param max_instances: The limit on the maximum number of function instances that may coexist at a given time.
        :return: None.
        """

        # set up cloud function variables
        oapen_project_id = OapenIrusUkTelescope.OAPEN_PROJECT_ID
        source_bucket = OapenIrusUkTelescope.OAPEN_BUCKET
        function_name = OapenIrusUkTelescope.FUNCTION_NAME
        function_region = OapenIrusUkTelescope.FUNCTION_REGION
        function_source_url = OapenIrusUkTelescope.FUNCTION_SOURCE_URL
        function_blob_name = OapenIrusUkTelescope.FUNCTION_BLOB_NAME
        location = f"projects/{oapen_project_id}/locations/{function_region}"
        full_name = f"{location}/functions/{function_name}"

        # zip source code and upload to bucket
        success, upload = upload_source_code_to_bucket(
            function_source_url, oapen_project_id, source_bucket, function_blob_name
        )
        if not success:
            raise AirflowException("Could not upload source code of cloud function to bucket.")

        # initialise cloud functions api
        creds = ServiceAccountCredentials.from_json_keyfile_name(os.environ.get(environment_vars.CREDENTIALS))
        service = build("cloudfunctions", "v1", credentials=creds, cache_discovery=False)

        # update or create cloud function
        exists = cloud_function_exists(service, location, full_name)
        if not exists or upload is True:
            update = True if exists else False
            success, msg = create_cloud_function(
                service, location, full_name, source_bucket, function_blob_name, max_instances, update
            )
            if success:
                logging.info(f"Creating or patching cloud function successful, response: {msg}")
            else:
                raise AirflowException(f"Creating or patching cloud function unsuccessful, error: {msg}")
        else:
            logging.info(f"Using existing cloud function, source code has not changed.")

    def call_cloud_function(self, publisher_id: str):
        """Call the google cloud function that is inside the oapen project id

        :param publisher_id: The publisher ID used with the OAPEN API.
        :return: None.
        """
        # set up cloud function variables
        oapen_project_id = OapenIrusUkTelescope.OAPEN_PROJECT_ID
        source_bucket = OapenIrusUkTelescope.OAPEN_BUCKET
        function_name = OapenIrusUkTelescope.FUNCTION_NAME
        function_region = OapenIrusUkTelescope.FUNCTION_REGION
        function_url = f"https://{function_region}-{oapen_project_id}.cloudfunctions.net/{function_name}"
        geoip_license_key = BaseHook.get_connection(AirflowConns.GEOIP_LICENSE_KEY).password

        # get the publisher_uuid or publisher_id, both are set to empty strings when publisher id is 'oapen'
        if self.release_date >= pendulum.datetime(2020, 4, 1):
            if publisher_id == "oapen":
                publisher_uuid = ""
            else:
                publisher_uuid = get_publisher_uuid(publisher_id)
            airflow_conn = AirflowConns.OAPEN_IRUS_UK_API
        else:
            publisher_uuid = "NA"
            airflow_conn = AirflowConns.OAPEN_IRUS_UK_LOGIN
            if publisher_id == "oapen":
                publisher_id = ""
        username = BaseHook.get_connection(airflow_conn).login
        password = BaseHook.get_connection(airflow_conn).password

        call_cloud_function(
            function_url,
            self.release_date.strftime("%Y-%m"),
            username,
            password,
            geoip_license_key,
            publisher_id,
            publisher_uuid,
            source_bucket,
            self.blob_name,
        )

    def transfer(self):
        """Transfer blob from bucket inside oapen project to bucket in airflow project.

        :return: None.
        """
        success = copy_blob_from_cloud_storage(self.blob_name, OapenIrusUkTelescope.OAPEN_BUCKET, self.download_bucket)
        if not success:
            raise AirflowException("Transfer blob unsuccessful")

    def download_transform(self):
        """Download blob with access stats to a local file.

        :return: None.
        """
        success = download_blob_from_cloud_storage(self.download_bucket, self.blob_name, self.download_path)
        if not success:
            raise AirflowException("Download blob unsuccessful")

        # Read gzipped data and create list of dicts
        with gzip.open(self.download_path, "r") as f:
            results = [json.loads(line) for line in f]

        # Add partition date
        results = add_partition_date(results, self.release_date, bigquery.TimePartitioningType.MONTH)

        # Write list into gzipped JSON Lines file
        list_to_jsonl_gz(self.transform_path, results)


class OapenIrusUkTelescope(SnapshotTelescope):
    DAG_ID_PREFIX = "oapen_irus_uk"

    OAPEN_PROJECT_ID = "oapen-usage-data-gdpr-proof"  # The oapen project id.
    OAPEN_BUCKET = f"{OAPEN_PROJECT_ID}_cloud-function"  # Storage bucket with the source code
    FUNCTION_NAME = "oapen_access_stats"  # Name of the google cloud function
    FUNCTION_REGION = "europe-west1"  # Region of the google cloud function
    FUNCTION_SOURCE_URL = (
        "https://github.com/The-Academic-Observatory/oapen-irus-uk-cloud-function/releases/"
        "download/v1.1.4/oapen-irus-uk-cloud-function.zip"
    )  # URL to the zipped source code of the
    # cloud function
    FUNCTION_MD5_HASH = "46b07fbc9e70756b3cc80cb9ab38934c"  # MD5 hash of the zipped source code
    FUNCTION_BLOB_NAME = "cloud_function_source_code.zip"  # blob name of zipped source code
    OAPEN_API_URL = "https://library.oapen.org/rest/search?query=publisher.name:{publisher_name}&expand=metadata"

    def __init__(
        self,
        organisation: Organisation,
        publisher_id: str,
        dag_id: Optional[str] = None,
        start_date: pendulum.DateTime = pendulum.datetime(2018, 1, 1),
        schedule_interval: str = "0 0 14 * *",
        dataset_id: str = "oapen",
        schema_folder: str = default_schema_folder(),
        dataset_description: str = "OAPEN dataset",
        table_descriptions: Dict = None,
        catchup: bool = True,
        airflow_vars: List = None,
        airflow_conns: List = None,
        max_active_runs=5,
        max_cloud_function_instances: int = 0,
    ):

        """The OAPEN irus uk telescope.
        :param organisation: the Organisation the DAG will process.
        :param publisher_id: the publisher ID, obtained from the 'extra' info from the API regarding the telescope.
        :param dag_id: the id of the DAG.
        :param start_date: the start date of the DAG.
        :param schedule_interval: the schedule interval of the DAG.
        :param dataset_id: the BigQuery dataset id.
        :param schema_folder: the SQL schema path.
        :param dataset_description: description for the BigQuery dataset.
        :param table_descriptions: a dictionary with table ids and corresponding table descriptions.
        :param catchup:  whether to catchup the DAG or not.
        :param airflow_vars: list of airflow variable keys, for each variable it is checked if it exists in airflow.
        :param airflow_conns: list of airflow connection keys, for each connection it is checked if it exists in airflow
        :param max_cloud_function_instances: the maximum cloud function instances.
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
            airflow_conns = [
                AirflowConns.GEOIP_LICENSE_KEY,
                AirflowConns.OAPEN_IRUS_UK_API,
                AirflowConns.OAPEN_IRUS_UK_LOGIN,
            ]

        if table_descriptions is None:
            table_descriptions = {
                self.DAG_ID_PREFIX: "Access stats from the OAPEN IRUS UK platform. Before 2020-04 "
                "from: https://irus.jisc.ac.uk/IRUSConsult/irus-oapen/v2/. "
                "After 2020-04 from the OAPEN_SUSHI API (documentation not "
                "published)."
            }

        if dag_id is None:
            dag_id = make_dag_id(self.DAG_ID_PREFIX, organisation.name)

        super().__init__(
            dag_id,
            start_date,
            schedule_interval,
            dataset_id,
            schema_folder,
            dataset_description=dataset_description,
            catchup=catchup,
            airflow_vars=airflow_vars,
            airflow_conns=airflow_conns,
            max_active_runs=max_active_runs,
            table_descriptions=table_descriptions,
        )
        self.max_cloud_function_instances = max_cloud_function_instances
        self.organisation = organisation
        self.project_id = organisation.gcp_project_id
        self.dataset_location = "us"  # TODO: add to API
        self.publisher_id = publisher_id

        self.add_setup_task(self.check_dependencies)
        # create PythonOperator with task concurrency of 1, so tasks to create cloud function never run in parallel
        self.add_task(self.create_cloud_function, task_concurrency=1)
        self.add_task(self.call_cloud_function)
        self.add_task(self.transfer)
        self.add_task(self.download_transform)
        self.add_task(self.upload_transformed)
        self.add_task(self.bq_load_partition)
        self.add_task(self.cleanup)

    def make_release(self, **kwargs) -> List[OapenIrusUkRelease]:
        """Create a list of OapenIrusUkRelease instances for a given month.

        :return: list of OapenIrusUkRelease instances
        """
        # Get release_date
        release_date = pendulum.instance(kwargs["execution_date"]).end_of("month")

        logging.info(f"Release date: {release_date}")
        releases = [OapenIrusUkRelease(self.dag_id, release_date, self.organisation)]
        return releases

    def check_dependencies(self, **kwargs) -> bool:
        """Check dependencies of DAG. Add to parent method to additionally check for a view id
        :return: True if dependencies are valid.
        """
        super().check_dependencies()

        if self.publisher_id is None:
            expected_extra = {"publisher_id": quote("Publisher Name")}
            raise AirflowException(f"Publisher ID is not set in 'extra' of telescope, extra example: {expected_extra}")
        return True

    def create_cloud_function(self, releases: List[OapenIrusUkRelease], **kwargs):
        """Task to create the cloud function for each release.

        :param releases: list of OapenIrusUkRelease instances
        :return: None.
        """
        for release in releases:
            release.create_cloud_function(self.max_cloud_function_instances)

    def call_cloud_function(self, releases: List[OapenIrusUkRelease], **kwargs):
        """Task to call the cloud function for each release.

        :param releases: list of OapenIrusUkRelease instances
        :return: None.
        """
        for release in releases:
            release.call_cloud_function(self.publisher_id)

    def transfer(self, releases: List[OapenIrusUkRelease], **kwargs):
        """Task to transfer the file for each release.

        :param releases: the list of OapenIrusUkRelease instances.
        :return: None.
        """
        for release in releases:
            release.transfer()

    def download_transform(self, releases: List[OapenIrusUkRelease], **kwargs):
        """Task to download the access stats to a local file for each release.

        :param releases: the list of OapenIrusUkRelease instances.
        :return: None.
        """
        for release in releases:
            release.download_transform()

    def bq_load_partition(self, releases: List[OapenIrusUkRelease], **kwargs):
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
                table_description = self.table_descriptions.get(table_id, "")

                bq_load_partition(
                    self.schema_folder,
                    self.project_id,
                    release.transform_bucket,
                    transform_blob,
                    self.dataset_id,
                    self.dataset_location,
                    table_id,
                    release.release_date,
                    self.source_format,
                    bigquery.table.TimePartitioningType.MONTH,
                    prefix=self.schema_prefix,
                    schema_version=self.schema_version,
                    dataset_description=self.dataset_description,
                    table_description=table_description,
                    **self.load_bigquery_table_kwargs,
                )


def get_publisher_uuid(publisher_name: str) -> str:
    """Get the publisher UUID from the OAPEN API using the publisher name.

    :param publisher_name: The name of the publisher
    :return: The publisher UUID
    """
    url = OapenIrusUkTelescope.OAPEN_API_URL.format(publisher_name=publisher_name)
    response = requests.get(url)
    logging.info(f"Getting publisher UUID for publisher: {publisher_name}, from: {url}")
    if response.status_code != 200:
        raise RuntimeError(
            f"Request to get publisher UUID unsuccessful, url: {url}, status code: {response.status_code}, "
            f"response: {response.text}, reason: {response.reason}"
        )
    response_json = response.json()
    publisher_uuid = response_json[0]["uuid"]
    logging.info(f"Found publisher UUID: {publisher_uuid}")
    return publisher_uuid


def upload_source_code_to_bucket(
    source_url: str, project_id: str, bucket_name: str, blob_name: str
) -> Tuple[bool, bool]:
    """Upload source code of cloud function to storage bucket

    :param source_url: The url to the zip file with source code
    :param project_id: The project id with the bucket
    :param bucket_name: The bucket name
    :param blob_name: The blob name
    :return: Whether task was successful and whether file was uploaded
    """

    # get zip file with source code from github release
    telescope_folder = workflow_path(SubFolder.downloaded.value, OapenIrusUkTelescope.DAG_ID_PREFIX)
    filepath = os.path.join(telescope_folder, "oapen_cloud_function.zip")
    expected_md5_hash = OapenIrusUkTelescope.FUNCTION_MD5_HASH
    filepath, download = get_file(filepath, source_url, md5_hash=expected_md5_hash)

    # check if current md5 hash matches expected md5 hash
    actual_md5_hash = _hash_file(filepath, algorithm="md5")
    if expected_md5_hash != actual_md5_hash:
        raise AirflowException(f"md5 hashes do not match, expected: {expected_md5_hash}, actual: {actual_md5_hash}")

    # create storage bucket
    create_cloud_storage_bucket(bucket_name, location="EU", project_id=project_id, lifecycle_delete_age=1)

    # upload zip to cloud storage
    success, upload = upload_file_to_cloud_storage(bucket_name, blob_name, filepath, project_id=project_id)
    return success, upload


def cloud_function_exists(service: Resource, location: str, full_name: str) -> bool:
    """Check if cloud function with a given name already exists

    :param service: Cloud function service
    :param location: Location of the cloud function
    :param full_name: Name of the cloud function
    :return: True if cloud function exists, else False
    """
    response = service.projects().locations().functions().list(parent=location).execute()
    if response:
        for function in response["functions"]:
            if function["name"] == full_name:
                return True
    return False


def create_cloud_function(
    service: Resource,
    location: str,
    full_name: str,
    source_bucket: str,
    blob_name: str,
    max_active_runs: int,
    update: bool,
) -> Tuple[bool, dict]:
    """Create cloud function.

    :param service: Cloud function service
    :param location: Location of the cloud function
    :param full_name: Name of the cloud function
    :param source_bucket: Name of bucket where the source code is stored
    :param blob_name: Blob name of source code inside bucket
    :param max_active_runs: The limit on the maximum number of function instances that may coexist at a given time
    :param update: Whether a new function is created or an existing one is updated
    :return: Status of the cloud function and error/success message
    """
    body = {
        "name": full_name,
        "description": "Pulls oapen irus uk data and replaces ip addresses with city and country info.",
        "entryPoint": "download",
        "runtime": "python37",
        "timeout": "540s",  # maximum
        "availableMemoryMb": 4096,  # maximum
        "maxInstances": max_active_runs,
        "ingressSettings": "ALLOW_ALL",
        "sourceArchiveUrl": os.path.join("gs://" + source_bucket, blob_name),
        "httpsTrigger": {"securityLevel": "SECURE_ALWAYS"},
    }
    if update:
        update_mask = ",".join(body.keys())
        response = (
            service.projects()
            .locations()
            .functions()
            .patch(name=full_name, updateMask=update_mask, body=body)
            .execute()
        )
        logging.info("Patching cloud function")
    else:
        response = service.projects().locations().functions().create(location=location, body=body).execute()
        logging.info("Creating cloud function")

    operation_name = response.get("name")
    done = response.get("done")
    while not done:
        time.sleep(10)
        response = service.operations().get(name=operation_name).execute()
        done = response.get("done")

    error = response.get("error")
    resp = response.get("response")
    success = True if resp else False
    msg = resp if success else error

    return success, msg


def call_cloud_function(
    function_url: str,
    release_date: str,
    username: str,
    password: str,
    geoip_license_key: str,
    publisher_name: str,
    publisher_uuid: str,
    bucket_name: str,
    blob_name: str,
):
    """Iteratively call cloud function, until it has finished processing all publishers.
    When a publisher name/uuid  is given, there is only 1 publisher, if it is empty the cloud function will process
    all available publishers. In that case, when the data is downloaded from the new platform it can be done in 1
    iteration, however for the old platform two files have to be downloaded separately for each publisher,
    this might take longer than the timeout time of the cloud function, so the process is split up in multiple calls.

    :param function_url: Url of the cloud function
    :param release_date: The release date in YYYY-MM
    :param username: Oapen username (email or requestor_id)
    :param password: Oapen password (password or api_key)
    :param geoip_license_key: License key of geoip database
    :param publisher_name: URL encoded name of the publisher (used for old version)
    :param publisher_uuid: UUID of the publisher (used for new version, 'NA' for old version)
    :param bucket_name: Name of the bucket to store oapen access stats data
    :param blob_name: Blob name to store oapen access stats data
    :return: None
    """
    creds = IDTokenCredentials.from_service_account_file(
        os.environ.get(environment_vars.CREDENTIALS), target_audience=function_url
    )
    authed_session = AuthorizedSession(creds)
    data = {
        "release_date": release_date,
        "username": username,
        "password": password,
        "geoip_license_key": geoip_license_key,
        "publisher_name": publisher_name,
        "publisher_uuid": publisher_uuid,
        "bucket_name": bucket_name,
        "blob_name": blob_name,
    }
    finished = False
    while not finished:
        response = authed_session.post(
            function_url, data=json.dumps(data), headers={"Content-Type": "application/json"}, timeout=550
        )
        logging.info(f"Call cloud function response status code: {response.status_code}, reason: {response.reason}")
        if response.status_code != 200:
            raise AirflowException("Cloud function unsuccessful")

        response_json = response.json()
        if response_json["unprocessed_publishers"]:
            data["unprocessed_publishers"] = response_json["unprocessed_publishers"]
            remaining_publishers = len(response_json["unprocessed_publishers"])
        else:
            finished = True
            remaining_publishers = 0

        entries = response_json["entries"]
        if entries == 0 and remaining_publishers == 0:
            raise AirflowSkipException("No access stats entries for publisher(s) in month.")

        logging.info(f"Processed {entries} entries in total. {remaining_publishers} publishers " f"left to process")
