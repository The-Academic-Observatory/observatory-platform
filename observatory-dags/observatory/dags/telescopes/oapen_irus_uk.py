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

import json
import logging
import os
import shutil
import time
from datetime import datetime
from functools import partial
from typing import List, Tuple, Optional
from urllib.parse import quote

import pendulum
import requests
from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook
from airflow.operators.python_operator import PythonOperator
from google.auth import environment_vars
from google.auth.transport.requests import AuthorizedSession
from google.oauth2.service_account import IDTokenCredentials
from googleapiclient.discovery import Resource, build
from oauth2client.service_account import ServiceAccountCredentials
from observatory.api.client.model.organisation import Organisation
from observatory.platform.telescopes.snapshot_telescope import SnapshotRelease, SnapshotTelescope
from observatory.platform.utils.airflow_utils import AirflowConns, AirflowVars
from observatory.platform.utils.config_utils import module_file_path
from observatory.platform.utils.gc_utils import copy_blob_from_cloud_storage, \
    create_cloud_storage_bucket, \
    download_blob_from_cloud_storage, \
    upload_file_to_cloud_storage
from observatory.platform.utils.template_utils import blob_name
from observatory.platform.utils.telescope_utils import make_dag_id


#TODO replace with make_org_id from telecope_utils
def make_org_id(organisation_name: str) -> str:
    """ Make an organisation id from the organisation name. Converts the organisation name to lower case,
    strips whitespace and replaces internal spaces with underscores.
    :param organisation_name: the organisation name.
    :return: the organisation id.
    """

    return organisation_name.strip().replace(' ', '_').lower()


class OapenIrusUkRelease(SnapshotRelease):
    def __init__(self, dag_id: str, release_date: pendulum.Pendulum, organisation: Organisation):
        """ Create a OapenIrusUkReleaes instance.

        :param dag_id: the DAG id.
        :param release_date: the date of the release.
        :param organisation: the Organisation of which data is processed.
        """

        transform_files_regex = f'{OapenIrusUkTelescope.DAG_ID_PREFIX}.jsonl.gz'
        super().__init__(dag_id, release_date, transform_files_regex=transform_files_regex)
        self.organisation = organisation
        self.organisation_id = make_org_id(organisation.name)

    @property
    def blob_name(self) -> str:
        """ Blob name for the access stats data, includes telescope dir

        :return: Blob name
        """
        return blob_name(os.path.join(self.download_folder,
                                      f'{self.release_date.strftime("%Y_%m")}_{self.organisation_id}.jsonl.gz'))

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

    @property
    def transform_path(self) -> str:
        """ Creates path to store the transformed oapen irus uk data

        :return: Full path to the download file
        """
        return os.path.join(self.transform_folder, f'{OapenIrusUkTelescope.DAG_ID_PREFIX}.jsonl.gz')

    def create_cloud_function(self, max_instances: int, oapen_project_id: str, source_bucket: str, function_name: str,
                              function_region: str):
        """ Create/update the google cloud function that is inside the oapen project id if the source code has changed.

        :param max_instances: The limit on the maximum number of function instances that may coexist at a given time.
        :param oapen_project_id: The oapen project id.
        :param source_bucket: Storage bucket with the source code
        :param function_name: Name of the google cloud function
        :return: None.
        """
        # set up cloud function variables
        location = f'projects/{oapen_project_id}/locations/{function_region}'
        full_name = f'{location}/functions/{function_name}'

        # zip source code and upload to bucket
        source_blob_name = 'cloud_function_source_code.zip'
        success, upload = upload_source_code_to_bucket(oapen_project_id, source_bucket, source_blob_name)
        if not success:
            raise AirflowException('Could not upload source code of cloud function to bucket.')

        # initialise cloud functions api
        creds = ServiceAccountCredentials.from_json_keyfile_name(os.environ.get(environment_vars.CREDENTIALS))
        service = build('cloudfunctions', 'v1', credentials=creds, cache_discovery=False)

        # update or create cloud function
        exists = cloud_function_exists(service, location, full_name)
        if not exists or upload is True:
            update = True if exists else False
            success = create_cloud_function(service, location, full_name, source_bucket, source_blob_name, max_instances,
                                           update)
        else:
            logging.info(f'Using existing cloud function, source code has not changed.')

    def call_cloud_function(self, oapen_project_id: str, source_bucket: str, function_name: str, function_region: str):
        """ Call the google cloud function that is inside the oapen project id

        :param oapen_project_id: The oapen project id.
        :param source_bucket: Storage bucket with the source code
        :param function_name: Name of the google cloud function
        :param function_region: Region where the google cloud function resides
        :return: None.
        """
        function_url = f'https://{function_region}-{oapen_project_id}.cloudfunctions.net/{function_name}'
        geoip_license_key = BaseHook.get_connection(AirflowConns.GEOIP_LICENSE_KEY).password

        if self.release_date >= datetime(2020, 4, 1):
            publisher_uuid = get_publisher_uuid(self.organisation_id)
            airflow_conn = AirflowConns.OAPEN_IRUS_UK_API
        else:
            publisher_uuid = "NA"
            airflow_conn = AirflowConns.OAPEN_IRUS_UK_LOGIN
        username = BaseHook.get_connection(airflow_conn).login
        password = BaseHook.get_connection(airflow_conn).password
        success = call_cloud_function(function_url, self.release_date.strftime('%Y-%m'), username, password,
                                      geoip_license_key, self.organisation_id, publisher_uuid, source_bucket, self.blob_name)
        if not success:
            raise AirflowException('Cloud function unsuccessful')

    def transfer(self, oapen_bucket: str):
        """ Transfer blob from bucket inside oapen project to bucket in airflow project.

        :param oapen_bucket: Name of the oapen bucket
        :return: None.
        """
        success = copy_blob_from_cloud_storage(self.blob_name, oapen_bucket, self.download_bucket)

    def download(self):
        """ Download blob with access stats to a local file.

        :return: None.
        """
        success = download_blob_from_cloud_storage(self.download_bucket, self.blob_name, self.transform_path)


class OapenIrusUkTelescope(SnapshotTelescope):
    DAG_ID_PREFIX = 'oapen_irus_uk'
    #TODO add other publishers
    ORG_MAPPING = {'ucl_press': quote("UCL Press", safe=''),
                   'anu_press': ''}

    def __init__(self, organisation: Organisation, dag_id: Optional[str] = None, start_date: datetime = datetime(2020, 2, 1),
                 schedule_interval: str = '@monthly', dataset_id: str = 'oapen',
                 dataset_description: str = 'Oapen dataset', catchup: bool = True, airflow_vars: List = None, airflow_conns: List = None,
                 max_active_runs=5):

        """ The OAPEN irus uk telescope.

        :param dag_id: the id of the DAG.
        :param start_date: the start date of the DAG.
        :param schedule_interval: the schedule interval of the DAG.
        :param dataset_id: the BigQuery dataset id.
        :param dataset_description: description for the BigQuery dataset.
        :param catchup:  whether to catchup the DAG or not.
        :param airflow_vars: list of airflow variable keys, for each variable it is checked if it exists in airflow.
        """

        if airflow_vars is None:
            airflow_vars = [AirflowVars.DATA_PATH, AirflowVars.PROJECT_ID, AirflowVars.DATA_LOCATION,
                            AirflowVars.DOWNLOAD_BUCKET, AirflowVars.TRANSFORM_BUCKET]

        if airflow_conns is None:
            airflow_conns = [AirflowConns.GEOIP_LICENSE_KEY, AirflowConns.OAPEN_IRUS_UK_API, AirflowConns.OAPEN_IRUS_UK_LOGIN]

        if dag_id is None:
            dag_id = make_dag_id(self.DAG_ID_PREFIX, organisation.name)

        super().__init__(dag_id, start_date, schedule_interval, dataset_id, dataset_description=dataset_description,
                         catchup=catchup, airflow_vars=airflow_vars, airflow_conns=airflow_conns, max_active_runs=max_active_runs)
        self.organisation = organisation
        self.oapen_project_id = 'oapen-usage-data-gdpr-proof'
        self.function_name = 'oapen_access_stats'
        self.function_region = 'europe-west1'
        self.oapen_bucket = f'{self.oapen_project_id}_cloud-function'

        self.add_setup_task(self.check_dependencies)
        # create PythonOperator with task concurrency of 1, so tasks to create cloud function never run in parallel
        self.add_task(partial(PythonOperator, task_id=self.create_cloud_function.__name__,
                              python_callable=partial(self.task_callable, self.create_cloud_function),
                              queue=self.queue, default_args=self.default_args, provide_context=True,
                              task_concurrency=1))
        self.add_task(self.call_cloud_function)
        self.add_task(self.transfer)
        self.add_task(self.download)
        self.add_task(self.upload_transformed)
        self.add_task(self.bq_load)
        self.add_task(self.cleanup)

    def make_release(self, **kwargs) -> List[OapenIrusUkRelease]:
        """ Create a list of OapenIrusUkRelease instances for a given month.

        :return: list of OapenIrusUkRelease instances
        """
        # Get release_date
        release_date = pendulum.instance(kwargs['execution_date'])

        logging.info(f'Release month: {release_date}')
        releases = [OapenIrusUkRelease(self.dag_id, release_date, self.organisation)]
        return releases

    def create_cloud_function(self, releases: List[OapenIrusUkRelease], **kwargs):
        """ Task to create the cloud function for each release.

        :param releases: list of OapenIrusUkRelease instances
        :return: None.
        """
        for release in releases:
            release.create_cloud_function(self.max_active_runs, self.oapen_project_id, self.oapen_bucket,
                                          self.function_name, self.function_region)

    def call_cloud_function(self, releases: List[OapenIrusUkRelease], **kwargs):
        """ Task to call the cloud function for each release.

        :param releases: list of OapenIrusUkRelease instances
        :return: None.
        """
        for release in releases:
            release.call_cloud_function(self.oapen_project_id, self.oapen_bucket,
                                        self.function_name, self.function_region)

    def transfer(self, releases: List[OapenIrusUkRelease], **kwargs):
        """ Task to transfer the file for each release.

        :param releases: the list of OapenIrusUkRelease instances.
        :return: None.
        """
        for release in releases:
            release.transfer(self.oapen_bucket)

    def download(self, releases: List[OapenIrusUkRelease], **kwargs):
        """ Task to download the access stats to a local file for each release.

        :param releases: the list of OapenIrusUkRelease instances.
        :return: None.
        """
        for release in releases:
            release.download()


def get_publisher_uuid(organisation_id: str) -> str:
    """ Get the publisher UUID from the OAPEN API using the publisher name.

    :param organisation_id: The name of the publisher
    :return: The publisher UUID
    """

    url = f'https://library.oapen.org/rest/search?query=publisher.name:{organisation_id}&expand=metadata'
    response = requests.get(url)
    logging.info(f'Getting publisher UUID for publisher: {organisation_id}, from: {url}')
    if response.status_code != 200:
        raise RuntimeError(
            f'Request to get publisher UUID unsuccessful, url: {url}, status code: {response.status_code}, response: {response.text}, reason: {response.reason}')
    response_json = response.json()
    publisher_uuid = response_json[0]['uuid']
    logging.info(f'Found publisher UUID: {publisher_uuid}')
    return publisher_uuid


def upload_source_code_to_bucket(project_id: str, bucket_name: str, blob_name: str) -> Tuple[bool, bool]:
    """ Upload source code of cloud function to storage bucket

    :param project_id: The project id with the bucket
    :param bucket_name: The bucket name
    :param blob_name: The blob name
    :return: Whether task was successful and whether file was uploaded
    """
    source_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'oapen_irus_uk_sc')
    zip_filename = os.path.join(module_file_path('observatory.dags.telescopes'), 'oapen_irus_uk_sc')
    zip_filepath = shutil.make_archive(zip_filename, 'zip', source_dir)

    # create storage bucket
    create_cloud_storage_bucket(bucket_name, location='EU', project_id=project_id, lifecycle_delete_age=1)

    # upload to cloud storage
    success, upload = upload_file_to_cloud_storage(bucket_name, blob_name, zip_filepath, project_id=project_id)
    return success, upload


def cloud_function_exists(service: Resource, location: str, full_name: str) -> bool:
    """ Check if cloud function with a given name already exists

    :param service: Cloud function service
    :param location: Location of the cloud function
    :param full_name: Name of the cloud function
    :return: True if cloud function exists, else False
    """
    response = service.projects().locations().functions().list(parent=location).execute()
    if response:
        for function in response['functions']:
            if function['name'] == full_name:
                return True
    return False


def create_cloud_function(service: Resource, location: str, full_name: str, source_bucket: str, blob_name: str,
                          max_active_runs: int, update: bool) -> bool:
    """ Create cloud function.

    :param service: Cloud function service
    :param location: Location of the cloud function
    :param full_name: Name of the cloud function
    :param source_bucket: Name of bucket where the source code is stored
    :param blob_name: Blob name of source code inside bucket
    :param max_active_runs: The limit on the maximum number of function instances that may coexist at a given time
    :param update: Whether a new function is created or an existing one is updated
    :return: Status of the cloud function
    """
    body = {
        "name": full_name,
        "description": 'Pulls oapen irus uk data and replaces ip addresses with city and country info.',
        "entryPoint": 'download',
        "runtime": 'python37',
        "timeout": '540s',  # maximum
        "availableMemoryMb": 4096,  # maximum
        "maxInstances": max_active_runs,
        "ingressSettings": 'ALLOW_ALL',
        "sourceArchiveUrl": os.path.join('gs://' + source_bucket, blob_name),
        "httpsTrigger": {
            'securityLevel': 'SECURE_ALWAYS'
        }
    }
    if update:
        update_mask = ','.join(body.keys())
        response = service.projects().locations().functions().patch(name=full_name, updateMask=update_mask,
                                                                    body=body).execute()
        logging.info('Patching cloud function')
    else:
        response = service.projects().locations().functions().create(location=location, body=body).execute()
        logging.info('Creating cloud function')

    operation_name = response.get('name')
    done = response.get('done')
    while not done:
        time.sleep(10)
        response = service.operations().get(name=operation_name).execute()
        done = response.get('done')

    error = response.get('error')
    resp = response.get('response')
    success = True if resp else False

    if success:
        logging.info(f'Creating or patching cloud function successful, response: {resp}')
    else:
        raise AirflowException(f'Creating or patching cloud function unsuccessful, error: {error}')

    return success


def call_cloud_function(function_url: str, release_date: str, username: str, password: str, geoip_license_key: str,
                        publisher_name: str, publisher_uuid: str, bucket_name: str, blob_name: str) -> bool:
    """ Call cloud function

    :param function_url: Url of the cloud function
    :param release_date: The release date in YYYY-MM
    :param username: Oapen username (email or requestor_id)
    :param password: Oapen password (password or api_key)
    :param geoip_license_key: License key of geoip database
    :param publisher_name: URL encoded name of the publisher (used for old version)
    :param publisher_uuid: UUID of the publisher (used for new version, 'NA' for old version)
    :param bucket_name: Name of the bucket to store oapen access stats data
    :param blob_name: Blob name to store oapen access stats data
    :return: Whether response code is 2000
    """
    creds = IDTokenCredentials.from_service_account_file(os.environ.get(environment_vars.CREDENTIALS),
                                                         target_audience=function_url)
    authed_session = AuthorizedSession(creds)
    data = {
        'release_date': release_date,
        'username': username,
        'password': password,
        'geoip_license_key': geoip_license_key,
        'publisher_name': publisher_name,
        'publisher_uuid': publisher_uuid,
        'bucket_name': bucket_name,
        'blob_name': blob_name
    }
    response = authed_session.post(function_url, data=json.dumps(data), headers={
        'Content-Type': 'application/json'
    }, timeout=550)
    logging.info(f'Call cloud function response status code: {response.status_code}, reason: {response.reason}')
    return response.status_code == 200
