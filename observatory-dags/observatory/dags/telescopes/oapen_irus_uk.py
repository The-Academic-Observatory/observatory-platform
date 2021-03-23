import logging
import os
import shutil
from datetime import datetime
from typing import List

import pendulum
from google.auth import environment_vars
from google.auth.transport.requests import AuthorizedSession
from google.oauth2.service_account import IDTokenCredentials
from googleapiclient.discovery import Resource, build
from oauth2client.service_account import ServiceAccountCredentials
from observatory.platform.telescopes.snapshot_telescope import SnapshotRelease, SnapshotTelescope
from observatory.platform.utils.airflow_utils import AirflowVariable as Variable, AirflowVars
from observatory.platform.utils.config_utils import module_file_path
from observatory.platform.utils.gc_utils import upload_file_to_cloud_storage


def create_cloud_function(service: Resource, location: str, full_name: str, source_bucket: str, blob_name: str,
                          max_active_runs: int):
    """

    :param service:
    :param location:
    :param full_name:
    :param source_bucket:
    :param blob_name:
    :param max_active_runs:
    :return:
    """
    project_id = Variable.get(AirflowVars.PROJECT_ID)
    body = {
        "name": full_name,
        "description": 'the description',
        "entryPoint": 'download',
        "runtime": 'python37',
        "timeout": '540s',  # maximum
        "availableMemoryMb": 4000,  # maximum
        "serviceAccountEmail": f'{project_id}@{project_id}.iam.gserviceaccount.com',  # airflow service account
        "maxInstances": max_active_runs,  # set to max number of active airflow dag runs
        "ingressSettings": 'ALLOW_ALL',
        "sourceArchiveUrl": os.path.join(source_bucket, blob_name),
        "httpsTrigger": {
            'securityLevel': 'SECURE_ALWAYS'
        }
    }
    service.projects().locations().functions().create(location=location, body=body).execute()


def call_function(function_url: str, release_date: str, publisher_info: dict, blob_name: str) -> bool:
    """

    :param function_url:
    :param release_date:
    :param publisher_info:
    :param blob_name:
    :return:
    """
    creds = IDTokenCredentials.from_service_account_file(os.environ.get(environment_vars.CREDENTIALS),
                                                         target_audience=function_url)
    authed_session = AuthorizedSession(creds)
    logging.info('Calling google cloud function')
    response = authed_session.get(function_url, data={
        'release_date': release_date,
        'publisher_info': publisher_info,
        'blob_name': blob_name
    })
    logging.info(f'Cloud function response status code: {response.status_code}')
    return response.status_code == 200


class OapenIrusUkRelease(SnapshotRelease):
    def __init__(self, dag_id: str, release_date: pendulum.Pendulum):
        """ Create a OapenIrusUkReleaes instance.

        :param dag_id: the DAG id.
        :param release_date: the date of the release.
        """

        transform_files_regex = f'{dag_id}.jsonl.gz'
        super().__init__(dag_id, release_date, transform_files_regex=transform_files_regex)

    def download(self, max_instances: int):
        """

        :param max_instances:
        :return:
        """
        # set up variables
        oapen_project_id = 'oapen-library-usage-portal'
        region = 'us-central1'
        location = f'projects/{oapen_project_id}/locations/{region}'
        function_name = 'oapen_access_stats'
        full_name = f'{location}/functions/{function_name}'

        # zip source code
        source_dir = module_file_path('observatory.dags.telescopes.oapen_irus_uk_sc.main')
        source_file_path = os.path.join(module_file_path('observatory.dags.telescopes'), 'oapen_irus_uk_sc.zip')
        shutil.make_archive(source_file_path, 'zip', source_dir)

        # upload to cloud storage
        source_bucket = 'gs://test_cloud_function_123'
        source_blob_name = 'main.zip'
        upload_file_to_cloud_storage(source_bucket, source_blob_name, source_file_path)

        # should have permissions to create/list/invoke cloud functions inside oapen project to airflow service account
        creds = ServiceAccountCredentials.from_json_keyfile_name(os.environ.get(environment_vars.CREDENTIALS))
        service = build('cloudfunctions', 'v1', credentials=creds)

        # create function if it does not exist yet
        response = service.projects().locations().functions().list(parent=location).execute()
        for function in response['functions']:
            if function['name'] == full_name:
                break
        else:
            create_cloud_function(service, location, full_name, source_bucket, source_blob_name, max_instances)

        # call function
        function_url = f'https://{region}-{oapen_project_id}.cloudfunctions.net/{function_name}'
        blob_name = self.transform_files_regex
        publisher_info = {
            "name": "UCL Press",
            "project": "aniek-dev",
            "bucket": "test_cloud_function_123"
        }
        success = call_function(function_url, self.release_date.strftime('%Y-%m'), publisher_info, blob_name)


class OapenIrusUkTelescope(SnapshotTelescope):
    DAG_ID = 'oapen_irus_uk'

    def __init__(self, dag_id: str = DAG_ID, start_date: datetime = datetime(2020, 9, 1),
                 schedule_interval: str = '@monthly', dataset_id: str = 'oapen',
                 dataset_description: str = 'Oapen dataset', catchup: bool = False, airflow_vars: List = None):

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
        super().__init__(dag_id, start_date, schedule_interval, dataset_id, dataset_description=dataset_description,
                         catchup=catchup, airflow_vars=airflow_vars)
        self.add_setup_task(self.check_dependencies)
        self.add_task(self.download)
        self.add_task(self.bq_load)
        self.add_task(self.cleanup)

    def make_release(self, **kwargs) -> List[OapenIrusUkRelease]:
        # Get release_date
        release_date = pendulum.instance(kwargs['dag_run'].execution_date)

        logging.info(f'Release month: {release_date}')
        releases = [OapenIrusUkRelease(self.dag_id, release_date)]
        return releases

    def download(self, releases: List[OapenIrusUkRelease], **kwargs):
        """ Task to download the GeonamesRelease release for a given month.

        :param releases: the list of GeonamesRelease instances.
        :return: None.
        """

        # Download each release
        for release in releases:
            release.download(self.max_active_runs)
