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

import boto3
from functools import partial
import jsonlines
import logging
import os
import pathlib
import pendulum
import re
import shutil
import tarfile
import xmltodict
from io import BytesIO
from multiprocessing import cpu_count, Pool
from typing import Tuple
from airflow.hooks.base_hook import BaseHook
from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.models.taskinstance import TaskInstance
from google.cloud import bigquery
from google.cloud.bigquery import SourceFormat

from observatory_platform.utils.config_utils import (AirflowConn,
                                                     AirflowVar,
                                                     SubFolder,
                                                     check_connections,
                                                     check_variables,
                                                     find_schema,
                                                     schema_path,
                                                     telescope_path)
from observatory_platform.utils.gc_utils import (create_bigquery_dataset,
                                                 load_bigquery_table,
                                                 upload_file_to_cloud_storage,
                                                 upload_files_to_cloud_storage)


def download_modified_records(aws_access_key: str, aws_access_secret: str, aws_bucket: str, download_dir: str,
                              release: 'OrcidRelease') -> list:
    s3resource = boto3.resource('s3', aws_access_key_id=aws_access_key, aws_secret_access_key=aws_access_secret)
    # orcid lambda file, containing info on last_modified dates of records
    bucket_name = os.path.dirname(OrcidTelescope.LAST_MODIFIED_FILE)
    object_name = os.path.basename(OrcidTelescope.LAST_MODIFIED_FILE)

    lambda_obj = s3resource.Object(bucket_name, object_name)
    lambda_content = lambda_obj.get()['Body'].read()

    # get records which have been modified between dates
    records_to_download = []
    # open tar file in memory
    with tarfile.open(fileobj=BytesIO(lambda_content)) as tar:
        for tar_resource in tar:
            if tar_resource.isfile():
                # extract last modified file in memory
                inner_file_bytes = tar.extractfile(tar_resource).read().decode().split('\n')
                for line in inner_file_bytes[1:]:
                    elements = line.split(',')
                    orcid_record = elements[0]

                    # parse through line by line, check if last_modified timestamp is between start/end date
                    last_modified_date = pendulum.parse(elements[3])

                    if last_modified_date >= release.prev_start_date:
                        records_to_download.append(orcid_record)
                    else:
                        break
    # TODO remove
    # records_to_download = ['0000-0001-8911-129X', '0000-0003-1168-8720']
    item_keys = get_item_keys(records_to_download, aws_bucket, s3resource)

    pool = Pool(processes=OrcidTelescope.MAX_PROCESSES)
    pool.map(partial(download_single_file, aws_bucket, download_dir, aws_access_key, aws_access_secret), item_keys)
    pool.close()
    pool.join()
    return item_keys


def download_all_records(aws_access_key: str, aws_access_secret: str, aws_bucket: str, download_dir: str,
                         release: 'OrcidRelease') -> list:
    s3client = boto3.client('s3', aws_access_key_id=aws_access_key, aws_secret_access_key=aws_access_secret)
    # Create the paginator
    paginator = s3client.get_paginator('list_objects_v2')

    # Create a PageIterator from the Paginator
    operation_parameters = {
        'Bucket': aws_bucket,
        'PaginationConfig': {
            'PageSize': 1000
        }
    }
    token_path = release.get_continuation_token_path(aws_bucket)
    if os.path.exists(token_path):
        f = open(token_path, 'r')
        continuation = f.readline()
        operation_parameters[
            'ContinuationToken'] = continuation

    page_iterator = paginator.paginate(**operation_parameters)

    page_count = 1
    all_item_keys = []
    for page in page_iterator:
        logging.info('Page count: ' + str(page_count))
        page_count += 1
        item_keys = []
        for item in page['Contents']:
            item_keys.append(item['Key'])
        all_item_keys += item_keys
        pool = Pool(processes=OrcidTelescope.MAX_PROCESSES)
        pool.map(partial(download_single_file, aws_bucket, download_dir, aws_access_key, aws_access_secret), item_keys)
        pool.close()
        pool.join()
        continuation_token = None
        try:
            continuation_token = page['NextContinuationToken']
        except KeyError:
            logging.info('No more continuation tokens')
        with open(token_path, 'w') as f:
            f.write(str(continuation_token))

    # delete file with continuation token
    pathlib.Path(token_path).unlink()
    return all_item_keys


def get_item_keys(records_to_sync: list, aws_bucket: str, s3resource) -> list:
    item_keys = []
    for record in records_to_sync:
        for item in s3resource.Bucket(aws_bucket).objects.filter(Prefix=record[-3:] + '/' + record):
            # last_modified_date = pendulum.parse(str(item.last_modified))
            # if release.prev_start_date > last_modified_date >= release.current_date:
            item_keys.append(item.key)
    return item_keys


def download_single_file(aws_bucket: str, download_dir: str, aws_access_key: str, aws_access_secret: str,
                         item_key: str):
    logging.info(f'Downloading {item_key}')
    s3client = boto3.client('s3', aws_access_key_id=aws_access_key, aws_secret_access_key=aws_access_secret)
    orcid_dir = str(pathlib.Path(*pathlib.Path(item_key).parts[1:-1]))
    full_dir = os.path.join(download_dir, orcid_dir)
    pathlib.Path(full_dir).mkdir(parents=True, exist_ok=True)
    s3client.download_file(aws_bucket, Key=item_key, Filename=os.path.join(full_dir, os.path.basename(item_key)))


def download_records(release: 'OrcidRelease') -> Tuple[list, list]:
    """ Downloads records

    :return:  None.
    """

    logging.info(f"Downloading records modified since {release.prev_start_date}")

    aws_access_conn = BaseHook.get_connection(AirflowConn.orcid.get())
    aws_access_key = aws_access_conn.login
    aws_access_secret = aws_access_conn.password

    activities_bucket = OrcidTelescope.ACTIVITIES_BUCKET
    activities_dir = release.get_download_dir(activities_bucket)
    summaries_bucket = OrcidTelescope.SUMMARIES_BUCKET
    summaries_dir = release.get_download_dir(OrcidTelescope.SUMMARIES_BUCKET)

    if release.prev_start_date:
        logging.info(f'Previous download date: {release.prev_start_date}')
        activities_records = download_modified_records(aws_access_key, aws_access_secret, activities_bucket,
                                                       activities_dir, release)
        summaries_records = download_modified_records(aws_access_key, aws_access_secret, summaries_bucket,
                                                      summaries_dir, release)
    else:
        logging.info('No previous download available')
        activities_records = download_all_records(aws_access_key, aws_access_secret, activities_bucket, activities_dir,
                                                  release)
        summaries_records = download_all_records(aws_access_key, aws_access_secret, summaries_bucket, summaries_dir,
                                                 release)

    logging.info(f"Successfully downloaded items")
    return activities_records, summaries_records


def upload_files_per_aws_bucket(download_dir: str, gcp_bucket: str):
    file_paths = []
    for dirpath, dirnames, filenames in os.walk(download_dir):
        file_paths += [os.path.join(dirpath, file) for file in filenames]
    bucket_date_dir = str(pathlib.Path(*pathlib.Path(download_dir).parts[-2:]))
    blob_names = [f'telescopes/{OrcidTelescope.DAG_ID}/{bucket_date_dir}/{os.path.relpath(path, download_dir)}'
                  for path in file_paths]
    upload_files_to_cloud_storage(gcp_bucket, blob_names, file_paths)


def convert(k):
    # Fields must contain only letters, numbers, and underscores, start with a letter or underscore, and be at most
    # 128 characters long
    # trim special characters at start
    k = re.sub('^[^A-Za-z0-9]+', "", k)
    # replace other special characters (except '_') in remaining string
    k = re.sub('\W+', '_', k)
    return k


def change_keys(obj, convert):
    """
    Recursively goes through the dictionary obj and replaces keys with the convert function.
    """
    if isinstance(obj, (str, int, float)):
        return obj
    if isinstance(obj, dict):
        new = obj.__class__()
        for k, v in list(obj.items()):
            if k.startswith('@xmlns') or k.startswith('activities:activities-summary'):
                pass
            else:
                new[convert(k)] = change_keys(v, convert)
    elif isinstance(obj, (list, set, tuple)):
        new = obj.__class__(change_keys(v, convert) for v in obj)
    else:
        return obj
    return new


def transform_records(release: 'OrcidRelease'):
    """ Transform orcid records files.
    :param: release
    :return: whether the transformation was successful or not.
    """

    activity_fields = {
        'educations': 'education:education',
        'employments': 'employment:employment',
        'works': 'work:work',
        'fundings': 'funding:funding',
        'peer-reviews': 'peer-review:peer-review'
    }
    pathlib.Path(os.path.dirname(release.transform_path)).mkdir(parents=True, exist_ok=True)
    # since data is appended, delete if old file exists for some reason
    try:
        pathlib.Path(release.transform_path).unlink()
    except FileNotFoundError:
        pass
    # one summary file per record
    for s_root, s_dirs, s_files in os.walk(release.get_download_dir(OrcidTelescope.SUMMARIES_BUCKET)):
        # for a single record
        for summary_file in s_files:
            # get corresponding id
            orcid_id = os.path.splitext(summary_file)[0]
            # create dict of data in summary xml file
            with open(os.path.join(s_root, summary_file), 'r') as f:
                orcid_dict = xmltodict.parse(f.read())['record:record']

            # one activity dir per record
            activity_dir = os.path.join(release.get_download_dir(OrcidTelescope.ACTIVITIES_BUCKET), orcid_id)
            # for one of the 5 activity categories
            for a_root, a_dirs, a_files in os.walk(activity_dir):
                for activity_file in a_files:
                    activity = activity_file.split('_')[1]
                    activity_field = activity_fields[activity]
                    # create dict of data in activity xml file
                    with open(os.path.join(a_root, activity_file), 'r') as f:
                        activity_dict = xmltodict.parse(f.read())[activity_field]
                    # make list per activity category and append individual activity
                    try:
                        orcid_dict[activity_field].append(activity_dict)
                    except KeyError:
                        orcid_dict[activity_field] = [activity_dict]

            # change keys & delete some values
            orcid_dict = change_keys(orcid_dict, convert)
            # one dict per orcid record, append to file.
            with open(release.transform_path, 'a') as json_out:
                with jsonlines.Writer(json_out) as writer:
                    writer.write_all([orcid_dict])


class OrcidRelease:
    """ Used to store info on a given crossref release """

    def __init__(self, current_date: pendulum.Pendulum, prev_start_date: pendulum.Pendulum):
        self.current_date = current_date
        self.prev_start_date = prev_start_date

    @property
    def lambda_file_path(self) -> str:
        return self.get_path(SubFolder.downloaded, "last_modified", "csv.tar")

    def get_continuation_token_path(self, bucket: str) -> str:
        return self.get_path(SubFolder.downloaded, f"continuation_token_{bucket}", "txt")

    def get_download_dir(self, bucket: str) -> str:
        date_str = self.current_date.strftime("%Y_%m_%d")
        path = os.path.join(telescope_path(SubFolder.downloaded, OrcidTelescope.DAG_ID), date_str, bucket)
        return path

    @property
    def transform_path(self) -> str:
        return self.get_path(SubFolder.transformed, OrcidTelescope.DAG_ID, "json")

    @property
    def transform_blob_name(self):
        return f'telescopes/{OrcidTelescope.DAG_ID}/{os.path.basename(self.transform_path)}'

    def get_path(self, sub_folder: SubFolder, file_name: str, ext: str) -> str:
        """ Gets complete path of file for download/transform directory or file.

        :param sub_folder: name of subfolder
        :param file_name: partial file name that is used for complete file_name
        :param ext: extension of the file
        :return: the path.
        """
        date_str = self.current_date.strftime("%Y_%m_%d")
        file_name = f"{file_name}_{date_str}.{ext}"
        path = os.path.join(telescope_path(sub_folder, OrcidTelescope.DAG_ID), file_name)
        return path


def pull_release(ti: TaskInstance) -> OrcidRelease:
    """ Pull a CrossrefMetadataRelease instance with xcom.

    :param ti: the Apache Airflow task instance.
    :return: the list of CrossrefMetadataRelease instances.
    """

    return ti.xcom_pull(key=OrcidTelescope.RELEASES_XCOM, task_ids=OrcidTelescope.TASK_ID_DOWNLOAD,
                        include_prior_dates=False)


class OrcidTelescope:
    """ A container for holding the constants and static functions for the crossref metadata telescope. """

    DAG_ID = 'orcid'
    DATASET_ID = 'orcid'
    DESCRIPTION = ''
    RELEASES_XCOM = "releases"
    ITEMS_XCOM = "items"
    QUEUE = 'remote_queue'
    MAX_PROCESSES = cpu_count()
    MAX_CONNECTIONS = cpu_count()
    MAX_RETRIES = 3
    ACTIVITIES_BUCKET = 'v2.0-activities'
    SUMMARIES_BUCKET = 'v2.0-summaries'
    LAST_MODIFIED_FILE = 'orcid-lambda-file/last_modified.csv.tar'

    TELESCOPE_URL = 'https://api.eventdata.crossref.org/v1/events?mailto={mail_to}&' \
                    'from-occurred-date={start_date}&until-occurred-date={end_date}&rows=10000'
    # TODO create debug file
    # DEBUG_FILE_PATH = os.path.join(test_fixtures_path(), 'telescopes', 'crossref_metadata.json.tar.gz')

    TASK_ID_CHECK_DEPENDENCIES = "check_dependencies"
    TASK_ID_CHECK_RELEASE = f"check_release"
    TASK_ID_DOWNLOAD = f"download"
    TASK_ID_UPLOAD_DOWNLOADED = 'upload_downloaded'
    TASK_ID_TRANSFORM = f"transform_releases"
    TASK_ID_UPLOAD_TRANSFORMED = 'upload_transformed'
    TASK_ID_BQ_LOAD = f"bq_load"
    TASK_ID_CLEANUP = f"cleanup"

    @staticmethod
    def check_dependencies():
        """ Check that all variables exist that are required to run the DAG.

        :return: None.
        """

        vars_valid = check_variables(AirflowVar.data_path.get(), AirflowVar.project_id.get(),
                                     AirflowVar.data_location.get(), AirflowVar.download_bucket_name.get(),
                                     AirflowVar.transform_bucket_name.get())
        conns_valid = check_connections(AirflowConn.orcid.get())

        if not vars_valid or not conns_valid:
            raise AirflowException('Required variables or connections are missing')

    @staticmethod
    def download(**kwargs):
        ti: TaskInstance = kwargs['ti']

        current_date = pendulum.now()
        prev_start_date = kwargs['prev_start_date_success']

        release = OrcidRelease(current_date, prev_start_date)
        ti.xcom_push(OrcidTelescope.RELEASES_XCOM, release)

        activities_items, summaries_items = download_records(release)
        ti.xcom_push(OrcidTelescope.ITEMS_XCOM, (activities_items, summaries_items))

    @staticmethod
    def upload_downloaded(**kwargs):
        """ Upload the downloaded files to a Google Cloud Storage bucket.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        ti: TaskInstance = kwargs['ti']

        release = pull_release(ti)

        gcp_bucket = Variable.get(AirflowVar.download_bucket_name.get())

        upload_files_per_aws_bucket(release.get_download_dir(OrcidTelescope.ACTIVITIES_BUCKET), gcp_bucket)
        upload_files_per_aws_bucket(release.get_download_dir(OrcidTelescope.SUMMARIES_BUCKET), gcp_bucket)

    @staticmethod
    def transform(**kwargs):
        """ Transform release with sed command and save to new file.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        # Pull releases
        ti: TaskInstance = kwargs['ti']
        release = pull_release(ti)

        # Transform release
        transform_records(release)

    @staticmethod
    def upload_transformed(**kwargs):
        """ Upload transformed release to a Google Cloud Storage bucket.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        # Pull releases
        ti: TaskInstance = kwargs['ti']
        release = pull_release(ti)

        # Get variables
        bucket_name = Variable.get(AirflowVar.transform_bucket_name.get())

        # Upload files
        logging.info(f'Uploading transformed file')

        success = upload_file_to_cloud_storage(bucket_name, release.transform_blob_name, release.transform_path)
        if success:
            logging.info(f'upload_transformed success: {release}')
        else:
            logging.error(f"upload_transformed error: {release}")
            exit(os.EX_DATAERR)

    @staticmethod
    def bq_load(**kwargs):
        """ Upload transformed release to a bigquery table.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        # Pull releases
        ti: TaskInstance = kwargs['ti']
        release = pull_release(ti)

        # Get variables
        project_id = Variable.get(AirflowVar.project_id.get())
        data_location = Variable.get(AirflowVar.data_location.get())
        bucket_name = Variable.get(AirflowVar.transform_bucket_name.get())

        # Create dataset
        dataset_id = OrcidTelescope.DATASET_ID
        create_bigquery_dataset(project_id, dataset_id, data_location, OrcidTelescope.DESCRIPTION)

        # Select schema file based on release date
        analysis_schema_path = schema_path('telescopes')
        schema_file_path = find_schema(analysis_schema_path, OrcidTelescope.DAG_ID, release.current_date)
        if schema_file_path is None:
            logging.error(f'No schema found with search parameters: analysis_schema_path={analysis_schema_path}, '
                          f'table_name={OrcidTelescope.DAG_ID}, release_date={release.current_date}')
            exit(os.EX_CONFIG)

        # Load BigQuery table
        uri = f"gs://{bucket_name}/{release.transform_blob_name}"
        logging.info(f"URI: {uri}")

        load_bigquery_table(uri, dataset_id, data_location, OrcidTelescope.DAG_ID, schema_file_path,
                            SourceFormat.NEWLINE_DELIMITED_JSON,
                            write_disposition=bigquery.WriteDisposition.WRITE_APPEND)

    @staticmethod
    def cleanup(**kwargs):
        """ Delete files of downloaded, extracted and transformed release.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        # Pull releases
        ti: TaskInstance = kwargs['ti']
        release = pull_release(ti)

        # delete download dirs
        try:
            shutil.rmtree(release.get_download_dir(OrcidTelescope.ACTIVITIES_BUCKET))
        except FileNotFoundError as e:
            logging.warning(
                f"No such file or directory {release.get_download_dir(OrcidTelescope.ACTIVITIES_BUCKET)}: {e}")
        try:
            shutil.rmtree(release.get_download_dir(OrcidTelescope.SUMMARIES_BUCKET))
        except FileNotFoundError as e:
            logging.warning(
                f"No such file or directory {release.get_download_dir(OrcidTelescope.SUMMARIES_BUCKET)}: {e}")

        # delete lambda last_modified file
        try:
            pathlib.Path(release.lambda_file_path).unlink()
        except FileNotFoundError as e:
            logging.warning(f"No such file or directory {release.transform_path}: {e}")

        # delete transformed file
        try:
            pathlib.Path(release.transform_path).unlink()
        except FileNotFoundError as e:
            logging.warning(f"No such file or directory {release.transform_path}: {e}")
