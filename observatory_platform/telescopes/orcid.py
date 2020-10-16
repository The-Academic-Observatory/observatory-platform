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
import jsonlines
import logging
import os
import pathlib
import pendulum
import re
import shutil
import subprocess
import tarfile
import xmltodict
from datetime import datetime, timedelta
from functools import partial
from io import BytesIO
from multiprocessing import cpu_count, Pool
from subprocess import Popen
from typing import Tuple
from airflow.hooks.base_hook import BaseHook
from airflow.exceptions import AirflowException, AirflowSkipException
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
                                                     telescope_path,
                                                     telescope_templates_path)
from observatory_platform.utils.gc_utils import (aws_to_google_cloud_storage_transfer,
                                                 bigquery_partitioned_table_id,
                                                 copy_bigquery_table,
                                                 create_bigquery_dataset,
                                                 download_blobs_from_cloud_storage,
                                                 load_bigquery_table,
                                                 run_bigquery_query,
                                                 storage_bucket_exists,
                                                 upload_file_to_cloud_storage)
from observatory_platform.utils.jinja2_utils import (make_sql_jinja2_filename, render_template)
from observatory_platform.utils.proc_utils import stream_process


# def create_chunks(complete_list, no_chunks):
#     chunks_list = []
#     for i in range(0, no_chunks):
#         chunks_list.append(complete_list[i::no_chunks])
#     return chunks_list


# def write_modified_records_list(aws_access_key_id: str, aws_secret_access_key: str, start_date: pendulum.Pendulum) ->\
#         pendulum.Pendulum:
#     modified_record_keys_path = OrcidRelease.modified_record_keys_path(start_date)
#
#     logging.info(f'Writing modified records to {modified_record_keys_path}')
#     s3resource = boto3.resource('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
#
#     aws_summaries_bucket = OrcidTelescope.SUMMARIES_BUCKET
#     # orcid lambda file, containing info on last_modified dates of records
#     aws_lambda_bucket = OrcidTelescope.LAMBDA_BUCKET
#     aws_lambda_object = OrcidTelescope.LAMBDA_OBJECT
#
#     lambda_obj = s3resource.Object(aws_lambda_bucket, aws_lambda_object)
#     lambda_content = lambda_obj.get()['Body'].read()
#
#     end_date = None
#     # open tar file in memory
#     with tarfile.open(fileobj=BytesIO(lambda_content)) as tar, open(modified_record_keys_path, 'w') as f:
#         for tar_resource in tar:
#             if tar_resource.isfile():
#                 # extract last modified file in memory
#                 inner_file_bytes = tar.extractfile(tar_resource).read().decode().split('\n')
#                 for line in inner_file_bytes[1:]:
#                     elements = line.split(',')
#                     orcid_record = elements[0]
#
#                     # parse through line by line, check if last_modified timestamp is between start/end date
#                     last_modified_date = pendulum.parse(elements[3])
#                     if not end_date:
#                         end_date = last_modified_date
#
#                     if start_date <= last_modified_date:
#                         directory = orcid_record[-3:]
#                         f.write(f"{directory}/{orcid_record}.xml" + "\n")
#                         # f.write(f"s3://{aws_summaries_bucket}/{directory}/{orcid_record}.xml" + "\n")
#                     else:
#                         break
#     return end_date


def write_modified_record_prefixes(aws_access_key_id: str, aws_secret_access_key: str, gc_download_bucket: str,
                                   release: 'OrcidRelease') -> int:
    s3resource = boto3.resource('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
    modified_records_path = release.modified_records_path

    logging.info(f'Writing modified records to {modified_records_path}')

    # orcid lambda file, containing info on last_modified dates of records
    aws_lambda_bucket = OrcidTelescope.LAMBDA_BUCKET
    aws_lambda_object = OrcidTelescope.LAMBDA_OBJECT

    lambda_obj = s3resource.Object(aws_lambda_bucket, aws_lambda_object)
    test = lambda_obj.get()
    lambda_content = lambda_obj.get()['Body'].read()

    modified_records_count = 0
    # open tar file in memory
    with tarfile.open(fileobj=BytesIO(lambda_content)) as tar, open(modified_records_path, 'w') as f:
        for tar_resource in tar:
            if tar_resource.isfile():
                # extract last modified file in memory
                inner_file_bytes = tar.extractfile(tar_resource).read().decode().split('\n')
                for line in inner_file_bytes[1:]:
                    elements = line.split(',')
                    orcid_record = elements[0]

                    # parse through line by line, check if last_modified timestamp is between start/end date
                    last_modified_date = pendulum.parse(elements[3])
                    # if not end_date:
                    #     end_date = last_modified_date

                    if release.end_date < last_modified_date:
                        continue
                    elif release.start_date <= last_modified_date:
                        directory = orcid_record[-3:]
                        f.write(f"gs://{gc_download_bucket}/{directory}/{orcid_record}.xml" + "\n")
                        modified_records_count += 1
                    else:
                        break
    # TODO remove
    # records_to_download = ['0000-0001-8911-129X', '0000-0003-1168-8720']

    return modified_records_count


# def get_all_record_prefixes(aws_access_key_id: str, aws_secret_access_key: str, start_date: pendulum.Pendulum) -> list:
#     s3client = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
#     # Create the paginator
#     paginator = s3client.get_paginator('list_objects_v2')
#
#     # Create a PageIterator from the Paginator
#     operation_parameters = {
#         'Bucket': OrcidTelescope.SUMMARIES_BUCKET,
#         'PaginationConfig': {
#             'PageSize': 1000
#         }
#     }
#     token_path = OrcidRelease.continuation_token_path(start_date)
#     if os.path.exists(token_path):
#         f = open(token_path, 'r')
#         continuation = f.readline()
#         operation_parameters['ContinuationToken'] = continuation
#
#     page_iterator = paginator.paginate(**operation_parameters)
#
#     page_count = 0
#     all_record_prefixes = []
#     for page in page_iterator:
#         logging.info('Page count: ' + str(page_count))
#         record_prefixes = []
#         for item in page['Contents']:
#             record_prefixes.append(item['Key'])
#         all_record_prefixes += record_prefixes
#
#         continuation_token = None
#         try:
#             continuation_token = page['NextContinuationToken']
#         except KeyError:
#             logging.info('No more continuation tokens')
#         with open(token_path, 'w') as f:
#             f.write(str(continuation_token))
#         page_count += 1
#
#     # delete file with continuation token
#     pathlib.Path(token_path).unlink()
#     return all_record_prefixes


# def get_item_keys(records_to_sync: list, aws_bucket: str, s3resource) -> list:
#     item_keys = []
#     for record in records_to_sync:
#         for item in s3resource.Bucket(aws_bucket).objects.filter(Prefix=record[-3:] + '/' + record):
#             # last_modified_date = pendulum.parse(str(item.last_modified))
#             # if release.prev_start_date > last_modified_date >= release.current_date:
#             item_keys.append(item.key)
#     return item_keys


# def google_transfer_files(aws_access_key_id: str, aws_secret_access_key: str, gc_project_id: str, gc_bucket: str,
#                           record_prefixes: list):
#     aws_to_google_cloud_storage_transfer(aws_access_key_id, aws_secret_access_key,
#                                          aws_bucket=OrcidTelescope.SUMMARIES_BUCKET, include_prefixes=record_prefixes,
#                                          gc_project_id=gc_project_id, gc_bucket=gc_bucket,
#                                          description="Transfer ORCID data from airflow telescope")


def write_boto_config(aws_access_key_id, aws_secret_access_key) -> str:
    boto_config_path = os.path.join(telescope_path(SubFolder.downloaded, OrcidTelescope.DAG_ID), '.boto')

    logging.info(f'Writing boto config file to {boto_config_path}')

    s3_host = "s3.eu-west-1.amazonaws.com"
    template_path = os.path.join(telescope_templates_path(), make_sql_jinja2_filename('boto'))
    rendered = render_template(template_path, s3_host=s3_host, aws_access_key_id=aws_access_key_id,
                               aws_secret_access_key=aws_secret_access_key)
    with open(boto_config_path, 'w') as f:
        f.write(rendered)
    return boto_config_path


def transfer_records(release: 'OrcidRelease') -> Tuple[bool, int]:
    aws_access_conn = BaseHook.get_connection(AirflowConn.orcid.get())
    aws_access_key_id = aws_access_conn.login
    aws_secret_access_key = aws_access_conn.password

    gc_download_bucket = Variable.get(AirflowVar.orcid_bucket_name.get())
    gc_project_id = Variable.get(AirflowVar.project_id.get())

    # print(last_modified_since, last_modified_before)
    success, objects_count = aws_to_google_cloud_storage_transfer(aws_access_key_id, aws_secret_access_key,
                                                                  aws_bucket=OrcidTelescope.SUMMARIES_BUCKET,
                                                                  include_prefixes=[], gc_project_id=gc_project_id,
                                                                  gc_bucket=gc_download_bucket,
                                                                  description="Transfer ORCID data from airflow "
                                                                              "telescope",
                                                                  last_modified_since=release.start_date,
                                                                  last_modified_before=release.end_date)

    # release = OrcidRelease(start_date, end_date, first_release)
    #
    # if first_release:
    #     record_prefixes = get_all_record_prefixes(aws_access_key_id, aws_secret_access_key, start_date)
    # else:
    #     record_prefixes, end_date = get_modified_record_prefixes(aws_access_key_id, aws_secret_access_key, start_date)
    #     # end_date = write_modified_records_list(aws_access_key_id, aws_secret_access_key, start_date)
    #
    # chunk_item_keys = create_chunks(record_prefixes, OrcidTelescope.MAX_PROCESSES)
    #
    # pool = Pool(processes=OrcidTelescope.MAX_PROCESSES)
    # pool.map(partial(google_transfer_files, aws_access_key_id, aws_secret_access_key, gc_project_id, gc_download_bucket),
    #          chunk_item_keys)
    # pool.close()
    # pool.join()

    # boto_config_path = write_boto_config(aws_access_key_id, aws_secret_access_key)
    # old_environ = dict(os.environ)
    # os.environ.update(BOTO_PATH=boto_config_path)
    # try:
    #     if first_release:
    #         gsutil_args = ["gsutil", "-m", "cp", f"s3://{aws_bucket}/060/0000-0003-0776-6060.xml", f"gs://{gc_download_bucket}/"
    #                                                                    f"{release.blob_dir_download}/"]
    #         #TODO uncomment
    #         # gsutil_args = ["gsutil", "-m", "cp", f"s3://{aws_bucket}", f"gs://{gc_download_bucket}/"
    #         #                                                            f"{release.blob_dir_download}/"]
    #         proc: Popen = subprocess.Popen(gsutil_args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=os.environ)
    #     else:
    #         modified_record_keys_path = OrcidRelease.modified_record_keys_path(start_date)
    #         gsutil_args = ["gsutil", "-m", "cp", "-I", f"gs://{gc_download_bucket}/{release.blob_dir_download}/"]
    #         proc: Popen = subprocess.Popen(gsutil_args, stdin=open(modified_record_keys_path),
    #                                        stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=os.environ)
    #     stream_process(proc, True)
    # finally:
    #     os.environ.clear()
    #     os.environ.update(old_environ)
    return success, objects_count


def args_list(args) -> list:
    return args


def download_records(release: 'OrcidRelease'):
    aws_access_conn = BaseHook.get_connection(AirflowConn.orcid.get())
    aws_access_key_id = aws_access_conn.login
    aws_secret_access_key = aws_access_conn.password

    gc_download_bucket = Variable.get(AirflowVar.orcid_bucket_name.get())

    boto_config_path = write_boto_config(aws_access_key_id, aws_secret_access_key)
    old_environ = dict(os.environ)
    os.environ.update(BOTO_PATH=boto_config_path)
    try:
        if release.first_release:
            gsutil_args = args_list(["gsutil", "-m", "cp", f"gs://{gc_download_bucket}/060/0000-0003-0776-6060.xml",
                                     release.download_dir])
            # gsutil_args = ["gsutil", "-m", "cp", f"s3://{gc_download_bucket}/", release.download_dir]
            proc: Popen = subprocess.Popen(gsutil_args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=os.environ)
        else:
            modified_records_count = write_modified_record_prefixes(aws_access_key_id, aws_secret_access_key,
                                                                    gc_download_bucket, release)
            if modified_records_count != release.modified_records_count:
                raise AirflowException(f"Modified records from transfer: {release.modified_records_count}, "
                                       f"modified records from lambda file: {modified_records_count}. These should be "
                                       f"the same.")
            gsutil_args = args_list(["gsutil", "-m", "cp", "-I", release.download_dir])
            proc: Popen = subprocess.Popen(gsutil_args, stdin=open(release.modified_records_path),
                                           stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=os.environ)
        stream_process(proc, True)
    finally:
        os.environ.clear()
        os.environ.update(old_environ)

    # download_blobs_from_cloud_storage(gc_download_bucket, prefix, destination_path)


# def upload_files_per_aws_bucket(download_dir: str, gcp_bucket: str):
#     file_paths = []
#     for dirpath, dirnames, filenames in os.walk(download_dir):
#         file_paths += [os.path.join(dirpath, file) for file in filenames]
#     bucket_date_dir = str(pathlib.Path(*pathlib.Path(download_dir).parts[-2:]))
#     blob_names = [f'telescopes/{OrcidTelescope.DAG_ID}/{bucket_date_dir}/{os.path.relpath(path, download_dir)}'
#                   for path in file_paths]
#     upload_files_to_cloud_storage(gcp_bucket, blob_names, file_paths)


def convert(k):
    if len(k.split(':')) > 1:
        k = k.split(':')[1]
    if k.startswith('@'):
        k = k[1:]
    k = k.replace('-', '_')
    # # Fields must contain only letters, numbers, and underscores, start with a letter or underscore, and be at most
    # # 128 characters long
    # # trim special characters at start
    # k = re.sub('^[^A-Za-z0-9]+', "", k)
    # # replace other special characters (except '_') in remaining string
    # k = re.sub('\W+', '_', k)
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
            if k.startswith('@xmlns'):
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

    # activity_fields = {
    #     'educations': 'education:education',
    #     'employments': 'employment:employment',
    #     'works': 'work:work',
    #     'fundings': 'funding:funding',
    #     'peer-reviews': 'peer-review:peer-review'
    # }
    # pathlib.Path(os.path.dirname(release.transform_path)).mkdir(parents=True, exist_ok=True)
    # since data is appended, delete if old file exists for some reason
    try:
        pathlib.Path(release.transform_path).unlink()
    except FileNotFoundError:
        pass
    # one summary file per record
    for s_root, s_dirs, s_files in os.walk(release.download_dir):
        # for a single record
        for summary_file in s_files:
            # # get corresponding id
            # orcid_id = os.path.splitext(summary_file)[0]
            if not summary_file.endswith('.xml'):
                continue
            # create dict of data in summary xml file
            with open(os.path.join(s_root, summary_file), 'r') as f:
                orcid_dict = xmltodict.parse(f.read())['record:record']

            # # one activity dir per record
            # activity_dir = os.path.join(release.get_download_dir(OrcidTelescope.ACTIVITIES_BUCKET), orcid_id)
            # # for one of the 5 activity categories
            # for a_root, a_dirs, a_files in os.walk(activity_dir):
            #     for activity_file in a_files:
            #         activity = activity_file.split('_')[1]
            #         activity_field = activity_fields[activity]
            #         # create dict of data in activity xml file
            #         with open(os.path.join(a_root, activity_file), 'r') as f:
            #             activity_dict = xmltodict.parse(f.read())[activity_field]
            #         # make list per activity category and append individual activity
            #         try:
            #             orcid_dict[activity_field].append(activity_dict)
            #         except KeyError:
            #             orcid_dict[activity_field] = [activity_dict]

            # change keys & delete some values
            orcid_dict = change_keys(orcid_dict, convert)
            # one dict per orcid record, append to file.
            with open(release.transform_path, 'a') as json_out:
                with jsonlines.Writer(json_out) as writer:
                    writer.write_all([orcid_dict])


def bq_load_partition(release: 'OrcidRelease'):
    # Get variables
    data_location = Variable.get(AirflowVar.data_location.get())
    bucket_name = Variable.get(AirflowVar.transform_bucket_name.get())

    # Select schema file based on release date
    analysis_schema_path = schema_path('telescopes')
    release_date = pendulum.instance(release.end_date)
    schema_file_path = find_schema(analysis_schema_path, OrcidTelescope.DAG_ID, release_date, ver='2.0')
    if schema_file_path is None:
        logging.error(f'No schema found with search parameters: analysis_schema_path={analysis_schema_path}, '
                      f'table_name={OrcidTelescope.DAG_ID}, release_date={release_date}')
        exit(os.EX_CONFIG)

    # Load BigQuery table
    uri = f"gs://{bucket_name}/{release.blob_name}"
    logging.info(f"URI: {uri}")

    # Create partition with records related to release
    table_id = bigquery_partitioned_table_id(OrcidTelescope.PARTITION_TABLE_ID, release_date)

    # Create separate partitioned table
    dataset_id = OrcidTelescope.DATASET_ID
    load_bigquery_table(uri, dataset_id, data_location, table_id, schema_file_path, SourceFormat.NEWLINE_DELIMITED_JSON,
                        partition=True, require_partition_filter=False)


def bq_delete_old_records(start_date: str, end_date: str):
    # Get merge variables
    dataset_id = OrcidTelescope.DATASET_ID
    main_table = OrcidTelescope.MAIN_TABLE_ID
    partitioned_table = OrcidTelescope.PARTITION_TABLE_ID

    merge_condition_field = 'common_orcid_identifier.common_path'
    updated_date_field = 'history_history.common_last_modified_date'

    template_path = os.path.join(telescope_templates_path(), make_sql_jinja2_filename('merge_delete_matched'))
    query = render_template(template_path, dataset=dataset_id, main_table=main_table, partitioned_table=partitioned_table,
                            merge_condition_field=merge_condition_field, start_date=start_date, end_date=end_date,
                            updated_date_field=updated_date_field)
    run_bigquery_query(query)


def bq_append_from_partition(release: 'OrcidRelease', start_date: pendulum.Pendulum, end_date: pendulum.Pendulum):
    # Get variables
    project_id = Variable.get(AirflowVar.project_id.get())
    data_location = Variable.get(AirflowVar.data_location.get())

    # Create dataset
    dataset_id = OrcidTelescope.DATASET_ID
    create_bigquery_dataset(project_id, dataset_id, data_location, OrcidTelescope.DESCRIPTION)

    # Select schema file based on release date
    analysis_schema_path = schema_path('telescopes')
    release_date = pendulum.instance(release.end_date)
    schema_file_path = find_schema(analysis_schema_path, OrcidTelescope.DAG_ID, release_date, ver='2.0')
    if schema_file_path is None:
        logging.error(f'No schema found with search parameters: analysis_schema_path={analysis_schema_path}, '
                      f'table_name={OrcidTelescope.DAG_ID}, release_date={release_date}')
        exit(os.EX_CONFIG)

    period = pendulum.period(start_date, end_date)
    source_table_ids = []
    partition_table_id = OrcidTelescope.PARTITION_TABLE_ID
    main_table_id = OrcidTelescope.MAIN_TABLE_ID
    for dt in period:
        table_id = f"{project_id}.{dataset_id}.{partition_table_id}${dt.strftime('%Y%m%d')}"
        source_table_ids.append(table_id)
    copy_bigquery_table(source_table_ids, main_table_id, data_location, bigquery.WriteDisposition.WRITE_APPEND)


def bq_append_from_file(release: 'OrcidRelease'):
    # Get variables
    project_id = Variable.get(AirflowVar.project_id.get())
    data_location = Variable.get(AirflowVar.data_location.get())
    bucket_name = Variable.get(AirflowVar.transform_bucket_name.get())

    # Create dataset
    dataset_id = OrcidTelescope.DATASET_ID
    create_bigquery_dataset(project_id, dataset_id, data_location, OrcidTelescope.DESCRIPTION)

    # Select schema file based on release date
    analysis_schema_path = schema_path('telescopes')
    release_date = pendulum.instance(release.end_date)
    schema_file_path = find_schema(analysis_schema_path, OrcidTelescope.DAG_ID, release_date, ver='2.0')
    if schema_file_path is None:
        logging.error(f'No schema found with search parameters: analysis_schema_path={analysis_schema_path}, '
                      f'table_name={OrcidTelescope.DAG_ID}, release_date={release_date}')
        exit(os.EX_CONFIG)

    # Load BigQuery table
    uri = f"gs://{bucket_name}/{release.blob_name}"
    logging.info(f"URI: {uri}")

    # Append to table table
    table_id = OrcidTelescope.MAIN_TABLE_ID
    load_bigquery_table(uri, dataset_id, data_location, table_id, schema_file_path, SourceFormat.NEWLINE_DELIMITED_JSON,
                        write_disposition=bigquery.WriteDisposition.WRITE_APPEND)


def cleanup_dirs(release: 'OrcidRelease'):
    try:
        shutil.rmtree(release.subdir(SubFolder.downloaded))
    except FileNotFoundError as e:
        logging.warning(f"No such file or directory {release.subdir(SubFolder.downloaded)}: {e}")

    try:
        shutil.rmtree(release.subdir(SubFolder.transformed))
    except FileNotFoundError as e:
        logging.warning(f"No such file or directory {release.subdir(SubFolder.transformed)}: {e}")


class OrcidRelease:
    """ Used to store info on a given crossref release """

    def __init__(self, start_date: pendulum.Pendulum, end_date: pendulum.Pendulum, first_release: bool = False):
        self.start_date = start_date
        self.end_date = end_date
        self.first_release = first_release
        self.modified_records_count = 0

    @property
    def lambda_file_path(self) -> str:
        return self.get_path(SubFolder.downloaded, "last_modified.csv.tar")

    @property
    def modified_records_path(self) -> str:
        return self.get_path(SubFolder.downloaded, "modified_records.txt")

    @property
    def continuation_token_path(self) -> str:
        return self.get_path(SubFolder.downloaded, "continuation_token.txt")

    # @property
    # def download_path(self) -> str:
    #     """
    #     :return: The file path for the downloaded crossref events
    #     """
    #     return self.get_path(SubFolder.downloaded, OrcidTelescope.DAG_ID, "json")

    @property
    def download_dir(self) -> str:
        download_dir = self.subdir(SubFolder.downloaded)
        if not os.path.exists(download_dir):
            os.makedirs(download_dir, exist_ok=True)

        return download_dir

    @property
    def transform_path(self) -> str:
        date_str = self.start_date.strftime("%Y_%m_%d") + "-" + self.end_date.strftime("%Y_%m_%d")

        file_name = f"{OrcidTelescope.DAG_ID}_{date_str}.json"
        return self.get_path(SubFolder.transformed, file_name)

    # def get_continuation_token_path(self, bucket: str) -> str:
    #     return self.get_path(SubFolder.downloaded, f"continuation_token_{bucket}", "txt")


    # def get_download_dir(self, bucket: str) -> str:
    #     date_str = self.end_date.strftime("%Y_%m_%d")
    #     path = os.path.join(telescope_path(SubFolder.downloaded, OrcidTelescope.DAG_ID), date_str, bucket)
    #     return path

    # @property
    # def blob_dir_download(self) -> str:
    #     """
    #     Returns blob name that is used to determine path inside google cloud storage bucket
    #     :return: The blob name
    #     """
    #     date_str = self.start_date.strftime("%Y_%m_%d") + "-" + self.end_date.strftime("%Y_%m_%d")
    #
    #     blob_name = f'telescopes/{OrcidTelescope.DAG_ID}/{date_str}'
    #     return blob_name

    @property
    def blob_name(self) -> str:
        """
        Returns blob name that is used to determine path inside google cloud storage bucket
        :return: The blob name
        """
        date_str = self.start_date.strftime("%Y_%m_%d") + "-" + self.end_date.strftime("%Y_%m_%d")

        file_name = f"{OrcidTelescope.DAG_ID}_{date_str}.json"
        blob_name = f'telescopes/{OrcidTelescope.DAG_ID}/{file_name}'

        return blob_name

    def subdir(self, sub_folder: SubFolder):
        """
        Path to subdirectory of a specific release for either downloaded/transformed files.
        :param sub_folder:
        :return:
        """
        date_str = self.start_date.strftime("%Y_%m_%d") + "-" + self.end_date.strftime("%Y_%m_%d")
        return os.path.join(telescope_path(sub_folder, OrcidTelescope.DAG_ID), date_str)

    def get_path(self, sub_folder: SubFolder, name: str) -> str:
        """
        Gets path to file based on subfolder and name. Will also create the subfolder if it doesn't exist yet.
        # :param start_date: Start date of this release
        :param sub_folder: Name of the subfolder
        :param name: File base name including extension
        :return: The file path.
        """
        release_subdir = self.subdir(sub_folder)
        if not os.path.exists(release_subdir):
            os.makedirs(release_subdir, exist_ok=True)

        path = os.path.join(release_subdir, name)
        return path


def pull_release(ti: TaskInstance) -> OrcidRelease:
    return ti.xcom_pull(key=OrcidTelescope.RELEASES_XCOM, task_ids=OrcidTelescope.TASK_ID_TRANSFER,
                        include_prior_dates=False)


def pull_last_modified(ti: TaskInstance) -> pendulum.Pendulum:
    return ti.xcom_pull(key=OrcidTelescope.LAST_MODIFIED_XCOM, task_ids=OrcidTelescope.TASK_ID_TRANSFER,
                        include_prior_dates=True)


def push_last_modified(ti: TaskInstance, start_date: pendulum.Pendulum):
    ti.xcom_push(key=OrcidTelescope.LAST_MODIFIED_XCOM, value=start_date)


class OrcidTelescope:
    """ A container for holding the constants and static functions for the crossref metadata telescope. """

    DAG_ID = 'orcid'
    DATASET_ID = 'orcid'
    MAIN_TABLE_ID = 'orcid'
    PARTITION_TABLE_ID = 'orcid_partitions'
    DESCRIPTION = ''
    RELEASES_XCOM = "releases"
    LAST_MODIFIED_XCOM = "start_date"
    QUEUE = 'remote_queue'
    MAX_RETRIES = 3
    MAX_PROCESSES = cpu_count()
    SUMMARIES_BUCKET = 'v2.0-summaries'
    LAMBDA_BUCKET = 'orcid-lambda-file'
    LAMBDA_OBJECT = 'last_modified.csv.tar'

    TELESCOPE_URL = 'https://api.eventdata.crossref.org/v1/events?mailto={mail_to}&' \
                    'from-occurred-date={start_date}&until-occurred-date={end_date}&rows=10000'
    # TODO create debug file
    # DEBUG_FILE_PATH = os.path.join(test_fixtures_path(), 'telescopes', 'crossref_metadata.json.tar.gz')

    TASK_ID_CHECK_DEPENDENCIES = "check_dependencies"
    TASK_ID_CHECK_RELEASE = "check_release"
    TASK_ID_TRANSFER = "transfer"
    TASK_ID_DOWNLOAD = "download"
    TASK_ID_EXTRACT = "extract"
    TASK_ID_TRANSFORM = "transform_releases"
    TASK_ID_UPLOAD_TRANSFORMED = "upload_transformed"
    TASK_ID_BQ_LOAD_PARTITION = "bq_load_partition"
    TASK_ID_CLEANUP = "cleanup"
    TASK_ID_BQ_DELETE_OLD = "bq_delete_old"
    TASK_ID_BQ_APPEND_NEW = "bq_append_new"

    @staticmethod
    def check_dependencies():
        """ Check that all variables exist that are required to run the DAG.

        :return: None.
        """

        vars_valid = check_variables(AirflowVar.data_path.get(), AirflowVar.project_id.get(),
                                     AirflowVar.data_location.get(), AirflowVar.orcid_bucket_name.get(),
                                     AirflowVar.transform_bucket_name.get())
        conns_valid = check_connections(AirflowConn.orcid.get())

        if not vars_valid or not conns_valid:
            raise AirflowException('Required variables or connections are missing')
        orcid_bucket_name = Variable.get(AirflowVar.orcid_bucket_name.get())
        if not storage_bucket_exists(orcid_bucket_name):
            raise AirflowException(f'Bucket to store Orcid ({orcid_bucket_name}) download data does '
                                   f'not exist')

    @staticmethod
    def transfer(**kwargs):
        ti: TaskInstance = kwargs['ti']

        # prev_start_date = kwargs['prev_start_date_success']
        # # if DAG is run for first time, set to start date of this DAG (note: different than start date of DAG run)
        # if prev_start_date:
        #     first_release = False
        # else:
        #     first_release = True
        #     prev_start_date = kwargs['dag'].default_args['start_date'].date()
        # start_date = (pendulum.instance(kwargs['dag_run'].start_date) - timedelta(days=1)).date()
        first_release = False
        start_date = pull_last_modified(ti)
        if not start_date:
            first_release = True
            start_date = kwargs['dag'].default_args['start_date'].date()
            #TODO uncomment
            start_date = pendulum.parse('2020-10-11 11:48:23.795099+00:00')
        end_date = pendulum.utcnow()
        logging.info(f'Start date: {start_date}, end date: {end_date}, first release: {first_release}')

        release = OrcidRelease(start_date, end_date, first_release)

        success, objects_count = transfer_records(release)
        release.modified_records_count = objects_count

        ti.xcom_push(OrcidTelescope.RELEASES_XCOM, release)
        ti.xcom_push(OrcidTelescope.LAST_MODIFIED_XCOM, end_date)

        if success and objects_count > 0:
            return True
        else:
            return False

    @staticmethod
    def download(**kwargs):
        ti: TaskInstance = kwargs['ti']
        release = pull_release(ti)

        download_records(release)

    # @staticmethod
    # def upload_downloaded(**kwargs):
    #     """ Upload the downloaded files to a Google Cloud Storage bucket.
    #
    #     :param kwargs: the context passed from the PythonOperator. See
    #     https://airflow.apache.org/docs/stable/macros-ref.html
    #     for a list of the keyword arguments that are passed to this argument.
    #     :return: None.
    #     """
    #
    #     ti: TaskInstance = kwargs['ti']
    #     release = pull_release(ti)
    #
    #     gcp_bucket = Variable.get(AirflowVar.download_bucket_name.get())
    #
    #     upload_files_per_aws_bucket(release.get_download_dir(OrcidTelescope.ACTIVITIES_BUCKET), gcp_bucket)
    #     upload_files_per_aws_bucket(release.get_download_dir(OrcidTelescope.SUMMARIES_BUCKET), gcp_bucket)

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

        upload_file_to_cloud_storage(bucket_name, release.blob_name, release.transform_path)

    @staticmethod
    def bq_load_partition(**kwargs):
        """
        Create a table shard containing only records of this release. The date in the table name is based on the end
        date of this release.
        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None
        """
        # Pull release
        ti: TaskInstance = kwargs['ti']
        release = pull_release(ti)

        if release.first_release:
            raise AirflowSkipException('Skipped, because first release')

        bq_load_partition(release)

    # @staticmethod
    # def bq_load(**kwargs):
    #     """ Upload transformed release to a bigquery table.
    #
    #     :param kwargs: the context passed from the PythonOperator. See
    #     https://airflow.apache.org/docs/stable/macros-ref.html
    #     for a list of the keyword arguments that are passed to this argument.
    #     :return: None.
    #     """
    #
    #     # Pull releases
    #     ti: TaskInstance = kwargs['ti']
    #     release = pull_release(ti)
    #
    #     # Get variables
    #     project_id = Variable.get(AirflowVar.project_id.get())
    #     data_location = Variable.get(AirflowVar.data_location.get())
    #     bucket_name = Variable.get(AirflowVar.transform_bucket_name.get())
    #
    #     # Create dataset
    #     dataset_id = OrcidTelescope.DATASET_ID
    #     create_bigquery_dataset(project_id, dataset_id, data_location, OrcidTelescope.DESCRIPTION)
    #
    #     # Select schema file based on release date
    #     analysis_schema_path = schema_path('telescopes')
    #     schema_file_path = find_schema(analysis_schema_path, OrcidTelescope.DAG_ID, release.end_date)
    #     if schema_file_path is None:
    #         logging.error(f'No schema found with search parameters: analysis_schema_path={analysis_schema_path}, '
    #                       f'table_name={OrcidTelescope.DAG_ID}, release_date={release.end_date}')
    #         exit(os.EX_CONFIG)
    #
    #     # Load BigQuery table
    #     uri = f"gs://{bucket_name}/{release.blob_name}"
    #     logging.info(f"URI: {uri}")
    #
    #     load_bigquery_table(uri, dataset_id, data_location, OrcidTelescope.DAG_ID, schema_file_path,
    #                         SourceFormat.NEWLINE_DELIMITED_JSON,
    #                         write_disposition=bigquery.WriteDisposition.WRITE_APPEND)

    @staticmethod
    def bq_delete_old(**kwargs):
        """
        Run a BigQuery MERGE query, merging the sharded table of this release into the main table containing all
        events.
        The tables are matched on the 'id' field and if a match occurs, a check will be done to determine whether the
        'updated date' of the corresponding event in the main table either does not exist or is older than that of the
        event in the sharded table. If this is the case, the event will be deleted from the main table.
        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None
        """
        # Pull releases
        ti: TaskInstance = kwargs['ti']
        release = pull_release(ti)

        if release.first_release:
            logging.info('Skipped, because first release')
            return
        elif datetime.now().weekday() != 5:
            raise AirflowSkipException('Skipped, only delete old records once a week on Sunday')

        start_date = (ti.previous_start_date_success + timedelta(days=1)).strftime("%Y-%m-%d")
        end_date = ti.start_date.strftime("%Y-%m-%d")

        bq_delete_old_records(start_date, end_date)

    @staticmethod
    def bq_append_new(**kwargs):
        """
        All events from this release in the corresponding table shard will be appended to the main table.
        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        # Pull release
        ti: TaskInstance = kwargs['ti']
        release = pull_release(ti)

        if release.first_release:
            bq_append_from_file(release)
        elif datetime.now().weekday() != 5:
            raise AirflowSkipException('Skipped, not first release and only append new records once a week on Sunday')
        else:
            start_date = pendulum.instance(ti.previous_start_date_success + timedelta(days=1))
            end_date = pendulum.instance(ti.start_date)

            bq_append_from_partition(release, start_date, end_date)

    @staticmethod
    def cleanup(**kwargs):
        """
        Delete subdirectories for downloaded and transformed events files.
        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        # Pull release
        ti: TaskInstance = kwargs['ti']
        release = pull_release(ti)

        cleanup_dirs(release)

