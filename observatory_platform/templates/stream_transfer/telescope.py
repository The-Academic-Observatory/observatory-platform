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

"""
A short summary on the telescope.
"""

from datetime import datetime
from types import SimpleNamespace
import jsonlines
import logging
import multiprocessing
import os
import pathlib
import pendulum
import subprocess
import xmltodict
from functools import partial
from subprocess import Popen
from airflow.hooks.base_hook import BaseHook
from airflow.exceptions import AirflowException
from airflow.models import Variable

from observatory_platform.utils.telescope_stream import (StreamTelescope, StreamRelease)
from observatory_platform.utils.config_utils import (AirflowConn,
                                                     AirflowVar,
                                                     SubFolder,
                                                     telescope_path)
from observatory_platform.utils.gc_utils import (aws_to_google_cloud_storage_transfer, storage_bucket_exists)
from observatory_platform.utils.telescope_utils import write_boto_config, args_list, change_keys, convert
from observatory_platform.utils.airflow_utils import AirflowVariable as Variable


class ExampleRelease(StreamRelease):
    """ Used to store info on a given release. """
    pass
    # @property
    # def lambda_path(self) -> str:
    #     return self.get_path(SubFolder.downloaded, "lambda_path", "txt")
    #
    # @property
    # def modified_records_path(self) -> str:
    #     return self.get_path(SubFolder.downloaded, "modified_records", "txt")


# def check_dependencies(kwargs):
#     """ Check whether the extra bucket to store data in exists.
#
#     :param kwargs: The context passed from the PythonOperator.
#     :return: None.
#     """
#     extra_bucket_name = Variable.get(AirflowVar.extra_bucket.get())
#     if not storage_bucket_exists(extra_bucket_name):
#         raise AirflowException(f'Extra bucket does not exist ({extra_bucket_name})')


# def create_release(start_date: pendulum.Pendulum, end_date: pendulum.Pendulum, telescope: SimpleNamespace,
#                    first_release: bool = False) -> ExampleRelease:
#     """ Create a release instance.
#
#     :param start_date: Start date of this run
#     :param end_date: End date of this run
#     :param telescope: Contains telescope properties
#     :param first_release: Whether this is the first release to be obtained
#     :return: Release instance
#     """
#     release = ExampleRelease(start_date, end_date, telescope, first_release)
#     return release


def transfer(release: ExampleRelease) -> bool:
    """ Transfer the release data from one bucket to a Google Cloud bucket.

    :param release: Release instance.
    :return: True if data is available and successfully transferred, else False.
    """
    return True
    # aws_access_conn = BaseHook.get_connection(AirflowConn.example.get())
    # aws_access_key_id = aws_access_conn.login
    # aws_secret_access_key = aws_access_conn.password
    #
    # gc_download_bucket = Variable.get(AirflowVar.extra_bucket.get())
    # gc_project_id = Variable.get(AirflowVar.project_id.get())
    # last_modified_since = None if release.first_release else release.start_date
    # success = False
    # total_count = 0
    # for i in range(release.telescope.max_retries):
    #     success, objects_count = aws_to_google_cloud_storage_transfer(aws_access_key_id, aws_secret_access_key,
    #                                                                   aws_bucket=release.telescope.summaries_bucket,
    #                                                                   include_prefixes=[], gc_project_id=gc_project_id,
    #                                                                   gc_bucket=gc_download_bucket,
    #                                                                   description="Example transfer telescope",
    #                                                                   last_modified_since=last_modified_since,
    #                                                                   last_modified_before=release.end_date)
    #     total_count += objects_count
    #
    # if not success:
    #     raise AirflowException(f'Google Storage Transfer unsuccessful, status: {success}')
    # logging.info(f'Total number of objects transferred: {total_count}')
    # return True if success and total_count > 0 else False


def download_transferred(release: ExampleRelease):
    """ Download the transferred release data to disk.

    :param release: Release instance.
    :return: None
    """
    pass
    # aws_access_conn = BaseHook.get_connection(AirflowConn.example.get())
    # aws_access_key_id = aws_access_conn.login
    # aws_secret_access_key = aws_access_conn.password
    #
    # gc_download_bucket = Variable.get(AirflowVar.extra_bucket.get())
    # boto_config_path = os.path.join(telescope_path(SubFolder.downloaded, release.telescope.dag_id), '.boto')
    # s3_host = "s3.eu-west-1.amazonaws.com"
    # write_boto_config(s3_host, aws_access_key_id, aws_secret_access_key, boto_config_path)
    #
    # old_environ = dict(os.environ)
    # os.environ.update(BOTO_PATH=boto_config_path)
    # try:
    #     if release.first_release:
    #         gsutil_args = args_list(["gsutil", "-m", "cp", "-r", f"gs://{gc_download_bucket}", release.download_dir])
    #         proc: Popen = subprocess.Popen(gsutil_args, env=os.environ)
    #     else:
    #         write_modified_record_prefixes(gc_download_bucket, release)
    #         gsutil_args = args_list(["gsutil", "-m", "cp", "-I", release.download_dir])
    #         proc: Popen = subprocess.Popen(gsutil_args, stdin=open(release.modified_records_path),
    #                                        stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=os.environ)
    #     try:
    #         out, err = proc.communicate(timeout=15)
    #     except subprocess.TimeoutExpired:
    #         out, err = proc.communicate()
    #
    # finally:
    #     os.environ.clear()
    #     os.environ.update(old_environ)


def transform(release: ExampleRelease):
    """ Transform release.

    :param release: Release instance.
    :return: None.
    """
    pass
    # try:
    #     pathlib.Path(release.transform_path).unlink()
    # except FileNotFoundError:
    #     pass
    # for s_root, s_dirs, s_files in os.walk(release.download_dir):
    #     transform_single_file_partial = partial(transform_single_file, s_root)
    #     pool = multiprocessing.Pool()
    #     with open(release.transform_path, 'a') as json_out:
    #         for orcid_dict in pool.imap(transform_single_file_partial, s_files):
    #             if not orcid_dict:
    #                 continue
    #             with jsonlines.Writer(json_out) as writer:
    #                 writer.write_all([orcid_dict])
    #     # prevent sigterm error airflow
    #     pool.close()
    #     pool.join()


class ExampleTelescope(StreamTelescope):
    """ A container for holding the constants and static functions of this telescope. """
    telescope = SimpleNamespace(dag_id='stream_transfer',
                                schedule_interval='@weekly',
                                start_date=datetime(2012, 1, 1),
                                queue='remote_queue',
                                max_retries=3,
                                description='The description',
                                dataset_id='example', main_table_id='example', partition_table_id='example_partitions',
                                merge_partition_field='example.id', updated_date_field='example.last_modified_date',
                                bq_merge_days=7,
                                schema_version='3.0',
                                download_ext='json', extract_ext='json', transform_ext='jsonl',
                                airflow_vars=[AirflowVar.data_path.get(), AirflowVar.project_id.get(),
                                              AirflowVar.data_location.get(), AirflowVar.transform_bucket_name.get()],
                                # airflow_vars=[AirflowVar.data_path.get(), AirflowVar.project_id.get(),
                                #               AirflowVar.data_location.get(), AirflowVar.transform_bucket_name.get(),
                                #               AirflowVar.extra_bucket.get()],
                                airflow_conns=[],
                                # airflow_conns=[AirflowVar.example.get()],
                                # summaries_bucket='v2.0-summaries',
                                )

    # optional
    # check_dependencies_custom = check_dependencies
    # create_release_custom = create_release

    # required
    transfer_custom = transfer
    download_transferred_custom = download_transferred
    transform_custom = transform


# def write_modified_record_prefixes(gc_download_bucket: str, release: ExampleRelease):
#     """ Write uris of modified records to a file.
#
#     :param gc_download_bucket: Google Cloud bucket to store data.
#     :param release: Release instance.
#     :return:
#     """
#     with open(release.lambda_path, 'r') as f_in, open(release.modified_records_path, 'w') as f_out:
#         for line in f_in:
#             # parse through line by line, check if last_modified timestamp is between start/end date
#             elements = line.split('\t')
#             last_modified_date = pendulum.parse(elements[0])
#             record = elements[1]
#
#             if release.end_date < last_modified_date:
#                 continue
#             elif release.start_date <= last_modified_date:
#                 f_out.write(f"gs://{gc_download_bucket}/{record}\n")
#             else:
#                 break


# def transform_single_file(root_path: str, file_path: str) -> dict:
#     """ Transform dictionary of a single record file.
#
#     :param root_path: Root path.
#     :param file_path: File path.
#     :return: Dictionary with records.
#     """
#     with open(os.path.join(root_path, file_path), 'r') as f:
#         example_dict = xmltodict.parse(f.read())
#
#     example_dict = change_keys(example_dict, convert)
#     return example_dict
