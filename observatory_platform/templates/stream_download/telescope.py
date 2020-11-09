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

import json
import jsonlines
import logging
import os
import pathlib
import pendulum
import xmltodict
from types import SimpleNamespace
from typing import Tuple, Union
from airflow.exceptions import AirflowException
from airflow.models import Variable
from requests.exceptions import RetryError

from observatory_platform.utils.config_utils import (AirflowVar,
                                                     SubFolder)
from observatory_platform.utils.gc_utils import storage_bucket_exists
from observatory_platform.utils.telescope_stream import (StreamTelescope, StreamRelease)
from observatory_platform.utils.url_utils import retry_session, get_ao_user_agent
from observatory_platform.utils.telescope_utils import change_keys, convert


class ExampleRelease(StreamRelease):
    """ Used to store info on a given release. """
    pass
    # @property
    # def token_path(self) -> str:
    #     return self.get_path(SubFolder.downloaded, "continuation_token", "txt")

#
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


def download(release: ExampleRelease) -> bool:
    """ Download the release data to local disk.

    :param release: Release instance.
    :return: True if data is available and successfully downloaded, else False.
    """
    return True
    # logging.info('Downloading data')
    # url = release.telescope.url.format(start_date=release.start_date.strftime("%Y-%m-%d"),
    #                                    end_date=release.end_date.strftime("%Y-%m-%d"))
    # # check if cursor files exist from a previous failed request
    # if os.path.isfile(release.token_path):
    #     # retrieve token
    #     with open(release.token_path, 'r') as f:
    #         next_token = json.load(f)
    #     # delete file
    #     pathlib.Path(release.token_path).unlink()
    #     # extract entries
    #     success, next_token, total_entries = extract_entries(url, release.download_path, next_token)
    # # if entries download path exists but no token file, the previous request has finished & successful
    # elif os.path.isfile(release.download_path):
    #     success = True
    #     next_token = None
    #     total_entries = 'See previous successful attempt'
    # # first time request
    # else:
    #     # extract events
    #     success, next_token, total_entries = extract_entries(url, release.download_path)
    #
    # if not success:
    #     with open(release.token_path, 'w') as f:
    #         json.dump(next_token, f)
    #     raise AirflowException(f'Download unsuccessful, status: {success}')
    #
    # logging.info(f'Download successful, total entries={total_entries}')
    # return True if success and total_entries != '0' else False


def transform(release: ExampleRelease):
    """ Transform release.

    :param release: Release instance.
    :return: None.
    """
    pass
    # with open(release.download_path, 'r') as f_in:
    #     data_dict = json.load(f_in)
    # # change keys
    # data_dict = change_keys(data_dict, convert)
    #
    # # one dict per orcid record, append to file.
    # with open(release.transform_path, 'w') as f_out:
    #     json.dump(data_dict, f_out)


class ExampleTelescope(StreamTelescope):
    """ A container for holding the constants and static functions of this telescope. """
    telescope = SimpleNamespace(dag_id='stream_download',
                                queue='remote_queue',
                                max_retries=3,
                                description='The description',
                                dataset_id='example', main_table_id='example', partition_table_id='example_partitions',
                                merge_partition_field='example.id', updated_date_field='example.last_modified_date',
                                bq_merge_days=7,
                                schema_version='3.0',
                                download_ext='json', extract_ext='json', transform_ext='jsonl',
                                airflow_vars=[AirflowVar.data_path.get(), AirflowVar.project_id.get(),
                                              AirflowVar.data_location.get(), AirflowVar.download_bucket_name.get(),
                                              AirflowVar.transform_bucket_name.get()],
                                airflow_conns=[],
                                # url='https://example_url.org/entries?from={start_date}&until={end_date}'
                                )

    # optional
    # check_dependencies_custom = check_dependencies
    # create_release_custom = create_release

    # required
    download_custom = download
    transform_custom = transform

# def extract_entries(url: str, entries_path: str, next_token: str = None, success: bool = True) -> \
#         Tuple[bool, Union[str, None], Union[str, None]]:
#     """
#     Extract the entries from the given url until no new resumption token is returned or a RetryError occurs. The
#     extracted entries are appended to a json file, with 1 list per request.
#     :param url: The telescope url
#     :param entries_path: Path to the file in which entries are stored
#     :param next_token: The next resumption token, this is in the response of the api
#     :param success: Whether all entries were extracted successfully or an error occurred
#     :return: success, next_cursor and number of total entries
#     """
#     headers = {
#         'User-Agent': f'{get_ao_user_agent()}'
#     }
#
#     tmp_url = url + f'&resumptionToken={next_token}' if next_token else url
#     try:
#         response = retry_session().get(tmp_url, headers=headers)
#     except RetryError:
#         return False, next_token, None
#     if response.status_code == 200:
#         response_dict = xmltodict.parse(response.content.decode('utf-8'))
#         total_entries = response_dict['TotalEntries']
#         entries = response_dict['ListEntries']
#         try:
#             next_token = response_dict['ResumptionToken']
#         except KeyError:
#             next_token = None
#         # write entries so far
#         with open(entries_path, 'a') as f:
#             with jsonlines.Writer(f) as writer:
#                 writer.write_all([entries])
#         if next_token:
#             success, next_token, total_entries = extract_entries(url, entries_path, next_token, success)
#             return success, next_token, total_entries
#         else:
#             return True, None, total_entries
#     else:
#         raise ConnectionError(f"Error requesting url: {url}, response: {response.text}")
