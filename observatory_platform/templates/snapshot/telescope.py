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
import jsonlines
import logging
import os
import pathlib
import pendulum
import subprocess
import xmltodict
from types import SimpleNamespace
from typing import Tuple, Union
from airflow.exceptions import AirflowException
from airflow.models import Variable
from requests.exceptions import RetryError

from observatory_platform.utils.config_utils import (AirflowVar,
                                                     SubFolder)
from observatory_platform.utils.data_utils import get_file
from observatory_platform.utils.gc_utils import storage_bucket_exists
from observatory_platform.utils.telescope_snapshot import (SnapshotTelescope, SnapshotRelease)
from observatory_platform.utils.telescope_utils import change_keys, convert
from observatory_platform.utils.url_utils import retry_session, get_ao_user_agent
from observatory_platform.utils.proc_utils import wait_for_process


class ExampleRelease(SnapshotRelease):
    """ Used to store info on a given release. """
    pass
    # @property
    # def url(self) -> str:
    #     return f'{self.telescope.url}/{self.date_str}'


# def check_dependencies(kwargs):
#     """ Check whether the extra bucket to store data in exists.
#
#     :param kwargs: The context passed from the PythonOperator.
#     :return: None.
#     """
#     extra_bucket_name = Variable.get(AirflowVar.extra_bucket.get())
#     if not storage_bucket_exists(extra_bucket_name):
#         raise AirflowException(f'Extra bucket does not exist ({extra_bucket_name})')


def list_releases(start_date: pendulum.Pendulum, end_date: pendulum.Pendulum, telescope: SimpleNamespace) -> list:
    """ List releases that are available between the start and end date.

    :param start_date: Start date of this run
    :param end_date: End date of this run
    :param telescope: Contains telescope properties
    :return: List of releases
    """
    releases = []
    release_date = pendulum.parse('2020-02-01')
    if start_date <= release_date < end_date:
        release = SnapshotRelease(None, release_date, telescope)
        releases.append(release)
    return releases


def download(release: ExampleRelease):
    """ Download the release data to local disk.

    :param release: Release instance.
    :return: None.
    """
    pass
    # logging.info(f"Downloading file: {release.download_path}, url: {release.url}")
    # file_path, updated = get_file(fname=release.download_path, origin=release.url, cache_dir=release.download_dir)
    #
    # if file_path:
    #     logging.info(f'Success downloading release.')
    # else:
    #     logging.error(f"Error downloading release.")
    #     exit(os.EX_DATAERR)
    #


def extract(release: ExampleRelease):
    """ Extract release.

    :param release: Release instance.
    :return: None.
    """
    pass
    # logging.info(f"Extracting file: {release.download_path}")
    #
    # cmd = f"gunzip -c {release.download_path} > {release.extract_path}"
    # p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, executable='/bin/bash')
    #
    # stdout, stderr = wait_for_process(p)
    # if stdout:
    #     logging.info(stdout)
    # if stderr:
    #     raise AirflowException(f"Error executing command {cmd}: {stderr}")
    #
    # logging.info(f"File extracted to: {release.extract_path}")


def transform(release: ExampleRelease):
    """ Transform release.

    :param release: Release instance.
    :return: None.
    """
    pass
    # with open(release.extract_path, 'r') as f_in:
    #     data_dict = json.load(f_in)
    #
    # # change keys
    # data_dict = change_keys(data_dict, convert)
    #
    # # one dict per orcid record, append to file.
    # with open(release.transform_path, 'w') as f_out:
    #     json.dump(data_dict, f_out)


class ExampleTelescope(SnapshotTelescope):
    """ A container for holding the constants and static functions of this telescope. """
    telescope = SimpleNamespace(dag_id='snapshot_download',
                                queue='remote_queue',
                                max_retries=3,
                                description='The description',
                                dataset_id='example', table_id='example',
                                schema_version='3.0',
                                download_ext='json', extract_ext='json', transform_ext='jsonl',
                                airflow_vars=[AirflowVar.data_path.get(), AirflowVar.project_id.get(),
                                              AirflowVar.data_location.get(), AirflowVar.download_bucket_name.get(),
                                              AirflowVar.transform_bucket_name.get()],
                                airflow_conns=[],
                                # url='https://example_url_snapshots.com'
                                )

    # optional
    # check_dependencies_custom = check_dependencies

    # required
    list_releases_custom = list_releases
    download_custom = download
    extract_custom = extract
    transform_custom = transform
