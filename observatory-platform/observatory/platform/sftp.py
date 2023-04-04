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


import os
from base64 import b64decode
from typing import Union

import paramiko
import pysftp
from airflow.hooks.base import BaseHook


def make_sftp_connection(sftp_conn_id: str) -> pysftp.Connection:
    """Create a SFTP connection using credentials from the Airflow sftp_conn_id connection.

    :param sftp_conn_id: the SFTP Airflow Connection ID.
    :return: SFTP connection
    """
    conn = BaseHook.get_connection(sftp_conn_id)
    host = conn.host

    # Add public host key
    public_key = conn.extra_dejson.get("host_key", None)
    if public_key is not None:
        key = paramiko.RSAKey(data=b64decode(public_key))
        cnopts = pysftp.CnOpts()
        cnopts.hostkeys.add(host, "ssh-rsa", key)
    else:
        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None

    # set up connection
    return pysftp.Connection(host, port=conn.port, username=conn.login, password=conn.password, cnopts=cnopts)


class SftpFolders:
    def __init__(self, dag_id: str, sftp_conn_id: str, sftp_root: str = "/"):
        """Initialise SftpFolders.

        :param dag_id: the dag id (namespace + organisation name)
        :param sftp_conn_id: TODO
        :param sftp_root: optional root to be added to sftp home path
        """
        self.dag_id = dag_id
        self.sftp_conn_id = sftp_conn_id
        self.sftp_root = sftp_root

    @property
    def sftp_home(self) -> str:
        """Make the SFTP home folder for an organisation.

        :return: the path to the folder.
        """

        return os.path.join(self.sftp_root, "workflows", self.dag_id)

    @property
    def upload(self) -> str:
        """The organisation's SFTP upload folder.

        :return: path to folder.
        """
        return os.path.join(self.sftp_home, "upload")

    @property
    def in_progress(self) -> str:
        """The organisation's SFTP in_progress folder.

        :return: path to folder.
        """
        return os.path.join(self.sftp_home, "in_progress")

    @property
    def finished(self) -> str:
        """The organisation's SFTP finished folder.

        :return: path to folder.
        """
        return os.path.join(self.sftp_home, "finished")

    def move_files_to_in_progress(self, upload_files: Union[list, str]):
        """Move files in list from upload to in-progress folder.

        :param upload_files: File name or list of file names that are in the upload folder and will be moved to the
        in_progress folder (can be full path or just file name)
        :return: None.
        """

        print(f"Files are: {upload_files}")

        if isinstance(upload_files, str):
            upload_files = [upload_files]

        with make_sftp_connection(self.sftp_conn_id) as sftp:
            sftp.makedirs(self.in_progress)
            for file in upload_files:
                file_name = os.path.basename(file)
                upload_file = os.path.join(self.upload, file_name)
                in_progress_file = os.path.join(self.in_progress, file_name)
                sftp.rename(upload_file, in_progress_file)

    def move_files_to_finished(self, in_progress_files: Union[list, str]):
        """Move files in list from in_progress to finished folder.

        :param in_progress_files: File name or list of file names that are in the in_progress folder and will be moved
        to the finished folder (can be full path or just file name)
        :return: None.
        """
        if isinstance(in_progress_files, str):
            in_progress_files = [in_progress_files]

        with make_sftp_connection(self.sftp_conn_id) as sftp:
            sftp.makedirs(self.finished)
            for file in in_progress_files:
                file_name = os.path.basename(file)
                in_progress_file = os.path.join(self.in_progress, file_name)
                finished_file = os.path.join(self.finished, file_name)
                sftp.rename(in_progress_file, finished_file)
