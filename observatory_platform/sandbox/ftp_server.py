# Copyright 2021-2024 Curtin University
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

# Author: Alex Massen-Hane

import contextlib
import threading
import time

from pyftpdlib.authorizers import DummyAuthorizer
from pyftpdlib.handlers import FTPHandler
from pyftpdlib.servers import ThreadedFTPServer


class FtpServer:
    """
    Create a Mock FTPServer instance.

    :param directory: The directory that is hosted on FTP server.
    :param host: Hostname of the server.
    :param port: The port number.
    :param startup_wait_secs: time in seconds to wait before returning from create to give the server enough
    time to start before connecting to it.
    """

    def __init__(
        self,
        directory: str = "/",
        host: str = "localhost",
        port: int = 21,
        startup_wait_secs: int = 1,
        root_username: str = "root",
        root_password: str = "pass",
    ):
        self.host = host
        self.port = port
        self.directory = directory
        self.startup_wait_secs = startup_wait_secs

        self.root_username = root_username
        self.root_password = root_password

        self.is_shutdown = True
        self.server_thread = None

    @contextlib.contextmanager
    def create(self):
        """Make and destroy a test FTP server.

        :yield: self.directory.
        """

        # Set up the FTP server with root and anonymous users.
        authorizer = DummyAuthorizer()
        authorizer.add_user(
            username=self.root_username, password=self.root_password, homedir=self.directory, perm="elradfmwMT"
        )
        authorizer.add_anonymous(self.directory)
        handler = FTPHandler
        handler.authorizer = authorizer

        try:
            # Start server in separate thread.
            self.server = ThreadedFTPServer((self.host, self.port), handler)
            self.server_thread = threading.Thread(target=self.server.serve_forever)
            self.server_thread.daemon = True
            self.server_thread.start()

            # Wait a little bit to give the server time to grab the socket
            time.sleep(self.startup_wait_secs)

            yield self.directory

        finally:
            # Stop server and wait for server thread to join
            self.is_shutdown = True
            if self.server_thread is not None:
                self.server.close_all()
                self.server_thread.join()
