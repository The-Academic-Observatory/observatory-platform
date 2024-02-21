# Copyright (c) 2011-2017 Ruslan Spivak
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.
#
# Sources:
# * https://github.com/rspivak/sftpserver/blob/master/src/sftpserver/__init__.py

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

import contextlib
import os
import socket
import threading
import time

import paramiko
from click.testing import CliRunner
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from sftpserver.stub_sftp import StubServer, StubSFTPServer


class SftpServer:
    """A Mock SFTP server for testing purposes"""

    def __init__(
        self,
        host: str = "localhost",
        port: int = 3373,
        level: str = "INFO",
        backlog: int = 10,
        startup_wait_secs: int = 1,
        socket_timeout: int = 10,
    ):
        """Create a Mock SftpServer instance.

        :param host: the host name.
        :param port: the port.
        :param level: the log level.
        :param backlog: ?
        :param startup_wait_secs: time in seconds to wait before returning from create to give the server enough
        time to start before connecting to it.
        """

        self.host = host
        self.port = port
        self.level = level
        self.backlog = backlog
        self.startup_wait_secs = startup_wait_secs
        self.is_shutdown = True
        self.tmp_dir = None
        self.root_dir = None
        self.private_key_path = None
        self.server_thread = None
        self.socket_timeout = socket_timeout

    def _generate_key(self):
        """Generate a private key.

        :return: the filepath to the private key.
        """

        key = rsa.generate_private_key(public_exponent=65537, key_size=2048, backend=default_backend())

        private_key_path = os.path.join(self.tmp_dir, "test_rsa.key")
        with open(private_key_path, "wb") as f:
            f.write(
                key.private_bytes(
                    encoding=serialization.Encoding.PEM,
                    format=serialization.PrivateFormat.TraditionalOpenSSL,
                    encryption_algorithm=serialization.NoEncryption(),
                )
            )

        return private_key_path

    def _start_server(self):
        paramiko_level = getattr(paramiko.common, self.level)
        paramiko.common.logging.basicConfig(level=paramiko_level)

        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.settimeout(self.socket_timeout)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, True)
        server_socket.bind((self.host, self.port))
        server_socket.listen(self.backlog)

        while not self.is_shutdown:
            try:
                conn, addr = server_socket.accept()
                transport = paramiko.Transport(conn)
                transport.add_server_key(paramiko.RSAKey.from_private_key_file(self.private_key_path))
                transport.set_subsystem_handler("sftp", paramiko.SFTPServer, StubSFTPServer)

                server = StubServer()
                transport.start_server(server=server)

                channel = transport.accept()
                while transport.is_active() and not self.is_shutdown:
                    time.sleep(1)

            except socket.timeout:
                # Timeout must be set for socket otherwise it will wait for a connection forever and block
                # the thread from exiting. At: conn, addr = server_socket.accept()
                pass

    @contextlib.contextmanager
    def create(self):
        """Make and destroy a test SFTP server.

        :yield: None.
        """

        with CliRunner().isolated_filesystem() as tmp_dir:
            # Override the root directory of the SFTP server, which is set as the cwd at import time
            self.tmp_dir = tmp_dir
            self.root_dir = os.path.join(tmp_dir, "home")
            os.makedirs(self.root_dir, exist_ok=True)
            StubSFTPServer.ROOT = self.root_dir

            # Generate private key
            self.private_key_path = self._generate_key()

            try:
                self.is_shutdown = False
                self.server_thread = threading.Thread(target=self._start_server)
                self.server_thread.start()

                # Wait a little bit to give the server time to grab the socket
                time.sleep(self.startup_wait_secs)

                yield self.root_dir
            finally:
                # Stop server and wait for server thread to join
                self.is_shutdown = True
                if self.server_thread is not None:
                    self.server_thread.join()
