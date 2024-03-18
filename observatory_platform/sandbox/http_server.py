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

# Author: Tuan Chien

import contextlib
import os
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from multiprocessing import Process

from observatory_platform.sandbox.test_utils import find_free_port


class HttpServer:
    """Simple HTTP server for testing. Serves files from a directory to http://locahost:port/filename"""

    def __init__(self, directory: str, host: str = "localhost", port: int = None):
        """Initialises the server.

        :param directory: Directory to serve.
        """

        self.directory = directory
        self.process = None

        self.host = host
        if port is None:
            port = find_free_port(host=self.host)
        self.port = port
        self.address = (self.host, self.port)
        self.url = f"http://{self.host}:{self.port}/"

    @staticmethod
    def serve_(address, directory):
        """Entry point for a new process to run HTTP server.

        :param address: Address (host, port) to bind server to.
        :param directory: Directory to serve.
        """

        os.chdir(directory)
        server = ThreadingHTTPServer(address, SimpleHTTPRequestHandler)
        server.serve_forever()

    def start(self):
        """Spin the server up in a new process."""

        # Don't try to start it twice.
        if self.process is not None and self.process.is_alive():
            return

        self.process = Process(
            target=HttpServer.serve_,
            args=(
                self.address,
                self.directory,
            ),
        )
        self.process.start()

    def stop(self):
        """Shutdown the server."""

        if self.process is not None and self.process.is_alive():
            self.process.kill()
            self.process.join()

    @contextlib.contextmanager
    def create(self):
        """Spin up a server for the duration of the session."""
        self.start()

        try:
            yield self.process
        finally:
            self.stop()
