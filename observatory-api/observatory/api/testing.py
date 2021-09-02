# Copyright 2021 Curtin University
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

# Author: James Diprose

import contextlib
import threading

from observatory.api.server.api import create_app
from observatory.api.server.orm import create_session, set_session
from sqlalchemy.pool import StaticPool
from werkzeug.serving import make_server


class ObservatoryApiEnvironment:
    def __init__(self, host: str = "localhost", port: int = 5000, seed_db: bool = False):
        """Create an ObservatoryApiEnvironment instance.

        :param host: the host name.
        :param port: the port.
        :param seed_db: whether to seed the database or not.
        """

        self.host = host
        self.port = port
        self.seed_db = seed_db
        self.db_uri = "sqlite://"
        self.session = None
        self.server = None
        self.server_thread = None

    @contextlib.contextmanager
    def create(self):
        """Make and destroy an Observatory API isolated environment, which involves:

        * Creating an in memory SQLite database for the API backend to connect to
        * Start the Connexion / Flask app

        :yield: None.
        """

        try:
            # Connect to in memory SQLite database with SQLAlchemy
            self.session = create_session(
                uri=self.db_uri, connect_args={"check_same_thread": False}, poolclass=StaticPool
            )
            set_session(self.session)

            # Create the Connexion App and start the server
            app = create_app()
            self.server = make_server(self.host, self.port, app)
            self.server_thread = threading.Thread(target=self.server.serve_forever)
            self.server_thread.start()
            yield
        finally:
            # Stop server and wait for server thread to join
            self.server.shutdown()
            self.server_thread.join()
