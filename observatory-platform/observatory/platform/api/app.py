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

from __future__ import annotations

import os

import connexion

import observatory.platform.api.api as api
import observatory.platform.api.orm as orm
from observatory.platform.api.orm import create_session


def set_session(session):
    api.session_ = session
    orm.session_ = session


def create_app() -> connexion.App:
    """ Create a Connexion App.

    :return: the Connexion App.
    """

    app_dir = os.path.dirname(os.path.realpath(__file__))

    # Create the application instance
    conn_app = connexion.App(__name__, specification_dir=app_dir)

    # Add the OpenAPI specification
    specification_path = os.path.join(app_dir, 'openapi-v1.yml')
    conn_app.add_api(specification_path)

    return conn_app


if __name__ == "__main__":
    # Setup Session
    session_ = create_session()
    set_session(session_)

    # Create the Connexion App
    app = create_app()


    @app.app.teardown_appcontext
    def remove_session(exception=None):
        if session_ is not None:
            session_.remove()


    app.run(debug=True)
