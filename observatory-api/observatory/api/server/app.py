# Copyright 2020-2021 Curtin University
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

# Author: Aniek Roelofs, James Diprose

from __future__ import annotations

from observatory.api.server.api import create_app
from observatory.api.server.orm import create_session, set_session

# Setup Observatory DB Session
session_ = create_session()
set_session(session_)

# Create the Connexion App
app = create_app()


@app.app.teardown_appcontext
def remove_session(exception=None):
    """Remove the SQLAlchemy session.

    :param exception:
    :return: None.
    """

    if session_ is not None:
        session_.remove()


if __name__ == "__main__":
    app.run(debug=True)
