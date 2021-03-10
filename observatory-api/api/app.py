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

import os

import connexion
from connexion import FlaskApp
from flask import current_app
from flask import render_template


def create_app() -> FlaskApp:
    """ Create app using connexion

    :return: FlaskApp instance
    """
    app_dir = os.path.dirname(os.path.realpath(__file__))
    # create the application instance
    conn_app = connexion.App(__name__, specification_dir=app_dir)
    # don't sort json output alphabetically
    conn_app.app.config['JSON_SORT_KEYS'] = False

    query_filter_parameters = ['id', 'name', 'published_year', 'coordinates', 'country', 'country_code', 'region',
                               'subregion', 'access_type', 'label', 'status', 'collaborator_coordinates',
                               'collaborator_country', 'collaborator_country_code', 'collaborator_id',
                               'collaborator_name', 'collaborator_region', 'collaborator_subregion', 'field', 'source',
                               'funder_country_code', 'funder_name', 'funder_sub_type', 'funder_type', 'journal',
                               'output_type', 'publisher']

    conn_app.add_api(os.path.join(app_dir, 'openapi.yml'), arguments={
        'query_parameters': query_filter_parameters
    })

    # add query filter parameters to app context, so they can be pulled in 'query.py'
    with conn_app.app.app_context():
        current_app.query_filter_parameters = query_filter_parameters
    return conn_app


app = create_app()


# Create a URL route in our application for "/"
@app.route("/")
def home():
    """ Create home route.
    :return: the rendered template "home.html"
    """
    return render_template("home.html")


if __name__ == "__main__":
    app.run(debug=True)
