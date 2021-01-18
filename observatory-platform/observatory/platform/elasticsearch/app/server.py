"""
Main module of the server file
"""

import connexion
from connexion.resolver import RestyResolver
from flask import render_template
import os
from flask import current_app


def basic_auth(api_key, required_scopes=None):
    app_dir = os.path.dirname(os.path.realpath(__file__))
    with open(os.path.join(app_dir, 'api', 'elasticsearch_auth.txt')) as auth_file:
        for line in auth_file:
            username, token = line.strip().split(' ')
            if api_key == token:
                return {'sub': username}
    return None


def create_app():
    app_dir = os.path.dirname(os.path.realpath(__file__))
    # create the application instance
    conn_app = connexion.App(__name__, specification_dir=app_dir)
    conn_app.app.config['JSON_SORT_KEYS'] = False

    query_filter_parameters = ['id', 'name', 'published_year', 'coordinates', 'country', 'country_code', 'region',
                               'subregion', 'access_type', 'label', 'status', 'collaborator_coordinates',
                               'collaborator_country', 'collaborator_country_code', 'collaborator_id',
                               'collaborator_name', 'collaborator_region', 'collaborator_subregion', 'field', 'source',
                               'funder_country_code', 'funder_name', 'funder_sub_type', 'funder_type', 'journal',
                               'output_type', 'publisher']

    conn_app.add_api(os.path.join(app_dir, 'openapi2.yml'), resolver=RestyResolver('api'), arguments={
        'query_parameters': query_filter_parameters
    })

    with conn_app.app.app_context():
        current_app.query_filter_parameters = query_filter_parameters
    return conn_app


app = create_app()


# Create a URL route in our application for "/"
@app.route("/")
def home():
    """
    This function just responds to the browser URL
    localhost:5000/
    :return:        the rendered template "home.html"
    """
    return render_template("home.html")


if __name__ == "__main__":
    app.run(debug=True)
