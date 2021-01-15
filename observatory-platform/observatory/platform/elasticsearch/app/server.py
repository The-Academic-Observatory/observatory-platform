"""
Main module of the server file
"""

import connexion
from flask import render_template
from flask import current_app
import os


def basic_auth(api_key, required_scopes=None):
    with open(os.path.join(app_dir, 'api', 'elasticsearch_auth.txt')) as auth_file:
        for line in auth_file:
            username, token = line.strip().split(' ')
            if api_key == token:
                return {'sub': username}
    return None


def create_app():
    # create the application instance
    app = connexion.App(__name__, specification_dir=app_dir)
    app.app.config['JSON_SORT_KEYS'] = False

    query_filter_parameters = ['id', 'name', 'published_year', 'coordinates', 'country', 'country_code', 'region',
                               'subregion', 'access_type', 'label', 'status', 'collaborator_coordinates',
                               'collaborator_country', 'collaborator_country_code', 'collaborator_id',
                               'collaborator_name', 'collaborator_region', 'collaborator_subregion', 'field', 'source',
                               'funder_country_code', 'funder_name', 'funder_sub_type', 'funder_type', 'journal',
                               'output_type', 'publisher']

    app.add_api(os.path.join(app_dir, 'openapi2.yml'), arguments={
        'query_parameters': query_filter_parameters
    })

    with app.app.app_context():
        current_app.query_filter_parameters = query_filter_parameters
    return app


app_dir = os.path.dirname(os.path.realpath(__file__))
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
