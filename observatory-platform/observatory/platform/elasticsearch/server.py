"""
Main module of the server file
"""

import connexion
from flask import render_template

# create the application instance
app = connexion.App(__name__, specification_dir="./")

query_filter_parameters = ['id', 'name', 'published_year', 'coordinates', 'country', 'country_code', 'region',
                           'subregion', 'access_type', 'label', 'status', 'collaborator_coordinates',
                           'collaborator_country', 'collaborator_country_code', 'collaborator_id', 'collaborator_name',
                           'collaborator_region', 'collaborator_subregion', 'field', 'source', 'funder_country_code',
                           'funder_name', 'funder_sub_type', 'funder_type', 'journal', 'output_type', 'publisher']

# app.add_api(rendered_openapi)
app.add_api('openapi2.yml', arguments={
    'query_parameters': query_filter_parameters
})


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
