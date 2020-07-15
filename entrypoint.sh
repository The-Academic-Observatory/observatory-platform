#!/bin/bash
# install mawk
sudo apt-get install -y mawk

# Make airflow user own everything in home directory
chown -R airflow:airflow ${AIRFLOW_USER_HOME}

# Install the Academic Observatory Python package requirements
cd ${AIRFLOW_USER_HOME}/academic-observatory
gosu airflow:airflow bash -c "pip3 install --user -r requirements.txt"

# Remove tests directory which other packages install to sometimes
rm -r ${AIRFLOW_USER_HOME}/.local/lib/python3.7/site-packages/tests

# Install the Academic Observatory Python package
gosu airflow:airflow bash -c "pip3 install -e . --user"

# Start the airflow webserver
cd ${AIRFLOW_USER_HOME}
gosu airflow:airflow bash -c "/entrypoint.sh webserver"