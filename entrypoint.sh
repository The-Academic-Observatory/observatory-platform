#!/bin/bash

# Make airflow user own everything in home directory
chown -R airflow:airflow ${AIRFLOW_USER_HOME}

# Install the Academic Observatory Python package
cd ${AIRFLOW_USER_HOME}/academic-observatory
gosu airflow:airflow bash -c "pip3 install --user -r requirements.txt && pip3 install -e . --user"

# Start the airflow webserver
cd ${AIRFLOW_USER_HOME}
gosu airflow:airflow bash -c "/entrypoint.sh webserver"