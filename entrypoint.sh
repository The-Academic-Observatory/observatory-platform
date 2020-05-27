#!/bin/bash

# Install the Academic Observatory Python package
cd ${AIRFLOW_USER_HOME}/academic-observatory && pip3 install --user -r requirements.txt && pip3 install -e . --user

# Start the airflow webserver
cd / && ./entrypoint.sh webserver