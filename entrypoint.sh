#!/bin/bash
# Make airflow user own everything in home directory
chown -R airflow ${AIRFLOW_HOME}

# Make airflow user own everything in observatory directory
chown -R airflow ${AO_HOME}

# Install the Academic Observatory Python package requirements
cd ${AO_HOME}/academic-observatory
gosu airflow bash -c "pip3 install --user -r requirements.txt"

# Remove tests directory which other packages install to sometimes
TESTS_DIRECTORY='/home/airflow/.local/lib/python3.7/site-packages/tests'
if [ -d ${TESTS_DIRECTORY} ]; then
  rm -r ${TESTS_DIRECTORY}
fi

# Install the Academic Observatory Python package
gosu airflow bash -c "pip3 install -e . --user"

# Start the airflow webserver
cd ${AIRFLOW_HOME}
gosu airflow /entrypoint $@