#!/usr/bin/env bash
# This script is run as the airflow user

# Initialise or upgrade the Airflow database and create the admin user
# Only run if webserver is command as there should only be one webserver
if [ $1="webserver" ]; then
  # Initialise / upgrade the Airflow database
  airflow upgradedb

  # Create the Admin user. This command will just print "airflow already exist in the db" if the user already exists
  airflow create_user -r Admin -u airflow -e ${AIRFLOW_UI_USER_EMAIL} -f Observatory -l Admin -p ${AIRFLOW_UI_USER_PASSWORD}
fi

# Enter academic observatory folder
cd /opt/observatory/academic-observatory

# Install Python dependencies for Academic Observatory
pip3 install -r requirements.txt --user

# Remove tests directory which other packages install to sometimes
TESTS_DIRECTORY='/home/airflow/.local/lib/python3.7/site-packages/tests'
if [ -d ${TESTS_DIRECTORY} ]; then
  rm -r ${TESTS_DIRECTORY}
fi

# Install the Academic Observatory Python package
pip3 install -e . --user

# Enter airflow home folder. Must be in the AIRFLOW_HOME folder (i.e. /opt/airflow) before running the next command
# otherwise the system will start but the workers and scheduler will not find the DAGs and other files because
# they look for them based on the current working directory.
cd ${AIRFLOW_HOME}

# Run entrypoint given by airflow docker file
/usr/bin/dumb-init -- /entrypoint "$@"