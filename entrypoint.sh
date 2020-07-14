#!/bin/bash
# Make airflow user own everything in home directory
chown -R airflow ${AIRFLOW_HOME}

# Make airflow user own everything in observatory directory
chown -R airflow ${AO_HOME}

# Install the Academic Observatory Python package requirements
cd ${AO_HOME}/academic-observatory
sudo -u airflow -EH bash -c "pip3 install --user -r requirements.txt"

# Remove tests directory which other packages install to sometimes
TESTS_DIRECTORY='/home/airflow/.local/lib/python3.7/site-packages/tests'
if [ -d ${TESTS_DIRECTORY} ]; then
  rm -r ${TESTS_DIRECTORY}
fi

# Initialise or upgrade the Airflow database and create the admin user
# Only run if webserver is command as there should only be one webserver
if [ $1="webserver" ]; then
  # Initialise / upgrade the Airflow database
  sudo -u airflow -EH bash -c "airflow upgradedb"

  # Create the Admin user. This command will just print "airflow already exist in the db" if the user already exists
  sudo -u airflow -EH bash -c "airflow create_user -r Admin -u airflow -e ${AIRFLOW_UI_USER_EMAIL} -f Observatory -l Admin -p ${AIRFLOW_UI_USER_PASSWORD}"
fi

# Install the Academic Observatory Python package
sudo -u airflow -EH bash -c "pip3 install -e . --user"

# Start the airflow webserver
cd ${AIRFLOW_HOME}
sudo -u airflow -EH bash -c "/usr/bin/dumb-init -- /entrypoint $@"