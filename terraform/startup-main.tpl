#!/usr/bin/env bash

# Set environment variables for Docker
export HOST_GOOGLE_APPLICATION_CREDENTIALS="${host_ao_home}/google_application_credentials.json"
export HOST_USER_ID=$(id -u airflow)
export HOST_LOGS_PATH="${host_airflow_home}/logs"
export HOST_DAGS_PATH="${host_ao_home}/academic-observatory/academic_observatory/dags"
export HOST_DATA_PATH="${host_ao_home}/data"
export HOST_PACKAGE_PATH="${host_ao_home}/academic-observatory"

export POSTGRES_HOSTNAME="${postgres_hostname}"
export POSTGRES_PASSWORD="sm://${project_id}/postgres_password"
export FERNET_KEY="sm://${project_id}/fernet_key"
export REDIS_HOSTNAME="redis"
export AIRFLOW_UI_USER_PASSWORD="sm://${project_id}/airflow_ui_user_password"
export AIRFLOW_UI_USER_EMAIL="sm://${project_id}/airflow_ui_user_email"

# Save google application credentials to file
sudo -u airflow -EH bash -c "berglas access sm://${project_id}/google_application_credentials | base64 --decode > ${host_ao_home}/google_application_credentials.json"

# Run program
cd $HOST_PACKAGE_PATH
sudo -u airflow -EH bash -c "cat docker-compose.cloud.yml docker-compose.observatory.yml > docker-compose.observatory-cloud.yml"
sudo -u airflow -EH bash -c "berglas exec -- docker-compose -f docker-compose.observatory-cloud.yml pull redis flower webserver scheduler worker_local"
sudo -u airflow -EH bash -c "berglas exec -- docker-compose -f docker-compose.observatory-cloud.yml build redis flower webserver scheduler worker_local"
sudo -u airflow -EH bash -c "berglas exec -- docker-compose -f docker-compose.observatory-cloud.yml up -d redis flower webserver scheduler worker_local"