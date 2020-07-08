#!/usr/bin/env bash

# Set environment variables
export AO_HOME=${ao_home}
export HOST_USER_ID=$(id -u airflow)
export HOST_LOGS_PATH="${ao_home}/logs"
export HOST_DAGS_PATH="${ao_home}/academic-observatory/academic_observatory/dags"
export HOST_DATA_PATH="${ao_home}/data"
export HOST_PACKAGE_PATH="${ao_home}/academic-observatory"
export POSTGRES_HOSTNAME="${postgres_hostname}"
export POSTGRES_PASSWORD="sm://${project_id}/postgres_password"
export FERNET_KEY="sm://${project_id}/fernet_key"
export REDIS_HOSTNAME="${redis_hostname}"

# Save google application credentials to file
berglas access sm://${project_id}/google_application_credentials | base64 --decode > ${ao_home}/google_application_credentials.json

# Run program
cd $HOST_PACKAGE_PATH
gosu airflow bash -c "berglas exec -- docker-compose -f docker-compose.observatory.yml pull worker_remote"
gosu airflow bash -c "berglas exec -- docker-compose -f docker-compose.observatory.yml build worker_remote"
gosu airflow bash -c "berglas exec -- docker-compose -f docker-compose.observatory.yml up -d worker_remote"
