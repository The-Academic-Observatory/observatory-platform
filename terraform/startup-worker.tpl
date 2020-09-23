#!/usr/bin/env bash

# Set environment variables for Docker
export HOST_GOOGLE_APPLICATION_CREDENTIALS="${host_ao_home}/google_application_credentials.json"
export HOST_USER_ID=$(id -u airflow)
export HOST_GROUP_ID=$(id -g airflow)
export HOST_LOGS_PATH="${host_airflow_home}/logs"
export HOST_DAGS_PATH="${host_ao_home}/observatory-platform/observatory_platform/dags"
export HOST_DATA_PATH="${host_ao_home}/data"
export HOST_PACKAGE_PATH="${host_ao_home}/observatory-platform"

export POSTGRES_HOSTNAME="${postgres_hostname}"
export POSTGRES_PASSWORD="sm://${project_id}/postgres_password"
export FERNET_KEY="sm://${project_id}/fernet_key"
export REDIS_HOSTNAME="${redis_hostname}"

names=('${join("' '", keys(airflow_variables))}')
values=('${join("' '", values(airflow_variables))}')

for index in $${!names[*]};
do export AIRFLOW_VAR_$${names[$index]^^}="$${values[$index]}";
done

# Save google application credentials to file
sudo -u airflow bash -c "berglas access sm://${project_id}/google_application_credentials | base64 --decode > ${host_ao_home}/google_application_credentials.json"

# Run program
cd $HOST_PACKAGE_PATH
sudo -u airflow bash -c "cat docker-compose.cloud.yml docker-compose.observatory.yml > docker-compose.observatory-cloud.yml"
sudo -u airflow bash -c "docker-compose -f docker-compose.observatory-cloud.yml pull worker_remote"

# Hardcoded list of environment variables that need to be preserved
STANDARD_ENV_PRESERVE="HOST_GOOGLE_APPLICATION_CREDENTIALS,HOST_USER_ID,HOST_GROUP_ID"
# Preserve all environment variables that begin with AIRFLOW_VAR or AIRFLOW_CONN
ALL_ENV_PRESERVE=$(printenv | awk -v env_preserve="$STANDARD_ENV_PRESERVE" -F'=' '$0 ~ /AIRFLOW_VAR|AIRFLOW_CONN/ {printf "%s,", $1} END {print env_preserve}')
sudo -u airflow --preserve-env=$ALL_ENV_PRESERVE bash -c "docker-compose -f docker-compose.observatory-cloud.yml build worker_remote"

# Hardcoded list of environment variables that need to be preserved
STANDARD_ENV_PRESERVE="HOST_GOOGLE_APPLICATION_CREDENTIALS,HOST_USER_ID,HOST_GROUP_ID,HOST_LOGS_PATH,\
HOST_DAGS_PATH,HOST_DATA_PATH,HOST_PACKAGE_PATH,POSTGRES_HOSTNAME,POSTGRES_PASSWORD,FERNET_KEY,REDIS_HOSTNAME"
# Preserve all environment variables that begin with AIRFLOW_VAR or AIRFLOW_CONN
ALL_ENV_PRESERVE=$(printenv | awk -v env_preserve="$STANDARD_ENV_PRESERVE" -F'=' '$0 ~ /AIRFLOW_VAR|AIRFLOW_CONN/ {printf "%s,", $1} END {print env_preserve}')
sudo -u airflow -H --preserve-env=$ALL_ENV_PRESERVE \
bash -c "berglas exec -- docker-compose -f docker-compose.observatory-cloud.yml up -d worker_remote"