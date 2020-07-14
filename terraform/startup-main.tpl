#!/usr/bin/env bash

# Set environment variables
export HOST_AIRFLOW_HOME="/opt/airflow"
export HOST_AO_HOME="/opt/observatory"
export HOST_GOOGLE_APPLICATION_CREDENTIALS="$HOST_AO_HOME/google_application_credentials.json"
export HOST_USER_ID=$(id -u airflow)
export HOST_LOGS_PATH="$HOST_AIRFLOW_HOME/logs"
export HOST_DAGS_PATH="$HOST_AO_HOME/academic-observatory/academic_observatory/dags"
export HOST_DATA_PATH="$HOST_AO_HOME/data"
export HOST_PACKAGE_PATH="$HOST_AO_HOME/academic-observatory"
export POSTGRES_HOSTNAME="${postgres_hostname}"
export POSTGRES_PASSWORD="sm://${project_id}/postgres_password"
export FERNET_KEY="sm://${project_id}/fernet_key"
export REDIS_HOSTNAME="redis"
export AIRFLOW_VAR_PROJECT_ID=${project_id}
export AIRFLOW_VAR_DATA_LOCATION=${data_location}
export AIRFLOW_VAR_DOWNLOAD_BUCKET_NAME=${download_bucket_name}
export AIRFLOW_VAR_TRANSFORM_BUCKET_NAME=${transform_bucket_name}
export AIRFLOW_VAR_ENVIRONMENT=${environment}

# Save google application credentials to file
berglas access sm://${project_id}/google_application_credentials | base64 --decode > $HOST_AO_HOME/google_application_credentials.json

# Run program
cd $HOST_PACKAGE_PATH
gosu airflow bash -c "berglas exec -- cat docker-compose.cloud.yml docker-compose.observatory.yml | docker-compose -f - pull redis flower init_db webserver scheduler worker_local"
gosu airflow bash -c "berglas exec -- cat docker-compose.cloud.yml docker-compose.observatory.yml | docker-compose -f - build redis flower init_db webserver scheduler worker_local"
gosu airflow bash -c "berglas exec -- cat docker-compose.cloud.yml docker-compose.observatory.yml | docker-compose -f - up -d redis flower init_db webserver scheduler worker_local"