#!/usr/bin/env bash
# This script runs commands as the root user

# Make airflow user own everything in home directory
chown -R airflow:airflow ${AIRFLOW_HOME}

# Make airflow user own everything in observatory directory
chown -R airflow:airflow /opt/observatory

# Hardcoded list of environment variables that need to be preserved
STANDARD_ENV_PRESERVE="AIRFLOW_HOME,AIRFLOW__CORE__EXECUTOR,AIRFLOW__CORE__SQL_ALCHEMY_CONN,AIRFLOW__CORE__FERNET_KEY,\
AIRFLOW__CORE__AIRFLOW_HOME,AIRFLOW__CORE__AIRFLOW_HOME,AIRFLOW__CORE__LOAD_EXAMPLES,AIRFLOW__CORE__LOAD_DEFAULT_CONNECTIONS,\
AIRFLOW__CORE__ENABLE_XCOM_PICKLING,AIRFLOW__WEBSERVER__RBAC,AIRFLOW__CELERY__BROKER_URL,AIRFLOW__CELERY__RESULT_BACKEND,AIRFLOW__SECRETS__BACKEND,\
AIRFLOW__SECRETS__BACKEND_KWARGS,GOOGLE_APPLICATION_CREDENTIALS,AO_HOME,AIRFLOW_UI_USER_EMAIL,AIRFLOW_UI_USER_PASSWORD"

# Preserve all environment variables that begin with AIRFLOW_VAR or AIRFLOW_CONN
ALL_ENV_PRESERVE=$(printenv | awk -v env_preserve="$STANDARD_ENV_PRESERVE" -F'=' '$0 ~ /AIRFLOW_VAR|AIRFLOW_CONN/ {printf "%s,", $1} END {print env_preserve}')

sudo --preserve-env=$ALL_ENV_PRESERVE --user airflow --set-home --login /entrypoint-airflow.sh "$@"