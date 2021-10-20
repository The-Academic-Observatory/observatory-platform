#!/usr/bin/env bash
# Copyright 2020 Curtin University
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Author: James Diprose

# This script runs commands as the root user

# Make airflow user own everything in home directory
chown -R airflow ${AIRFLOW_HOME}

# Make airflow user own everything in observatory directory
chown -R airflow /opt/observatory

# Hardcoded list of environment variables that need to be preserved
STANDARD_ENV_PRESERVE="AIRFLOW_HOME,GOOGLE_APPLICATION_CREDENTIALS,AO_HOME,AIRFLOW_UI_USER_EMAIL,AIRFLOW_UI_USER_PASSWORD"

# Preserve all environment variables that begin with AIRFLOW__, AIRFLOW_VAR or AIRFLOW_CONN
ALL_ENV_PRESERVE=$(printenv | awk -v env_preserve="$STANDARD_ENV_PRESERVE" -F'=' '$0 ~ /AIRFLOW__|AIRFLOW_VAR|AIRFLOW_CONN/ {printf "%s,", $1} END {print env_preserve}')

sudo --preserve-env=$ALL_ENV_PRESERVE --user airflow --set-home --login /entrypoint-airflow.sh "$@"