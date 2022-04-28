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

# Author: Tuan Chien

# Create database if it does not exist
psql ${BASE_DB_URI} -tc "SELECT 1 FROM pg_database WHERE datname = '${API_SERVER_DB}'" | grep -q 1 || psql ${BASE_DB_URI} -c "CREATE DATABASE ${API_SERVER_DB}"

# Launch API server
sudo --preserve-env=OBSERVATORY_DB_URI --user airflow --set-home --login gunicorn -b 0.0.0.0:${OBSERVATORY_DB_PORT} --timeout 0 observatory.api.server.app:app