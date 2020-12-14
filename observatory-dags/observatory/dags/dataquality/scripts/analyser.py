#!/usr/bin/python3

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


from google.oauth2 import service_account
from observatory.dags.dataquality.mag import MagAnalyser

from elasticsearch_dsl import connections
import pandas_gbq

# Enable logging for debug
import logging

logging.getLogger().setLevel(logging.INFO)


def init_es_connection():
    """ Elastic search connection details. """
    user = ''
    password = ''
    hostname = ''
    port = ''
    connections.create_connection(hosts=[f'https://{user}:{password}@{hostname}:{port}'], timeout=30000)


if __name__ == '__main__':
    init_es_connection()
    project_id = ''
    dataset_id = ''

    # Google credentials
    credentials = service_account.Credentials.from_service_account_file(
        '')
    pandas_gbq.context.credentials = credentials
    pandas_gbq.context.project = ''  # Project to make queries from. Could be same as project_id

    mag = MagAnalyser(project_id, dataset_id)
    mag.run()
    # mag.erase(index=True)
