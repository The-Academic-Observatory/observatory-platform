# Copyright 2020 Curtin University.
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


import os

import numpy as np
import pandas as pd

from observatory_platform.database.elastic.observatory_database import ObservatoryDatabase


def load_csv(csv_path: str):
    df = pd.read_csv(csv_path)
    df = df.replace({np.nan: None})
    for index, row in df.iterrows():
        yield row.to_dict()


def load_elastic(data_path: str, user: str, secret: str, hostname: str, port: str):
    host = f'https://{user}:{secret}@{hostname}:{port}'
    client = ObservatoryDatabase(host=host)

    # Index metrics
    mappings_filename = 'metrics-mappings.json'
    indexes = ['metrics-country', 'metrics-funder', 'metrics-groups', 'metrics-institution', 'metrics-publisher']
    for index_id in indexes:
        file_path = os.path.join(data_path, f'{index_id}.csv')
        success = client.index_documents(index_id, mappings_filename, load_csv(file_path))

    # Index events
    mappings_filename = 'events-mappings.json'
    indexes = ['events-country', 'events-funder']
    for index_id in indexes:
        file_path = os.path.join(data_path, f'{index_id}.csv')
        success = client.index_documents(index_id, mappings_filename, load_csv(file_path))

    # Index outputs
    mappings_filename = 'outputs-mappings.json'
    indexes = ['outputs-country']
    for index_id in indexes:
        file_path = os.path.join(data_path, f'{index_id}.csv')
        success = client.index_documents(index_id, mappings_filename, load_csv(file_path))

    mappings_filename = 'citations-mappings.json'
    indexes = ['citations-country']
    for index_id in indexes:
        file_path = os.path.join(data_path, f'{index_id}.csv')
        success = client.index_documents(index_id, mappings_filename, load_csv(file_path))
