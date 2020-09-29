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

import logging

from airflow.models import DagBag, Variable

from observatory_platform.utils.config_utils import get_dags_path

AIRFLOW_VAR_LOAD_DEFAULT_DAGS = 'LOAD_DEFAULT_DAGS'
AIRFLOW_VAR_DAGS_PACKAGE_NAMES = 'DAGS_PACKAGE_NAMES'
OBSERVATORY_PACKAGE_NAME = 'observatory_platform'
OBSERVATORY_DAGS_MODULE_NAME = 'default_dags'


def load_dag_bag(path: str) -> None:
    """ Load a DAG Bag from a given path.

    :param path: the path to the DAG bag.
    :return: None.
    """
    logging.info(f'Loading DAG bag from path: {dags_path}')
    dag_bag = DagBag(path)
    if dag_bag:
        for dag_id, dag in dag_bag.dags.items():
            logging.info(f'Adding DAG: dag_id={dag_id}, dag={dag}')
            globals()[dag_id] = dag


# If load default dags is true, then load DAGs from path
load_default_dags = bool(Variable.get(AIRFLOW_VAR_LOAD_DEFAULT_DAGS))
if load_default_dags:
    dags_path = get_dags_path(OBSERVATORY_PACKAGE_NAME, dags_module_name=OBSERVATORY_DAGS_MODULE_NAME)
    logging.info(f'Observatory Platform default DAGs path: {dags_path}')
    load_dag_bag(dags_path)

# Load DAGs for each DAG path
dags_packages = Variable.get(AIRFLOW_VAR_DAGS_PACKAGE_NAMES).split(':')
for package_name in dags_packages:
    dags_path = get_dags_path(package_name)
    logging.info(f'{package_name} DAGs path: {dags_path}')
    load_dag_bag(dags_path)
