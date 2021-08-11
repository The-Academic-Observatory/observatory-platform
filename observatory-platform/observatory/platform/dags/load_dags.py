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

# airflow dags
# Author: James Diprose

import json
import logging

from airflow.models import DagBag, Variable
from observatory.platform.utils.airflow_utils import AirflowVariable, AirflowVars
from observatory.platform.utils.config_utils import module_file_path


def load_dag_bag(path: str) -> None:
    """Load a DAG Bag from a given path.

    :param path: the path to the DAG bag.
    :return: None.
    """
    logging.info(f"Loading DAG bag from path: {dags_path}")
    dag_bag = DagBag(path)
    if dag_bag:
        for dag_id, dag in dag_bag.dags.items():
            logging.info(f"Adding DAG: dag_id={dag_id}, dag={dag}")
            globals()[dag_id] = dag


# Load DAGs for each DAG path
dags_modules_str = AirflowVariable.get(AirflowVars.DAGS_MODULE_NAMES)
logging.info(f"dags_modules str: {dags_modules_str}")
dags_modules = json.loads(dags_modules_str)
logging.info(f"dags_modules: {dags_modules}")
for module_name in dags_modules:
    dags_path = module_file_path(module_name)
    logging.info(f"{module_name} DAGs path: {dags_path}")
    load_dag_bag(dags_path)
