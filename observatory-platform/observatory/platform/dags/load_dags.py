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

# The keywords airflow and DAG are required to load the DAGs from this file, see bullet 2 in the Apache Airflow FAQ:
# https://airflow.apache.org/docs/stable/faq.html

# Author: James Diprose

import json
import logging

from airflow.models import DagBag, Variable
from airflow.secrets.environment_variables import EnvironmentVariablesBackend

from observatory.platform.utils.airflow_utils import AirflowVars
from observatory.platform.utils.config_utils import module_file_path


def get_dags_modules() -> dict:
    """ Get the dags modules from the Airflow Variable

    :return: Dags modules
    """

    # Try to get value from env variable first, saving costs from GC secret usage
    dags_modules_str = EnvironmentVariablesBackend().get_variable(AirflowVars.DAGS_MODULE_NAMES)
    if not dags_modules_str:
        dags_modules_str = Variable.get(AirflowVars.DAGS_MODULE_NAMES)
    logging.info(f"dags_modules str: {dags_modules_str}")
    dags_modules_ = json.loads(dags_modules_str)
    logging.info(f"dags_modules: {dags_modules_}")
    return dags_modules_


def load_dag_bag(path: str) -> None:
    """Load a DAG Bag from a given path.

    :param path: the path to the DAG bag.
    :return: None.
    """
    logging.info(f"Loading DAG bag from path: {dags_path}")
    dag_bag = DagBag(path)

    if dag_bag:
        if len(dag_bag.import_errors):
            # Collate loading errors as single string and raise it as exception
            results = []
            for path, exception in dag_bag.import_errors.items():
                results.append(f"DAG import exception: {path}\n{exception}\n\n")
            raise Exception("\n".join(results))
        else:
            # Load dags
            for dag_id, dag in dag_bag.dags.items():
                logging.info(f"Adding DAG: dag_id={dag_id}, dag={dag}")
                globals()[dag_id] = dag


# Load DAGs for each DAG path
dags_modules = get_dags_modules()
for module_name in dags_modules:
    dags_path = module_file_path(module_name)
    logging.info(f"{module_name} DAGs path: {dags_path}")
    load_dag_bag(dags_path)
