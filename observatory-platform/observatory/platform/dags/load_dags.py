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

import logging

from airflow.models import DagBag

from observatory.platform.utils.config_utils import module_file_path
from observatory.platform.utils.workflow_utils import fetch_dags_modules, fetch_dag_bag

# Load DAGs for each DAG path
dags_modules = fetch_dags_modules()
for module_name in dags_modules:
    dags_path = module_file_path(module_name)
    logging.info(f"{module_name} DAGs path: {dags_path}")
    dag_bag: DagBag = fetch_dag_bag(dags_path)

    # Load dags
    for dag_id, dag in dag_bag.dags.items():
        logging.info(f"Adding DAG: dag_id={dag_id}, dag={dag}")
        globals()[dag_id] = dag
