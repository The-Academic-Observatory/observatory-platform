# Copyright 2023 Curtin University
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
from typing import List

from observatory.platform.airflow import fetch_workflows, make_workflow
from observatory.platform.observatory_config import Workflow
from observatory.platform.workflows.workflow import Workflow as ObservatoryWorkflow

# Load DAGs
workflows: List[Workflow] = fetch_workflows()
for config in workflows:
    logging.info(f"Making Workflow: {config.name}, dag_id={config.dag_id}")
    workflow = make_workflow(config)

    if isinstance(workflow, ObservatoryWorkflow):
        dag = workflow.make_dag()
        logging.info(f"Adding DAG: dag_id={workflow.dag_id}, dag={dag}")
        globals()[workflow.dag_id] = dag
