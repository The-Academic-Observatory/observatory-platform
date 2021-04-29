# Copyright 2021 Curtin University
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

# The keywords airflow and DAG are required to load the DAGs from this file, see bullet 2 in the Apache Airflow FAQ:
# https://airflow.apache.org/docs/stable/faq.html

import logging

from observatory.dags.workflows.update_datasets import UpdateDatasetsWorkflow
from observatory.platform.utils.airflow_utils import AirflowVariable as Variable, AirflowVars

try:
    # Make workflows
    project_ids = Variable.get(AirflowVars.UPDATE_DATASETS_PROJECT_IDS, deserialize_json=True)
    for project_id in project_ids:
        workflow = UpdateDatasetsWorkflow(dst_gcp_project_id=project_id)
        globals()[workflow.dag_id] = workflow.make_dag()
except KeyError as e:
    logging.warning(f'Airflow Variable {AirflowVars.UPDATE_DATASETS_PROJECT_IDS} not set')
