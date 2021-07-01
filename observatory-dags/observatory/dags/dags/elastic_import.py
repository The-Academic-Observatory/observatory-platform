# Copyright 2020, 2021 Curtin University
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

import dataclasses
from typing import List

from observatory.dags.workflows.elastic_import_workflow import ElasticImportWorkflow
from observatory.platform.utils.telescope_utils import make_dag_id

DATASET_ID = "data_export"
DATA_LOCATION = "us"
FILE_TYPE_JSONL = "jsonl.gz"
DAG_ONIX_WORKFLOW_PREFIX = "onix_workflow"
DAG_PREFIX = "elastic_import"


@dataclasses.dataclass
class ElasticImportConfig:
    dag_id: str = None
    project_id: str = None
    dataset_id: str = None
    bucket_name: str = None
    data_location: str = None
    file_type: str = None
    sensor_dag_ids: List[str] = None
    kibana_spaces: List[str] = None


configs = [
    ElasticImportConfig(
        dag_id=make_dag_id(DAG_PREFIX, "observatory"),
        project_id="academic-observatory",
        dataset_id=DATASET_ID,
        bucket_name="academic-observatory-transform",
        data_location=DATA_LOCATION,
        file_type=FILE_TYPE_JSONL,
        sensor_dag_ids=["doi"],
        kibana_spaces=["coki-scratch-space", "coki-dashboards", "dev-coki-dashboards"],
    ),
    ElasticImportConfig(
        dag_id=make_dag_id(DAG_PREFIX, "anu_press"),
        project_id="oaebu-anu-press",
        dataset_id=DATASET_ID,
        bucket_name="oaebu-anu-press-transform",
        data_location=DATA_LOCATION,
        file_type=FILE_TYPE_JSONL,
        sensor_dag_ids=[make_dag_id(DAG_ONIX_WORKFLOW_PREFIX, "anu_press")],
        kibana_spaces=["oaebu-anu-press", "dev-oaebu-anu-press"],
    ),
    ElasticImportConfig(
        dag_id=make_dag_id(DAG_PREFIX, "ucl_press"),
        project_id="oaebu-ucl-press",
        dataset_id=DATASET_ID,
        bucket_name="oaebu-ucl-press-transform",
        data_location=DATA_LOCATION,
        file_type=FILE_TYPE_JSONL,
        sensor_dag_ids=[make_dag_id(DAG_ONIX_WORKFLOW_PREFIX, "ucl_press")],
        kibana_spaces=["oaebu-ucl-press", "dev-oaebu-ucl-press"],
    ),
    ElasticImportConfig(
        dag_id=make_dag_id(DAG_PREFIX, "wits_press"),
        project_id="oaebu-witts-press",
        dataset_id=DATASET_ID,
        bucket_name="oaebu-witts-press-transform",
        data_location=DATA_LOCATION,
        file_type=FILE_TYPE_JSONL,
        sensor_dag_ids=[make_dag_id(DAG_ONIX_WORKFLOW_PREFIX, "wits_press")],
        kibana_spaces=["oaebu-wits-press", "dev-oaebu-wits-press"],
    ),
    ElasticImportConfig(
        dag_id=make_dag_id(DAG_PREFIX, "umich_press"),
        project_id="oaebu-umich-press",
        dataset_id=DATASET_ID,
        bucket_name="oaebu-umich-press-transform",
        data_location=DATA_LOCATION,
        file_type=FILE_TYPE_JSONL,
        sensor_dag_ids=[make_dag_id(DAG_ONIX_WORKFLOW_PREFIX, "umich_press")],
        kibana_spaces=["oaebu-umich-press", "dev-oaebu-umich-press"],
    ),
]

for config in configs:
    dag = ElasticImportWorkflow(
        dag_id=config.dag_id,
        project_id=config.project_id,
        dataset_id=config.dataset_id,
        bucket_name=config.bucket_name,
        data_location=config.data_location,
        file_type=config.file_type,
        sensor_dag_ids=config.sensor_dag_ids,
        kibana_spaces=config.kibana_spaces,
    ).make_dag()
    globals()[dag.dag_id] = dag
