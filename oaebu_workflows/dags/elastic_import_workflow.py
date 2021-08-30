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

import json
import os
from typing import Dict

from observatory.platform.elastic.kibana import TimeField
from observatory.platform.utils.jinja2_utils import render_template
from observatory.platform.utils.workflow_utils import make_dag_id
from observatory.platform.workflows.elastic_import_workflow import ElasticImportWorkflow, ElasticImportConfig

from oaebu_workflows.config import elastic_mappings_folder

DATASET_ID = "data_export"
DATA_LOCATION = "us"
FILE_TYPE_JSONL = "jsonl.gz"
DAG_ONIX_WORKFLOW_PREFIX = "onix_workflow"
DAG_PREFIX = "elastic_import"
ELASTIC_MAPPINGS_PATH = elastic_mappings_folder()
OAEBU_KIBANA_TIME_FIELDS = [
    TimeField("^oaebu-.*-unmatched-book-metrics$", "release_date"),
    TimeField("^oaebu-.*-book-product-list$", "time_field"),
    TimeField("^oaebu-.*$", "month"),
]


def load_elastic_mappings_oaebu(path: str, table_prefix: str) -> Dict:
    """For the OAEBU project, load the Elastic mappings for a given table_prefix.
    :param path: the path to the mappings files.
    :param table_prefix: the table_id prefix (without shard date).
    :return: the rendered mapping as a Dict.
    """

    if not table_prefix.startswith("oaebu"):
        raise ValueError("Table must begin with 'oaebu'")
    elif "unmatched" in table_prefix:
        mappings_path = os.path.join(path, "oaebu-unmatched-metrics-mappings.json.jinja2")
        return json.loads(render_template(mappings_path))
    else:
        parts = table_prefix.split("_")[3:]
        mappings_file_name = "oaebu" + "-" + "-".join(parts[2:]) + "-mappings.json.jinja2"
        mappings_path = os.path.join(path, mappings_file_name)
        aggregation_level = parts[1]
        return json.loads(render_template(mappings_path, aggregation_level=aggregation_level))


configs = [
    ElasticImportConfig(
        dag_id=make_dag_id(DAG_PREFIX, "anu_press"),
        project_id="oaebu-anu-press",
        dataset_id=DATASET_ID,
        bucket_name="oaebu-anu-press-transform",
        data_location=DATA_LOCATION,
        file_type=FILE_TYPE_JSONL,
        sensor_dag_ids=[make_dag_id(DAG_ONIX_WORKFLOW_PREFIX, "anu_press")],
        elastic_mappings_path=ELASTIC_MAPPINGS_PATH,
        elastic_mappings_func=load_elastic_mappings_oaebu,
        kibana_spaces=["oaebu-anu-press", "dev-oaebu-anu-press"],
        kibana_time_fields=OAEBU_KIBANA_TIME_FIELDS,
    ),
    ElasticImportConfig(
        dag_id=make_dag_id(DAG_PREFIX, "ucl_press"),
        project_id="oaebu-ucl-press",
        dataset_id=DATASET_ID,
        bucket_name="oaebu-ucl-press-transform",
        data_location=DATA_LOCATION,
        file_type=FILE_TYPE_JSONL,
        sensor_dag_ids=[make_dag_id(DAG_ONIX_WORKFLOW_PREFIX, "ucl_press")],
        elastic_mappings_path=ELASTIC_MAPPINGS_PATH,
        elastic_mappings_func=load_elastic_mappings_oaebu,
        kibana_spaces=["oaebu-ucl-press", "dev-oaebu-ucl-press"],
        kibana_time_fields=OAEBU_KIBANA_TIME_FIELDS,
    ),
    ElasticImportConfig(
        dag_id=make_dag_id(DAG_PREFIX, "wits_press"),
        project_id="oaebu-witts-press",
        dataset_id=DATASET_ID,
        bucket_name="oaebu-witts-press-transform",
        data_location=DATA_LOCATION,
        file_type=FILE_TYPE_JSONL,
        sensor_dag_ids=[make_dag_id(DAG_ONIX_WORKFLOW_PREFIX, "wits_press")],
        elastic_mappings_path=ELASTIC_MAPPINGS_PATH,
        elastic_mappings_func=load_elastic_mappings_oaebu,
        kibana_spaces=["oaebu-wits-press", "dev-oaebu-wits-press"],
        kibana_time_fields=OAEBU_KIBANA_TIME_FIELDS,
    ),
    ElasticImportConfig(
        dag_id=make_dag_id(DAG_PREFIX, "university_of_michigan_press"),
        project_id="oaebu-umich-press",
        dataset_id=DATASET_ID,
        bucket_name="oaebu-umich-press-transform",
        data_location=DATA_LOCATION,
        file_type=FILE_TYPE_JSONL,
        sensor_dag_ids=[make_dag_id(DAG_ONIX_WORKFLOW_PREFIX, "university_of_michigan_press")],
        elastic_mappings_path=ELASTIC_MAPPINGS_PATH,
        elastic_mappings_func=load_elastic_mappings_oaebu,
        kibana_spaces=["oaebu-umich-press", "dev-oaebu-umich-press"],
        kibana_time_fields=OAEBU_KIBANA_TIME_FIELDS,
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
        elastic_mappings_folder=ELASTIC_MAPPINGS_PATH,
        elastic_mappings_func=config.elastic_mappings_func,
        kibana_spaces=config.kibana_spaces,
        kibana_time_fields=config.kibana_time_fields,
    ).make_dag()
    globals()[dag.dag_id] = dag
