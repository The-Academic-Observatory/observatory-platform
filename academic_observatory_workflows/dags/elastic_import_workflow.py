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
from typing import List

from academic_observatory_workflows.config import elastic_mappings_folder
from observatory.platform.elastic.kibana import TimeField
from observatory.platform.utils.jinja2_utils import render_template
from observatory.platform.utils.workflow_utils import make_dag_id
from observatory.platform.workflows.elastic_import_workflow import (
    ElasticImportWorkflow,
    ElasticImportConfig,
    load_elastic_mappings_simple,
)

DATASET_ID = "data_export"
DATA_LOCATION = "us"
FILE_TYPE_JSONL = "jsonl.gz"
DAG_ONIX_WORKFLOW_PREFIX = "onix_workflow"
DAG_PREFIX = "elastic_import"
ELASTIC_MAPPINGS_PATH = elastic_mappings_folder()
AO_KIBANA_TIME_FIELDS = [TimeField("^.*$", "published_year")]


def load_elastic_mappings_ao(path: str, table_prefix: str, simple_prefixes: List = None):
    """For the Observatory project, load the Elastic mappings for a given table_prefix.
    :param path: the path to the mappings files.
    :param table_prefix: the table_id prefix (without shard date).
    :param simple_prefixes: the prefixes of mappings to load with the load_elastic_mappings_simple function.
    :return: the rendered mapping as a Dict.
    """

    # Set default simple_prefixes
    if simple_prefixes is None:
        simple_prefixes = ["ao_doi"]

    if not table_prefix.startswith("ao"):
        raise ValueError("Table must begin with 'ao'")
    elif any([table_prefix.startswith(prefix) for prefix in simple_prefixes]):
        return load_elastic_mappings_simple(path, table_prefix)
    else:
        prefix, aggregate, facet = table_prefix.split("_", 2)
        mappings_file_name = "ao-relations-mappings.json.jinja2"
        is_fixed_facet = facet in ["unique_list", "access_types", "disciplines", "output_types", "events", "metrics"]
        if is_fixed_facet:
            mappings_file_name = f"ao-{facet.replace('_', '-')}-mappings.json.jinja2"
        mappings_path = os.path.join(path, mappings_file_name)
        return json.loads(render_template(mappings_path, aggregate=aggregate, facet=facet))


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
        elastic_mappings_path=ELASTIC_MAPPINGS_PATH,
        elastic_mappings_func=load_elastic_mappings_ao,
        kibana_time_fields=AO_KIBANA_TIME_FIELDS,
    )
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
