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


from observatory.dags.workflows.elastic_import_workflow import ElasticImportWorkflow

DAG_ID = 'observatory_elastic_import'
EXPORT_DATASET_ID = 'observatory_elastic'
EXPORT_FILE_TYPE = 'csv'

workflow = ElasticImportWorkflow(export_dataset_id=EXPORT_DATASET_ID,
                                 export_file_type=EXPORT_FILE_TYPE,
                                 dag_id=DAG_ID).make_dag()

globals()[workflow.dag_id] = workflow
