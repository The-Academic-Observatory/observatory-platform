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

# Author: Richard Hosking

# The keywords airflow and DAG are required to load the DAGs from this file, see bullet 2 in the Apache Airflow FAQ:
# https://airflow.apache.org/docs/stable/faq.html

from observatory.dags.workflows.oaebu_partners import OaebuPartnerName, OaebuPartners
from observatory.dags.workflows.oapen_workflow import OapenWorkflow

org_name = 'OAPEN'
gcp_project_id = 'oaebu-oapen'
gcp_dataset_id = 'oaebu'

# OAEBU dataset id's
oaebu_dataset= "oaebu"
oaebu_onix_dataset= "oapen_onix"
oaebu_intermediate_dataset= "oaebu_intermediate"
oaebu_elastic_dataset= "data_export"

# IRUS-UK Data
irus_uk_dag_id_prefix="oapen_irus_uk"
irus_uk_dataset_id="oapen"
irus_uk_table_id="oapen_irus_uk"

# Academic Observatory
ao_gcp_project_id = "academic-observatory"

# OAPEN metadata
oapen_metadata_dataset_id = "oapen"
oapen_metadata_table_id = "metadata"

# Public Book Data
public_book_metadata_dataset_id = "observatory"
public_book_metadata_table_id = "book"

oapen_workflow = OapenWorkflow(
    org_name=org_name,
    gcp_project_id=gcp_project_id,
)

globals()[oapen_workflow.dag_id] = oapen_workflow.make_dag()
