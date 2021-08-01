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

# Author: Tuan Chien

# The keywords airflow and DAG are required to load the DAGs from this file, see bullet 2 in the Apache Airflow FAQ:
# https://airflow.apache.org/docs/stable/faq.html


from observatory.api.client.identifiers import TelescopeTypes
from observatory.dags.workflows.oaebu_partners import OaebuPartnerName, OaebuPartners
from observatory.dags.workflows.onix_workflow import OnixWorkflow
from observatory.platform.utils.telescope_utils import make_observatory_api


# Temporary function. Create oaebu partner metadata.
# Get rid of this when we change the Observatory API.
def get_oaebu_partner_data(project_id, org_name):

    oaebu_data = [
        OaebuPartners(
            name=OaebuPartnerName.google_analytics,
            dag_id_prefix="google_analytics",
            gcp_project_id=project_id,
            gcp_dataset_id="google",
            gcp_table_id="google_analytics",
            isbn_field_name="publication_id",
            sharded=False,
        ),
        OaebuPartners(
            name=OaebuPartnerName.google_books_sales,
            dag_id_prefix="google_books",
            gcp_project_id=project_id,
            gcp_dataset_id="google",
            gcp_table_id="google_books_sales",
            isbn_field_name="Primary_ISBN",
            sharded=False,
        ),
        OaebuPartners(
            name=OaebuPartnerName.google_books_traffic,
            dag_id_prefix="google_books",
            gcp_project_id=project_id,
            gcp_dataset_id="google",
            gcp_table_id="google_books_traffic",
            isbn_field_name="Primary_ISBN",
            sharded=False,
        ),
        OaebuPartners(
            name=OaebuPartnerName.jstor_country,
            dag_id_prefix="jstor",
            gcp_project_id=project_id,
            gcp_dataset_id="jstor",
            gcp_table_id="jstor_country",
            isbn_field_name="eISBN",
            sharded=False,
        ),
        OaebuPartners(
            name=OaebuPartnerName.jstor_institution,
            dag_id_prefix="jstor",
            gcp_project_id=project_id,
            gcp_dataset_id="jstor",
            gcp_table_id="jstor_institution",
            isbn_field_name="eISBN",
            sharded=False,
        ),
        OaebuPartners(
            name=OaebuPartnerName.oapen_irus_uk,
            dag_id_prefix="oapen_irus_uk",
            gcp_project_id=project_id,
            gcp_dataset_id="oapen",
            gcp_table_id="oapen_irus_uk",
            isbn_field_name="ISBN",
            sharded=False,
        ),
        OaebuPartners(
            name=OaebuPartnerName.ucl_discovery,
            dag_id_prefix="ucl_discovery",
            gcp_project_id=project_id,
            gcp_dataset_id="ucl",
            gcp_table_id="ucl_discovery",
            isbn_field_name="ISBN",
            sharded=False,
        ),
    ]

    publisher_to_provider_mapping = {
        "ANU Press": [
            OaebuPartnerName.google_analytics,
            OaebuPartnerName.google_books_sales,
            OaebuPartnerName.google_books_traffic,
            OaebuPartnerName.jstor_country,
            OaebuPartnerName.jstor_institution,
            OaebuPartnerName.oapen_irus_uk,
        ],
        "UCL Press": [
            OaebuPartnerName.google_books_sales,
            OaebuPartnerName.google_books_traffic,
            OaebuPartnerName.jstor_country,
            OaebuPartnerName.jstor_institution,
            OaebuPartnerName.oapen_irus_uk,
            OaebuPartnerName.ucl_discovery,
        ],
        "University of Michigan Press": [
            OaebuPartnerName.google_books_sales,
            OaebuPartnerName.google_books_traffic,
            OaebuPartnerName.jstor_country,
            OaebuPartnerName.jstor_institution,
            OaebuPartnerName.oapen_irus_uk,
        ],
        "Wits University Press": [
            OaebuPartnerName.jstor_country,
            OaebuPartnerName.jstor_institution,
            OaebuPartnerName.oapen_irus_uk,
        ],
    }

    publisher_data_partners = list()

    for data in oaebu_data:
        if data.name in publisher_to_provider_mapping[org_name]:
            publisher_data_partners.append(data)

    return publisher_data_partners


# Fetch all ONIX telescopes
api = make_observatory_api()
telescope_type = api.get_telescope_type(type_id=TelescopeTypes.onix)
telescopes = api.get_telescopes(telescope_type_id=telescope_type.id, limit=1000)

# Create workflows for each organisation
for telescope in telescopes:
    org_name = telescope.organisation.name
    gcp_project_id = telescope.organisation.gcp_project_id
    gcp_bucket_name = telescope.organisation.gcp_transform_bucket

    data_partners = get_oaebu_partner_data(gcp_project_id, org_name)

    onix_workflow = OnixWorkflow(
        org_name=org_name, gcp_project_id=gcp_project_id, gcp_bucket_name=gcp_bucket_name, data_partners=data_partners,
    )

    globals()[onix_workflow.dag_id] = onix_workflow.make_dag()
