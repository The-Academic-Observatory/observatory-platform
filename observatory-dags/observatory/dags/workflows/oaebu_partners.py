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
#
#
# Author: Tuan Chien


from dataclasses import dataclass


class OaebuPartnerName:
    """ OAEBU partner name constants. """

    google_analytics = "Google Analytics"
    google_books_sales = "Google Books Sales"
    google_books_traffic = "Google Books Traffic"
    jstor_country = "JSTOR Country"
    jstor_institution = "JSTOR Institution"
    oapen_irus_uk = "OAPEN IRUS UK"
    ucl_discovery = "UCL Discovery"


@dataclass
class OaebuPartners:
    """Temporary class for storing information about data sources we are using to produce oaebu intermediate tables for.  Change or remove this later when Observatory API is more mature.

    :param name: Name of the data partner.
    :param dag_id_prefix: The prefix of the DAG id that the data originates from.
    :param gcp_project_id: GCP Project ID.
    :param gcp_dataset_id: GCP Dataset ID.
    :param gcp_table_id: Table name without the date suffix.
    :param isbn_field_name: Name of the field containing the ISBN.
    :param title_field_name: Name of the field containing the Title.
    :param sharded: whether the table is sharded or not.
    """

    name: str
    dag_id_prefix: str
    gcp_project_id: str
    gcp_dataset_id: str
    gcp_table_id: str
    isbn_field_name: str
    title_field_name: str
    sharded: bool
