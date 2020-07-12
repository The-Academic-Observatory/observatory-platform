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

# Author: Richard Hosking

from os import path
from google.cloud import bigquery

from academic_observatory.utils.gc_utils import create_bigquery_table_from_query

def aggregate_mag(from_project, release):

    sql_path = path.join(path.dirname(path.abspath(__file__)), "..", "sql", "aggregate_mag.sql" )

    with open(sql_path) as f:
        sql = f.read()
        sql = sql.replace("##PAPERS##", '.'.join([from_project, "mag", "Papers" + release]))
        sql = sql.replace("##PAPER_ABSTRACTS_INVERTED_INDEX##", '.'.join([from_project, "mag", "PaperAbstractsInvertedIndex" + release]))
        sql = sql.replace("##PAPER_FIELDS_OF_STUDY##", '.'.join([from_project, "mag", "PaperFieldsOfStudy" + release]))
        sql = sql.replace("##FIELDS_OF_STUDY##", '.'.join([from_project, "mag", "FieldsOfStudy" + release]))
        sql = sql.replace("##FIELDS_OF_STUDY_EXTENDED_ATTRIBUTES##", '.'.join([from_project, "mag", "FieldOfStudyExtendedAttributes" + release]))
        sql = sql.replace("##PAPER_AUTHOR_AFFILIATIONS##", '.'.join([from_project, "mag", "PaperAuthorAffiliations" + release]))
        sql = sql.replace("##AFFILIATIONS##", '.'.join([from_project, "mag", "Affiliations" + release]))
        sql = sql.replace("##PAPER+EXTENDED_ATTRIBUTES##", '.'.join([from_project, "mag", "PaperExtendedAttributes" + release]))
        sql = sql.replace("##PAPER_RESOURCES##", '.'.join([from_project, "mag", "PaperResources" + release]))


    destiniation_project = "coki-214004"
    destiniation_dataset = "test"
    destiniation_table  = "mag"
    destiniation_location = "us-west4"
    description = "description test"

    labels = {"test": "label"}

    create_bigquery_table_from_query(sql = sql, labels = labels, project_id = destiniation_project, 
                                     dataset_id = destiniation_dataset, table_id = destiniation_table, 
                                     location = destiniation_location, description = description)

aggregate_mag("coki-jamie-dev", "20200605")