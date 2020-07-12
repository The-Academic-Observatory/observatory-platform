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

def aggregate_crossref_events(from_project, from_dataset, from_table):

    sql_path = path.join(path.dirname(path.abspath(__file__)), "..", "sql", "aggregate_crossref_events.sql" )

    with open(sql_path) as f:
        sql_input = f.read()

        from_table = '.'.join([from_project, from_dataset, from_table])
        sql = sql_input.replace("##CROSSREF_EVENTS_RAW##", from_table)

    destiniation_project = "coki-214004"
    destiniation_dataset = "global"
    destiniation_table  = "crossref_events"
    destiniation_location = "US"
    description = "description test"

    labels = {"test": "label"}

    create_bigquery_table_from_query(sql = sql, labels = labels, project_id = destiniation_project, 
                                     dataset_id = destiniation_dataset, table_id = destiniation_table, 
                                     location = destiniation_location, description = description)

aggregate_crossref_events("academic-observatory-telescope", "crossref_events", "crossref_events")