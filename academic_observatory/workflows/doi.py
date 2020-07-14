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

from academic_observatory.utils.gc_utils import create_bigquery_table_from_query, sql_builder, load_sql_file

def run_all():

    destiniation_project = "coki-214004"
    destiniation_dataset = "global"
    destiniation_table  = "crossref_events"
    destiniation_location = "US"

    # Crossref events
    aggregate_crossref_events("academic-observatory-telescope", 
                              destiniation_project, destiniation_dataset, destiniation_table, destiniation_location)
    
    # Unpaywall
    aggregate_crossref_events("academic-observatory-telescope", 
                              destiniation_project, destiniation_dataset, destiniation_table, destiniation_location)

    # MAG
    aggregate_mag("coki-jamie-dev", "20200605", 
                  destiniation_project, destiniation_dataset, destiniation_table, destiniation_location)


def aggregate_crossref_events(from_project, destiniation_project, destiniation_dataset, destiniation_table, destiniation_location):
    """ Aggregrate the current state of crossref_events into a table keyed by DOI

    """

    sql = load_sql_file("aggregate_crossref_events.sql")
    sql = sql_builder(sql = sql, project = from_project, dataset = "crossref_events", tables = ["crossref_events"])

    create_bigquery_table_from_query(sql = sql, project_id = destiniation_project, 
                                     dataset_id = destiniation_dataset, table_id = destiniation_table, 
                                     location = destiniation_location)


def compute_oa_colours_from_unpaywall(from_project, from_release, destiniation_project, 
                                      destiniation_dataset, destiniation_table, destiniation_location):
    """ Compute the Colour-based Opened access designation of each entry in Unpaywall

    """

    sql = load_sql_file("compute_oa_colours_from_unpaywall.sql")
    sql = sql_builder(sql = sql, project = from_project, dataset = "unpaywall", tables = ["unpaywall"], is_release = True, release = from_release)

    create_bigquery_table_from_query(sql = sql, project_id = destiniation_project, 
                                     dataset_id = destiniation_dataset, table_id = destiniation_table, 
                                     location = destiniation_location)


def aggregate_mag(from_project, from_release, destiniation_project, 
                  destiniation_dataset, destiniation_table, destiniation_location):
    """ Aggregate all the various MAG tables into one keyed off of a DOI

    """

    sql = load_sql_file("aggregate_mag.sql")
    sql = sql_builder(sql = sql, project = from_project, dataset = "mag", 
                      tables = ["Papers", "PaperAbstractsInvertedIndex", "PaperFieldsOfStudy", "FieldsOfStudy", "FieldOfStudyExtendedAttributes", 
                               "PaperAuthorAffiliations", "Affiliations", "PaperExtendedAttributes", "PaperResources"], 
                      is_release = True, release = from_release)

    create_bigquery_table_from_query(sql = sql, project_id = destiniation_project, 
                                     dataset_id = destiniation_dataset, table_id = destiniation_table, 
                                     location = destiniation_location)



def supplment_crossref_funders(from_project, from_dataset, from_table, destiniation_project, 
                                      destiniation_dataset, destiniation_table, destiniation_location):
    """ Compute the Colour-based Opened access designation of each entry in Unpaywall

    """

    sql = load_sql_file("extend_crossref_funders.sql")
    sql = sql_builder(sql = sql, project = from_project, dataset = from_dataset, tables = [from_table], is_release = True, release = from_release)

    create_bigquery_table_from_query(sql = sql, project_id = destiniation_project, 
                                     dataset_id = destiniation_dataset, table_id = destiniation_table, 
                                     location = destiniation_location)



def extend_grid_with_iso3166_and_home_repos(from_project, from_dataset, from_table, destiniation_project, 
                                      destiniation_dataset, destiniation_table, destiniation_location):
    """ Compute the Colour-based Opened access designation of each entry in Unpaywall

    """

    sql = load_sql_file("extend_grid_with_iso3166_and_home_repos.sql")
    sql = sql_builder(sql = sql, project = from_project, dataset = "grid", tables = ["grid"], is_release = True, release = from_release)
    sql = sql_builder(sql = sql, project = from_project, dataset = "iso3611", tables = ["iso_3611"], is_release = True, release = from_release)
    sql = sql_builder(sql = sql, project = from_project, dataset = "coki", tables = ["grid_home_repo"], is_release = True, release = from_release)

    create_bigquery_table_from_query(sql = sql, project_id = destiniation_project, 
                                     dataset_id = destiniation_dataset, table_id = destiniation_table, 
                                     location = destiniation_location)



class DoiWorkflow():
    """

    """

    DAG_ID = 'doi'
    DESCRIPTION = 'Combining all raw data sources into a linked DOIs dataset'
    TASK_ID_LIST = f'{DAG_ID}_list_releases'
    TASK_ID_TRANSFER = f'{DAG_ID}_transfer'
    TASK_ID_DOWNLOAD = f'{DAG_ID}_download'
    TASK_ID_TRANSFORM = f'{DAG_ID}_transform'
    TASK_ID_UPLOAD = f'{DAG_ID}_upload'
    TASK_ID_DB_LOAD = f'{DAG_ID}_db_load'
    TASK_ID_STOP = f'{DAG_ID}_stop'
    TOPIC_NAME = 'message'
    MAX_PROCESSES = cpu_count()
    MAX_CONNECTIONS = cpu_count()
    MAX_RETRIES = 3