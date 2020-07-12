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

from academic_observatory.utils.gc_utils import create_bigquery_table_from_query

def run_all():

    destiniation_project = "coki-214004"
    destiniation_dataset = "global"
    destiniation_table  = "crossref_events"
    destiniation_location = "US"

    # Crossref events
    aggregate_crossref_events("academic-observatory-telescope", "crossref_events", "crossref_events", 
                              destiniation_project, destiniation_dataset, destiniation_table, destiniation_location)
    
    # Unpaywall
    aggregate_crossref_events("academic-observatory-telescope", "unpaywall", "unpaywall", 
                              destiniation_project, destiniation_dataset, destiniation_table, destiniation_location)

    # MAG
    aggregate_mag("coki-jamie-dev", "20200605", 
                  destiniation_project, destiniation_dataset, destiniation_table, destiniation_location)


def aggregate_crossref_events(from_project, from_dataset, from_table, destiniation_project, 
                              destiniation_dataset, destiniation_table, destiniation_location):
    """ Aggregrate the current state of crossref_events into a table keyed by DOI

    """

    sql_params = {"CROSSREF_EVENTS": '.'.join([from_project, from_dataset, from_table])}
    sql = sql_builder("aggregate_crossref_events.sql", sql_params)

    create_bigquery_table_from_query(sql = sql, project_id = destiniation_project, 
                                     dataset_id = destiniation_dataset, table_id = destiniation_table, 
                                     location = destiniation_location)


def compute_oa_colours_from_unpaywall(from_project, from_dataset, from_table, destiniation_project, 
                                      destiniation_dataset, destiniation_table, destiniation_location):
    """ Compute the Colour-based Opened access designation of each entry in Unpaywall

    """

    sql_params = {"UNPAYWALL": '.'.join([from_project, from_dataset, from_table])}
    sql = sql_builder("compute_oa_colours_from_unpaywall.sql", sql_params)

    create_bigquery_table_from_query(sql = sql, project_id = destiniation_project, 
                                     dataset_id = destiniation_dataset, table_id = destiniation_table, 
                                     location = destiniation_location)


def aggregate_mag(from_project, release, destiniation_project, 
                  destiniation_dataset, destiniation_table, destiniation_location):
    """ Aggregate all the various MAG tables into one keyed off of a DOI

    """

    sql_params = {
        "PAPERS": '.'.join([from_project, "mag", "Papers" + release]),
        "PAPER_ABSTRACTS_INVERTED_INDEX": '.'.join([from_project, "mag", "PaperAbstractsInvertedIndex" + release]),
        "PAPER_FIELDS_OF_STUDY": '.'.join([from_project, "mag", "PaperFieldsOfStudy" + release]),
        "FIELDS_OF_STUDY": '.'.join([from_project, "mag", "FieldsOfStudy" + release]),
        "FIELDS_OF_STUDY_EXTENDED_ATTRIBUTES": '.'.join([from_project, "mag", "FieldOfStudyExtendedAttributes" + release]),
        "PAPER_AUTHOR_AFFILIATIONS": '.'.join([from_project, "mag", "PaperAuthorAffiliations" + release]),
        "AFFILIATIONS": '.'.join([from_project, "mag", "Affiliations" + release]),
        "PAPER_EXTENDED_ATTRIBUTES": '.'.join([from_project, "mag", "PaperExtendedAttributes" + release]),
        "PAPER_RESOURCES": '.'.join([from_project, "mag", "PaperResources" + release])
        }
    sql = sql_builder("aggregate_mag.sql", sql_params)

    create_bigquery_table_from_query(sql = sql, project_id = destiniation_project, 
                                     dataset_id = destiniation_dataset, table_id = destiniation_table, 
                                     location = destiniation_location)



def supplment_crossref_funders(from_project, from_dataset, from_table, destiniation_project, 
                                      destiniation_dataset, destiniation_table, destiniation_location):
    """ Compute the Colour-based Opened access designation of each entry in Unpaywall

    """

    sql_params = {"CROSSREF": '.'.join([from_project, from_dataset, from_table])}
    sql = sql_builder("extend_crossref_funders.sql", sql_params)

    create_bigquery_table_from_query(sql = sql, project_id = destiniation_project, 
                                     dataset_id = destiniation_dataset, table_id = destiniation_table, 
                                     location = destiniation_location)



def extend_grid_with_iso3166_and_home_repos(from_project, from_dataset, from_table, destiniation_project, 
                                      destiniation_dataset, destiniation_table, destiniation_location):
    """ Compute the Colour-based Opened access designation of each entry in Unpaywall

    """

    sql_params = {
        "GRID": '.'.join([from_project, "mag", "Papers" + release]),
        "ISO_3166": '.'.join([from_project, "mag", "PaperAbstractsInvertedIndex" + release]),
        "GRID_HOME_REPOS": '.'.join([from_project, "mag", "PaperFieldsOfStudy" + release]),
        }

    sql = sql_builder("extend_grid_with_iso3166_and_home_repos.sql", sql_params)

    create_bigquery_table_from_query(sql = sql, project_id = destiniation_project, 
                                     dataset_id = destiniation_dataset, table_id = destiniation_table, 
                                     location = destiniation_location)