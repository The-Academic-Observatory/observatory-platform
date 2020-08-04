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

import logging
import os

from observatory_platform.utils.config_utils import ObservatoryConfig
from observatory_platform.utils.gc_utils import create_bigquery_table_from_query, \
                                                sql_builder, load_sql_file

WORKFLOW_DATASET="academic-observatory-workflow"
DATA_PRODUCTS_DATASET="academic-observatory"


def run_all():
    # Just for testing, DELETE ME

    destiniation_project = "coki-214004"
    destiniation_dataset = "global"
    destiniation_table = "crossref_events"
    destiniation_location = "US"

    # Crossref events
    db_aggregate_crossref_events("academic-observatory-telescope", destiniation_project, destiniation_dataset, destiniation_table, destiniation_location)

    # Unpaywall
    db_aggregate_crossref_events("academic-observatory-telescope", destiniation_project, destiniation_dataset, destiniation_table, destiniation_location)

    # MAG
    db_aggregate_mag("coki-jamie-dev", "20200605", destiniation_project, destiniation_dataset, destiniation_table, destiniation_location)


def db_extend_grid_with_iso3166_and_home_repos(origin_project, grid_release, destiniation_project, destiniation_location):
    """ Compute the Colour-based Opened access designation of each entry in Unpaywall

    :param origin_project: the GCP project containing the input data
    :param grid_release: the specific release of GRID to use
    :param destiniation_project: the GCP project to save the results
    :param destiniation_location: the GCP data location to save the results
    """

    sql = load_sql_file("extend_grid_with_iso3166_and_home_repos.sql")
    sql = sql_builder(sql=sql, project=origin_project, dataset="grid", tables=["grid"], is_release=True, release=grid_release)
    sql = sql_builder(sql=sql, project=origin_project, dataset="iso3166", tables=["iso3166_countries_and_regions"])
    sql = sql_builder(sql=sql, project=origin_project, dataset="coki", tables=["grid_home_repo"])

    create_bigquery_table_from_query(sql=sql, project_id=destiniation_project,
                                     dataset_id=WORKFLOW_DATASET,
                                     table_id= '_'.join("grid_extended", grid_release),
                                     location=destiniation_location)


def db_aggregate_crossref_events(origin_project, destiniation_project, destiniation_location):
    """ Aggregrate the current state of crossref_events into a table keyed by DOI

    :param origin_project: the GCP project containing the input data
    :param destiniation_project: the GCP project to save the results
    :param destiniation_location: the GCP data location to save the results
    """

    sql = load_sql_file("aggregate_crossref_events.sql")
    sql = sql_builder(sql=sql, project=origin_project, dataset="crossref_events", tables=["crossref_events"])

    create_bigquery_table_from_query(sql=sql, project_id=destiniation_project,
                                     dataset_id=WORKFLOW_DATASET,
                                     table_id="crossref_events_aggregated", 
                                     location=destiniation_location)


def db_aggregate_mag(origin_project, mag_release, destiniation_project, destiniation_location):
    """ Aggregate all the various MAG tables into one keyed off of a DOI

    :param origin_project: the GCP project containing the input data
    :param mag_release: the specific release of mag to use
    :param destiniation_project: the GCP project to save the results
    :param destiniation_location: the GCP data location to save the results
    """

    sql = load_sql_file("aggregate_mag.sql")
    sql = sql_builder(sql=sql, project=origin_project, dataset="mag", 
                      tables=["Papers", "PaperAbstractsInvertedIndex", "PaperFieldsOfStudy", "FieldsOfStudy", "FieldOfStudyExtendedAttributes",
                               "PaperAuthorAffiliations", "Affiliations", "PaperExtendedAttributes", "PaperResources"],
                      is_release=True, release=mag_release)

    create_bigquery_table_from_query(sql=sql, project_id=destiniation_project,
                                     dataset_id=WORKFLOW_DATASET,
                                     table_id='_'.join("mag_aggregated", mag_release), 
                                     location=destiniation_location)


def db_compute_oa_colours_from_unpaywall(origin_project, unpaywall_release, destiniation_project, destiniation_location):
    """ Compute the Colour-based Opened access designation of each entry in Unpaywall

    :param origin_project: the GCP project containing the input data
    :param unpaywall_release: the specific release of unpaywall to use
    :param destiniation_project: the GCP project to save the results
    :param destiniation_location: the GCP data location to save the results
    """

    sql = load_sql_file("compute_oa_colours_from_unpaywall.sql")
    sql = sql_builder(sql=sql, project=origin_project, dataset="unpaywall", tables=["unpaywall"], is_release=True, release=unpaywall_release)

    create_bigquery_table_from_query(sql=sql, project_id=destiniation_project,
                                     dataset_id=WORKFLOW_DATASET,
                                     table_id='_'.join("unpaywall_oa_colours", unpaywall_release), 
                                     location=destiniation_location)


def db_join_crossref_with_funders(origin_project, crossref_release, fundref_release, destiniation_project, destiniation_location):
    """ Join a Crossref release with additional information found within the FundRef dataset

    """

    sql = load_sql_file("join_crossref_with_funders.sql")
    sql = sql_builder(sql=sql, project=origin_project, dataset="crossref", tables=["crossref_metadata"], is_release=True, release=crossref_release)
    sql = sql_builder(sql=sql, project=origin_project, dataset="crossref", tables=["fundref"], is_release=True, release=fundref_release)

    create_bigquery_table_from_query(sql=sql, project_id=destiniation_project,
                                     dataset_id=WORKFLOW_DATASET,
                                     table_id='_'.join("crossref_with_fundref", crossref_release),
                                     location=destiniation_location)


def db_aggregate_open_citations(origin_project, open_citations_release, destiniation_project, destiniation_location):
    """ Aggregate an Open Citations release into a dataset with one row per DOI

    """

    sql = load_sql_file("aggregate_open_citations.sql")
    sql = sql_builder(sql=sql, project=origin_project, dataset="open_citations", tables=["open_citations"], is_release=True, release=open_citations_release)

    create_bigquery_table_from_query(sql=sql, project_id=destiniation_project,
                                     dataset_id=WORKFLOW_DATASET,
                                     table_id='_'.join("open_citations_aggregated", open_citations_release), 
                                     location=destiniation_location)


def db_aggregate_wos(origin_project, destiniation_project, destiniation_location):
    """ Aggregate the current state of WoS

    """

    sql = load_sql_file("aggregate_wos.sql")
    sql = sql_builder(sql=sql, project=origin_project, dataset="wos", tables=["wos"])

    create_bigquery_table_from_query(sql=sql, project_id=destiniation_project,
                                     dataset_id=WORKFLOW_DATASET,
                                     table_id="wos_aggregated", 
                                     location=destiniation_location)


def db_aggregate_scopus(origin_project, destiniation_project, destiniation_location):
    """ Aggregate the current state of Scopus

    """

    sql = load_sql_file("aggregate_scopus.sql")
    sql = sql_builder(sql=sql, project=origin_project, dataset="scopus", tables=["scopus"])

    create_bigquery_table_from_query(sql=sql, project_id=destiniation_project,
                                     dataset_id=WORKFLOW_DATASET,
                                     table_id="scopus_aggregated", 
                                     location=destiniation_location)


def db_create_dois_table(origin_project, crossref_release, unpaywall_release, 
                         mag_release, open_citations_release,                         
                         destiniation_project, destiniation_location):
    """ Aggregate all the previous tables together into a single DOI table as a release

    """

    sql = load_sql_file("create_doi_snapshot.sql")
    sql = sql_builder(sql=sql, project=origin_project, dataset=WORKFLOW_DATASET, tables=['crossref'], is_release=True, release=crossref_release)
    sql = sql_builder(sql=sql, project=origin_project, dataset=WORKFLOW_DATASET, tables=['unpaywall_oa_colours'], is_release=True, release=unpaywall_release)
    sql = sql_builder(sql=sql, project=origin_project, dataset=WORKFLOW_DATASET, tables=['mag_aggregated'], is_release=True, release=mag_release)
    sql = sql_builder(sql=sql, project=origin_project, dataset=WORKFLOW_DATASET, tables=['open_citations_aggregated'], is_release=True, release=open_citations_release)
    sql = sql_builder(sql=sql, project=origin_project, dataset=WORKFLOW_DATASET, tables=['wos_aggregated'])
    sql = sql_builder(sql=sql, project=origin_project, dataset=WORKFLOW_DATASET, tables=['scopus_aggregated'])
    sql = sql_builder(sql=sql, project=origin_project, dataset=WORKFLOW_DATASET, tables=['crossref_events_aggregated'])

    create_bigquery_table_from_query(sql=sql, project_id=destiniation_project,
                                     dataset_id=DATA_PRODUCTS_DATASET,
                                     table_id='dois', 
                                     location=destiniation_location)


class DoiWorkflow():
    """

    """

    DAG_ID = 'doi'
    DESCRIPTION = 'Combining all raw data sources into a linked DOIs dataset'
    TASK_ID_EXTEND_GRID_WITH_ISO3166_AND_HOME_REPOS = 'extend_grid'
    TASK_ID_AGGREGATE_CROSSREF_EVENTS = 'aggregate_crossref'
    TASK_ID_AGGREGATE_MAG = 'aggregate_mag'
    TASK_ID_COMPUTE_OA_COLOURS = 'compute_oa_colours'
    TASK_ID_EXTEND_CROSSREF_FUNDERS = 'extend_crossref'
    TASK_ID_AGGREGATE_OPEN_CITATIONS = 'aggregrate_open_citations'
    TASK_ID_AGGREGATE_WOS = 'aggregrate_wos'
    TASK_ID_AGGREGATE_SCOPUS = 'aggregate_scopus'
    TASK_ID_BUILD_DOIS_TABLE = 'build_dois_table'
    TOPIC_NAME = 'message'

    @staticmethod
    def extend_grid(**kwargs):
        """
        Extend a GRID Release with a list of home_repos and iso3166 information

        :param kwargs:
        :return: the identifier of the task to execute next.
        """

        # Get GRID releases
        ti: TaskInstance = kwargs['ti']
        release = pull_grid_release(ti)

        # Get Observatory Config
        config = ObservatoryConfig.load(os.environ['CONFIG_PATH'])
        project_id = config.project_id
        data_location = config.data_location

        origin_project = "coki-jamie-dev"

        success = db_aggregate_mag(origin_project, release, destiniation_project, destiniation_dataset, destiniation_table, destiniation_location)

        if success:
            logging.info(f'Success Extending GRID release: {release}')
        else:
            logging.error(f"Error Extending GRID release: {release}")
            exit(os.EX_DATAERR)
    

    @staticmethod
    def aggregate_crossref_events(**kwargs):
        """
        Aggregate the current state of Crossref Events into a single table grouped by DOI

        :param kwargs:
        :return: the identifier of the task to execute next.
        """

        # Get Observatory Config
        config = ObservatoryConfig.load(os.environ['CONFIG_PATH'])
        project_id = config.project_id
        data_location = config.data_location
        origin_project = "##fill in this information##"

        success = db_aggregate_crossref_events(origin_project, destiniation_project, destiniation_dataset, destiniation_table, destiniation_location)

        if success:
            logging.info(f'Success Extending GRID release: {release}')
        else:
            logging.error(f"Error Extending GRID release: {release}")
            exit(os.EX_DATAERR)
    

    # MAG
    @staticmethod
    def aggregate_crossref_events(**kwargs):
        """
        Aggregate the current state of Crossref Events into a single table grouped by DOI

        :param kwargs:
        :return: the identifier of the task to execute next.
        """

        # Get Observatory Config
        config = ObservatoryConfig.load(os.environ['CONFIG_PATH'])
        project_id = config.project_id
        data_location = config.data_location
        origin_project = "##fill in this information##"

        success = db_aggregate_crossref_events(origin_project, destiniation_project, destiniation_dataset, destiniation_table, destiniation_location)

        if success:
            logging.info(f'Success Extending GRID release: {release}')
        else:
            logging.error(f"Error Extending GRID release: {release}")
            exit(os.EX_DATAERR)
    

    # OA Colours
    @staticmethod
    def aggregate_crossref_events(**kwargs):
        """
        Aggregate the current state of Crossref Events into a single table grouped by DOI

        :param kwargs:
        :return: the identifier of the task to execute next.
        """

        # Get Observatory Config
        config = ObservatoryConfig.load(os.environ['CONFIG_PATH'])
        project_id = config.project_id
        data_location = config.data_location
        origin_project = "##fill in this information##"

        success = db_aggregate_crossref_events(origin_project, destiniation_project, destiniation_dataset, destiniation_table, destiniation_location)

        if success:
            logging.info(f'Success Extending GRID release: {release}')
        else:
            logging.error(f"Error Extending GRID release: {release}")
            exit(os.EX_DATAERR)


    # Crossref funders
    @staticmethod
    def aggregate_crossref_events(**kwargs):
        """
        Aggregate the current state of Crossref Events into a single table grouped by DOI

        :param kwargs:
        :return: the identifier of the task to execute next.
        """

        # Get Observatory Config
        config = ObservatoryConfig.load(os.environ['CONFIG_PATH'])
        project_id = config.project_id
        data_location = config.data_location
        origin_project = "##fill in this information##"

        success = db_aggregate_crossref_events(origin_project, destiniation_project, destiniation_dataset, destiniation_table, destiniation_location)

        if success:
            logging.info(f'Success Extending GRID release: {release}')
        else:
            logging.error(f"Error Extending GRID release: {release}")
            exit(os.EX_DATAERR)


    # Open Citaitons
    @staticmethod
    def aggregate_crossref_events(**kwargs):
        """
        Aggregate the current state of Crossref Events into a single table grouped by DOI

        :param kwargs:
        :return: the identifier of the task to execute next.
        """

        # Get Observatory Config
        config = ObservatoryConfig.load(os.environ['CONFIG_PATH'])
        project_id = config.project_id
        data_location = config.data_location
        origin_project = "##fill in this information##"

        success = db_aggregate_crossref_events(origin_project, destiniation_project, destiniation_dataset, destiniation_table, destiniation_location)

        if success:
            logging.info(f'Success Extending GRID release: {release}')
        else:
            logging.error(f"Error Extending GRID release: {release}")
            exit(os.EX_DATAERR)


    # WoS
    @staticmethod
    def aggregate_crossref_events(**kwargs):
        """
        Aggregate the current state of Crossref Events into a single table grouped by DOI

        :param kwargs:
        :return: the identifier of the task to execute next.
        """

        # Get Observatory Config
        config = ObservatoryConfig.load(os.environ['CONFIG_PATH'])
        project_id = config.project_id
        data_location = config.data_location
        origin_project = "##fill in this information##"

        success = db_aggregate_crossref_events(origin_project, destiniation_project, destiniation_dataset, destiniation_table, destiniation_location)

        if success:
            logging.info(f'Success Extending GRID release: {release}')
        else:
            logging.error(f"Error Extending GRID release: {release}")
            exit(os.EX_DATAERR)


    # Scopus
    @staticmethod
    def aggregate_crossref_events(**kwargs):
        """
        Aggregate the current state of Crossref Events into a single table grouped by DOI

        :param kwargs:
        :return: the identifier of the task to execute next.
        """

        # Get Observatory Config
        config = ObservatoryConfig.load(os.environ['CONFIG_PATH'])
        project_id = config.project_id
        data_location = config.data_location
        origin_project = "##fill in this information##"

        success = db_aggregate_crossref_events(origin_project, destiniation_project, destiniation_dataset, destiniation_table, destiniation_location)

        if success:
            logging.info(f'Success Extending GRID release: {release}')
        else:
            logging.error(f"Error Extending GRID release: {release}")
            exit(os.EX_DATAERR)


    # Create DOIs table
    @staticmethod
    def aggregate_crossref_events(**kwargs):
        """
        Aggregate the current state of Crossref Events into a single table grouped by DOI

        :param kwargs:
        :return: the identifier of the task to execute next.
        """

        # Get Observatory Config
        config = ObservatoryConfig.load(os.environ['CONFIG_PATH'])
        project_id = config.project_id
        data_location = config.data_location
        origin_project = "##fill in this information##"

        success = db_aggregate_crossref_events(origin_project, destiniation_project, destiniation_dataset, destiniation_table, destiniation_location)

        if success:
            logging.info(f'Success Extending GRID release: {release}')
        else:
            logging.error(f"Error Extending GRID release: {release}")
            exit(os.EX_DATAERR)