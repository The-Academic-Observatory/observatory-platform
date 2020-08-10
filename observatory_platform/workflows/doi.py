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

# Author: Richard Hosking, James Diprose

import logging
import os
from typing import List

import pendulum
from airflow.exceptions import AirflowException
from airflow.models import Variable

from observatory_platform.telescopes.crossref_metadata import CrossrefMetadataTelescope
from observatory_platform.telescopes.fundref import FundrefTelescope
from observatory_platform.telescopes.grid import GridTelescope
from observatory_platform.telescopes.mag import MagTelescope
from observatory_platform.telescopes.unpaywall import UnpaywallTelescope
from observatory_platform.utils.config_utils import AirflowVar, check_variables, workflow_templates_path
from observatory_platform.utils.gc_utils import (bigquery_partitioned_table_id, create_bigquery_table_from_query,
                                                 render_sql_query, sql_jinja2_filename, run_bigquery_query,
                                                 create_bigquery_dataset)


def set_task_state(success: bool, msg_success: str, msg_failed: str):
    if success:
        logging.info(msg_success)
    else:
        logging.error(msg_failed)
        raise AirflowException(msg_failed)


def select_table_suffixes(project_id: str, dataset_id: str, table_id: str, end_date: pendulum.Date,
                          limit: int = 1) -> List:
    template_path = os.path.join(workflow_templates_path(), 'select_table_suffixes.sql.jinja2')
    query = render_sql_query(template_path,
                             project_id=project_id,
                             dataset_id=dataset_id,
                             table_id=table_id,
                             end_date=end_date.strftime('%Y-%m-%d'),
                             limit=limit)
    rows = run_bigquery_query(query)
    suffixes = [row['suffix'] for row in rows]
    return suffixes


class DoiWorkflow:
    DAG_ID = 'doi'
    DESCRIPTION = 'Combining all raw data sources into a linked DOIs dataset'
    TASK_ID_EXTEND_GRID = 'extend_grid'
    TASK_ID_AGGREGATE_CROSSREF_EVENTS = 'aggregate_crossref_events'
    TASK_ID_AGGREGATE_MAG = 'aggregate_mag'
    TASK_ID_AGGREGATE_UNPAYWALL = 'aggregate_unpaywall'
    TASK_ID_EXTEND_CROSSREF_METADATA = 'extend_crossref_metadata'
    TASK_ID_AGGREGATE_OPEN_CITATIONS = 'aggregate_open_citations'
    TASK_ID_AGGREGATE_WOS = 'aggregate_wos'
    TASK_ID_AGGREGATE_SCOPUS = 'aggregate_scopus'
    TASK_ID_CREATE_DOI_SNAPSHOT = 'create_doi_snapshot'
    PROCESSED_DATASET_ID = 'observatory_processed'
    PROCESSED_DATASET_DESCRIPTION = 'Intermediate processing dataset for the Academic Observatory.'
    OBSERVATORY_DATASET_ID = 'observatory'
    OBSERVATORY_DATASET_ID_DATASET_DESCRIPTION = 'The Academic Observatory dataset.'
    TOPIC_NAME = 'message'

    @staticmethod
    def check_dependencies(**kwargs):
        """ Check that all variables and connections exist that are required to run the DAG.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        vars_valid = check_variables(AirflowVar.data_path.get(), AirflowVar.project_id.get(),
                                     AirflowVar.data_location.get(), AirflowVar.download_bucket_name.get(),
                                     AirflowVar.transform_bucket_name.get())

        if not vars_valid:
            raise AirflowException('Required variables are missing')

    @staticmethod
    def extend_grid(**kwargs):
        """ Extend a GRID Release with a list of home_repos and iso3166 information.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        # Get variables
        project_id = Variable.get(AirflowVar.project_id.get())
        data_location = Variable.get(AirflowVar.data_location.get())

        # Create dataset
        create_bigquery_dataset(project_id, DoiWorkflow.PROCESSED_DATASET_ID, data_location,
                                DoiWorkflow.PROCESSED_DATASET_DESCRIPTION)

        end_date = kwargs['next_execution_date'].subtract(microseconds=1).date()
        release_date = select_table_suffixes(project_id, GridTelescope.DATASET_ID, GridTelescope.DAG_ID, end_date)
        if len(release_date):
            release_date = release_date[0]
        else:
            raise AirflowException(f'No GRID release with a table suffix <= {end_date} found')

        # Create processed table
        template_path = os.path.join(workflow_templates_path(), sql_jinja2_filename(DoiWorkflow.TASK_ID_EXTEND_GRID))
        sql = render_sql_query(template_path,
                               project_id=project_id,
                               grid_release_date=release_date)

        processed_table_id = bigquery_partitioned_table_id('grid_extended', end_date)
        success = create_bigquery_table_from_query(sql=sql,
                                                   project_id=project_id,
                                                   dataset_id=DoiWorkflow.PROCESSED_DATASET_ID,
                                                   table_id=processed_table_id,
                                                   location=data_location)

        set_task_state(success,
                       f'{DoiWorkflow.TASK_ID_EXTEND_GRID} success',
                       f'{DoiWorkflow.TASK_ID_EXTEND_GRID} failed')

    @staticmethod
    def aggregate_crossref_events(**kwargs):
        """ Aggregate the current state of Crossref Events into a single table grouped by DOI.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        # Get variables
        project_id = Variable.get(AirflowVar.project_id.get())
        data_location = Variable.get(AirflowVar.data_location.get())
        end_date = kwargs['next_execution_date'].subtract(microseconds=1).date()

        # Create processed table
        template_path = os.path.join(workflow_templates_path(),
                                     sql_jinja2_filename(DoiWorkflow.TASK_ID_AGGREGATE_CROSSREF_EVENTS))
        sql = render_sql_query(template_path, project_id=project_id)
        # TODO: perhaps only include records up until the end date of this query?

        processed_table_id = bigquery_partitioned_table_id('crossref_events', end_date)
        success = create_bigquery_table_from_query(sql=sql,
                                                   project_id=project_id,
                                                   dataset_id=DoiWorkflow.PROCESSED_DATASET_ID,
                                                   table_id=processed_table_id,
                                                   location=data_location)

        set_task_state(success,
                       f'{DoiWorkflow.TASK_ID_AGGREGATE_CROSSREF_EVENTS} success',
                       f'{DoiWorkflow.TASK_ID_AGGREGATE_CROSSREF_EVENTS} failed')

    @staticmethod
    def aggregate_mag(**kwargs):
        """ Aggregate MAG.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        # Get variables
        project_id = Variable.get(AirflowVar.project_id.get())
        data_location = Variable.get(AirflowVar.data_location.get())
        end_date = kwargs['next_execution_date'].subtract(microseconds=1).date()

        # Get last MAG release date before current end date
        table_id = 'Affiliations'
        release_date = select_table_suffixes(project_id, MagTelescope.DATASET_ID, table_id, end_date)
        if len(release_date):
            release_date = release_date[0]
        else:
            raise AirflowException(f'No MAG release with a table suffix <= {end_date} found')

        # Create processed table
        template_path = os.path.join(workflow_templates_path(), sql_jinja2_filename(DoiWorkflow.TASK_ID_AGGREGATE_MAG))
        sql = render_sql_query(template_path,
                               project_id=project_id,
                               release_date=release_date)

        processed_table_id = bigquery_partitioned_table_id(MagTelescope.DAG_ID, end_date)
        success = create_bigquery_table_from_query(sql=sql,
                                                   project_id=project_id,
                                                   dataset_id=DoiWorkflow.PROCESSED_DATASET_ID,
                                                   table_id=processed_table_id,
                                                   location=data_location)

        set_task_state(success,
                       f'{DoiWorkflow.TASK_ID_AGGREGATE_MAG} success',
                       f'{DoiWorkflow.TASK_ID_AGGREGATE_MAG} failed')

    @staticmethod
    def aggregate_unpaywall(**kwargs):
        """ Compute the Open Access colours from Unpaywall.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        # Get variables
        project_id = Variable.get(AirflowVar.project_id.get())
        data_location = Variable.get(AirflowVar.data_location.get())
        end_date = kwargs['next_execution_date'].subtract(microseconds=1).date()

        # Get last Unpaywall release date before current end date
        release_date = select_table_suffixes(project_id, UnpaywallTelescope.DATASET_ID,
                                             UnpaywallTelescope.DAG_ID, end_date)
        if len(release_date):
            release_date = release_date[0]
        else:
            raise AirflowException(f'Unpaywall release with a table suffix <= {end_date} not found')

        # Create processed table
        template_path = os.path.join(workflow_templates_path(),
                                     sql_jinja2_filename(DoiWorkflow.TASK_ID_AGGREGATE_UNPAYWALL))
        sql = render_sql_query(template_path,
                               project_id=project_id,
                               release_date=release_date)

        processed_table_id = bigquery_partitioned_table_id(UnpaywallTelescope.DAG_ID, end_date)
        success = create_bigquery_table_from_query(sql=sql,
                                                   project_id=project_id,
                                                   dataset_id=DoiWorkflow.PROCESSED_DATASET_ID,
                                                   table_id=processed_table_id,
                                                   location=data_location)

        set_task_state(success,
                       f'{DoiWorkflow.TASK_ID_AGGREGATE_UNPAYWALL} success',
                       f'{DoiWorkflow.TASK_ID_AGGREGATE_UNPAYWALL} failed')

    @staticmethod
    def extend_crossref_metadata(**kwargs):
        """ Extend Crossref Metadata with Crossref Events.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        # Get variables
        project_id = Variable.get(AirflowVar.project_id.get())
        data_location = Variable.get(AirflowVar.data_location.get())
        end_date = kwargs['next_execution_date'].subtract(microseconds=1).date()

        # Get last Funref and Crossref Metadata release dates before current end date
        fundref_release_date = select_table_suffixes(project_id, FundrefTelescope.DATASET_ID,
                                                     FundrefTelescope.DAG_ID, end_date)
        crossref_metadata_release_date = select_table_suffixes(project_id, CrossrefMetadataTelescope.DATASET_ID,
                                                               CrossrefMetadataTelescope.DAG_ID, end_date)
        if len(fundref_release_date) and len(crossref_metadata_release_date):
            fundref_release_date = fundref_release_date[0]
            crossref_metadata_release_date = crossref_metadata_release_date[0]
        else:
            raise AirflowException(f'Fundref and Crossref Metadata release with a table suffix <= {end_date} not found')

        # Create processed table
        template_path = os.path.join(workflow_templates_path(),
                                     sql_jinja2_filename(DoiWorkflow.TASK_ID_EXTEND_CROSSREF_METADATA))
        sql = render_sql_query(template_path,
                               project_id=project_id,
                               crossref_metadata_release_date=crossref_metadata_release_date,
                               fundref_release_date=fundref_release_date)

        processed_table_id = bigquery_partitioned_table_id('crossref_metadata_extended', end_date)
        success = create_bigquery_table_from_query(sql=sql,
                                                   project_id=project_id,
                                                   dataset_id=DoiWorkflow.PROCESSED_DATASET_ID,
                                                   table_id=processed_table_id,
                                                   location=data_location)

        set_task_state(success,
                       f'{DoiWorkflow.TASK_ID_EXTEND_CROSSREF_METADATA} success',
                       f'{DoiWorkflow.TASK_ID_EXTEND_CROSSREF_METADATA} failed')

    @staticmethod
    def aggregate_open_citations(**kwargs):
        """ Aggregate Open Citations.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        # Get variables
        project_id = Variable.get(AirflowVar.project_id.get())
        data_location = Variable.get(AirflowVar.data_location.get())
        end_date = kwargs['next_execution_date'].subtract(microseconds=1).date()

        # Get last Open Citations release date before current end date
        release_date = select_table_suffixes(project_id, 'open_citations',
                                             'open_citations', end_date)
        if len(release_date):
            release_date = release_date[0]
        else:
            raise AirflowException(f'Open citations release with a table suffix <= {end_date} not found')

        # Create processed dataset
        template_path = os.path.join(workflow_templates_path(),
                                     sql_jinja2_filename(DoiWorkflow.TASK_ID_AGGREGATE_OPEN_CITATIONS))
        sql = render_sql_query(template_path,
                               project_id=project_id,
                               release_date=release_date)

        processed_table_id = bigquery_partitioned_table_id('open_citations', end_date)
        success = create_bigquery_table_from_query(sql=sql,
                                                   project_id=project_id,
                                                   dataset_id=DoiWorkflow.PROCESSED_DATASET_ID,
                                                   table_id=processed_table_id,
                                                   location=data_location)

        set_task_state(success,
                       f'{DoiWorkflow.TASK_ID_AGGREGATE_OPEN_CITATIONS} success',
                       f'{DoiWorkflow.TASK_ID_AGGREGATE_OPEN_CITATIONS} failed')

    @staticmethod
    def aggregate_wos(**kwargs):
        """ Aggregate Web of Science.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        # Get variables
        project_id = Variable.get(AirflowVar.project_id.get())
        data_location = Variable.get(AirflowVar.data_location.get())
        end_date = kwargs['next_execution_date'].subtract(microseconds=1).date()

        # Create
        template_path = os.path.join(workflow_templates_path(), sql_jinja2_filename(DoiWorkflow.TASK_ID_AGGREGATE_WOS))
        sql = render_sql_query(template_path,
                               project_id=project_id)
        # TODO: only include records up until the end date

        processed_table_id = bigquery_partitioned_table_id('wos', end_date)
        success = create_bigquery_table_from_query(sql=sql,
                                                   project_id=project_id,
                                                   dataset_id=DoiWorkflow.PROCESSED_DATASET_ID,
                                                   table_id=processed_table_id,
                                                   location=data_location)

        set_task_state(success,
                       f'{DoiWorkflow.TASK_ID_AGGREGATE_WOS} success',
                       f'{DoiWorkflow.TASK_ID_AGGREGATE_WOS} failed')

    @staticmethod
    def aggregate_scopus(**kwargs):
        """ Aggregate Scopus.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        # Get variables
        project_id = Variable.get(AirflowVar.project_id.get())
        data_location = Variable.get(AirflowVar.data_location.get())
        end_date = kwargs['next_execution_date'].subtract(microseconds=1).date()

        # Create processed dataset
        template_path = os.path.join(workflow_templates_path(),
                                     sql_jinja2_filename(DoiWorkflow.TASK_ID_AGGREGATE_SCOPUS))
        sql = render_sql_query(template_path,
                               project_id=project_id)

        processed_table_id = bigquery_partitioned_table_id('scopus', end_date)
        success = create_bigquery_table_from_query(sql=sql,
                                                   project_id=project_id,
                                                   dataset_id=DoiWorkflow.PROCESSED_DATASET_ID,
                                                   table_id=processed_table_id,
                                                   location=data_location)

        set_task_state(success,
                       f'{DoiWorkflow.TASK_ID_AGGREGATE_SCOPUS} success',
                       f'{DoiWorkflow.TASK_ID_AGGREGATE_SCOPUS} failed')

    # @staticmethod
    # def create_doi_snapshot(**kwargs):
    #     """ Create DOI snapshot.
    #
    #     :param kwargs: the context passed from the PythonOperator. See
    #     https://airflow.apache.org/docs/stable/macros-ref.html
    #     for a list of the keyword arguments that are passed to this argument.
    #     :return: None.
    #     """
    #
    #     # Get variables
    #     project_id = Variable.get(AirflowVar.project_id.get())
    #     data_location = Variable.get(AirflowVar.data_location.get())
    #     release_date = ''  # todays date?
    #
    #     # Create
    #     template_path = os.path.join(workflow_templates_path(),
    #                                  sql_jinja2_filename(DoiWorkflow.TASK_ID_CREATE_DOI_SNAPSHOT))
    #     sql = render_query(template_path,
    #                        project_id=project_id,
    #                        dataset_id='')
    #
    #     processed_dataset_id = DoiWorkflow.PROCESSED_DATASET_ID
    #     processed_table_id = bigquery_partitioned_table_id('', release_date)
    #     success = create_bigquery_table_from_query(sql=sql,
    #                                                project_id=project_id,
    #                                                dataset_id=processed_dataset_id,
    #                                                table_id=processed_table_id,
    #                                                location=data_location)
    #
    #     set_task_state(success,
    #                    f'{DoiWorkflow.TASK_ID_CREATE_DOI_SNAPSHOT} success',
    #                    f'{DoiWorkflow.TASK_ID_CREATE_DOI_SNAPSHOT} failed')
