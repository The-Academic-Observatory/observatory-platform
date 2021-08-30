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

# Author: James Diprose

"""
A DAG that harvests the Microsoft Academic Graph (MAG) dataset: https://www.microsoft.com/en-us/research/project/microsoft-academic-graph/

Saved to the BigQuery tables:
    <project_id>.mag.AffiliationsYYYYMMDD
    <project_id>.mag.AuthorExtendedAttributesYYYYMMDD
    <project_id>.mag.AuthorsYYYYMMDD
    <project_id>.mag.ConferenceInstancesYYYYMMDD
    <project_id>.mag.ConferenceSeriesYYYYMMDD
    <project_id>.mag.EntityRelatedEntitiesYYYYMMDD
    <project_id>.mag.FieldOfStudyChildrenYYYYMMDD
    <project_id>.mag.FieldOfStudyExtendedAttributesYYYYMMDD
    <project_id>.mag.FieldsOfStudyYYYYMMDD
    <project_id>.mag.JournalsYYYYMMDD
    <project_id>.mag.PaperAbstractsInvertedIndexYYYYMMDD
    <project_id>.mag.PaperAuthorAffiliationsYYYYMMDD
    <project_id>.mag.PaperCitationContextsYYYYMMDD
    <project_id>.mag.PaperExtendedAttributesYYYYMMDD
    <project_id>.mag.PaperFieldsOfStudyYYYYMMDD
    <project_id>.mag.PaperRecommendationsYYYYMMDD
    <project_id>.mag.PaperReferencesYYYYMMDD
    <project_id>.mag.PaperResourcesYYYYMMDD
    <project_id>.mag.PapersYYYYMMDD
    <project_id>.mag.PaperUrlsYYYYMMDD
    <project_id>.mag.RelatedFieldOfStudyYYYYMMDD
"""

import pendulum
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, ShortCircuitOperator
from academic_observatory_workflows.workflows.mag_telescope import MagTelescope

default_args = {"owner": "airflow", "start_date": pendulum.datetime(2020, 7, 1)}

with DAG(dag_id=MagTelescope.DAG_ID, schedule_interval="@weekly", default_args=default_args, max_active_runs=1) as dag:
    # Check that dependencies exist before starting
    check = PythonOperator(
        task_id=MagTelescope.TASK_ID_CHECK_DEPENDENCIES,
        python_callable=MagTelescope.check_dependencies,
        provide_context=True,
        queue=MagTelescope.QUEUE,
    )

    # List releases and skip all subsequent tasks if there is no release to process
    list_releases = ShortCircuitOperator(
        task_id=MagTelescope.TASK_ID_LIST,
        python_callable=MagTelescope.list_releases,
        provide_context=True,
        queue=MagTelescope.QUEUE,
    )

    # Transfer all MAG releases to Google Cloud storage that were processed in the given interval
    transfer = PythonOperator(
        task_id=MagTelescope.TASK_ID_TRANSFER,
        python_callable=MagTelescope.transfer,
        provide_context=True,
        queue=MagTelescope.QUEUE,
    )

    # Download all MAG releases for a given interval
    download = PythonOperator(
        task_id=MagTelescope.TASK_ID_DOWNLOAD,
        python_callable=MagTelescope.download,
        provide_context=True,
        queue=MagTelescope.QUEUE,
    )

    # Transform all MAG releases for a given interval
    transform = PythonOperator(
        task_id=MagTelescope.TASK_ID_TRANSFORM,
        python_callable=MagTelescope.transform,
        provide_context=True,
        queue=MagTelescope.QUEUE,
    )

    # Upload all transformed MAG releases for a given interval to Google Cloud
    upload_transformed = PythonOperator(
        task_id=MagTelescope.TASK_ID_UPLOAD_TRANSFORMED,
        python_callable=MagTelescope.upload_transformed,
        provide_context=True,
        queue=MagTelescope.QUEUE,
        retries=MagTelescope.RETRIES,
    )

    # Load all MAG releases for a given interval to BigQuery
    bq_load = PythonOperator(
        task_id=MagTelescope.TASK_ID_BQ_LOAD,
        python_callable=MagTelescope.bq_load,
        provide_context=True,
        queue=MagTelescope.QUEUE,
    )

    # Cleanup local files
    cleanup = PythonOperator(
        task_id=MagTelescope.TASK_ID_CLEANUP,
        python_callable=MagTelescope.cleanup,
        provide_context=True,
        queue=MagTelescope.QUEUE,
    )

    check >> list_releases >> transfer >> download >> transform >> upload_transformed >> bq_load >> cleanup
