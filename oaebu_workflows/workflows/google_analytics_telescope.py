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

# Author: Aniek Roelofs

from __future__ import annotations

import logging
import os
from datetime import timedelta
from typing import Dict, List, Optional, Tuple

import pendulum
from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.hooks.base import BaseHook
from google.cloud import bigquery
from googleapiclient.discovery import Resource, build
from oauth2client.service_account import ServiceAccountCredentials

from observatory.api.client.model.organisation import Organisation
from oaebu_workflows.config import schema_folder as default_schema_folder
from observatory.platform.utils.airflow_utils import AirflowConns, AirflowVars
from observatory.platform.utils.file_utils import list_to_jsonl_gz
from observatory.platform.utils.workflow_utils import add_partition_date, make_dag_id
from observatory.platform.utils.workflow_utils import (
    blob_name,
    bq_load_partition,
    table_ids_from_path,
)
from observatory.platform.workflows.snapshot_telescope import (
    SnapshotRelease,
    SnapshotTelescope,
)


class GoogleAnalyticsRelease(SnapshotRelease):
    def __init__(
        self, dag_id: str, start_date: pendulum.DateTime, end_date: pendulum.DateTime, organisation: Organisation
    ):
        """Construct a GoogleAnalyticsRelease.

        :param dag_id: the id of the DAG.
        :param start_date: the start date of the download period.
        :param end_date: the end date of the download period, also used as release date for BigQuery table and
        file paths
        :param organisation: the Organisation of which data is processed.
        """
        self.dag_id_prefix = GoogleAnalyticsTelescope.DAG_ID_PREFIX
        transform_files_regex = f"{self.dag_id_prefix}.jsonl.gz"

        super().__init__(dag_id=dag_id, release_date=end_date, transform_files_regex=transform_files_regex)

        self.organisation = organisation
        self.start_date = start_date
        self.end_date = end_date

    @property
    def download_bucket(self):
        """The download bucket name.
        :return: the download bucket name.
        """
        return self.organisation.gcp_download_bucket

    @property
    def transform_bucket(self):
        """The transform bucket name.
        :return: the transform bucket name.
        """
        return self.organisation.gcp_transform_bucket

    @property
    def transform_path(self) -> str:
        """Get the path to the transformed file.

        :return: the file path.
        """
        return os.path.join(self.transform_folder, f"{self.dag_id_prefix}.jsonl.gz")

    def download_transform(self, view_id: str, pagepath_regex: str) -> bool:
        """Downloads and transforms an individual Google Analytics release.

        :param view_id: The view id.
        :param pagepath_regex: The regex expression for the pagepath of a book.
        :return: True when data available for period, False if no data is available
        """

        service = initialize_analyticsreporting()
        results = get_reports(service, self.organisation.name, view_id, pagepath_regex, self.start_date, self.end_date)
        results = add_partition_date(results, self.release_date, bigquery.TimePartitioningType.MONTH)
        if results:
            list_to_jsonl_gz(self.transform_path, results)
            return True
        else:
            if (pendulum.today("UTC") - self.end_date).in_months() >= 26:
                logging.info(
                    "No data available. Google Analytics data is only available for 26 months, see "
                    "https://support.google.com/analytics/answer/7667196?hl=en for more info"
                )
            return False


class GoogleAnalyticsTelescope(SnapshotTelescope):
    """Google Analytics Telescope."""

    DAG_ID_PREFIX = "google_analytics"
    ANU_ORG_NAME = "ANU Press"

    def __init__(
        self,
        organisation: Organisation,
        view_id: str,
        pagepath_regex: str,
        dag_id: Optional[str] = None,
        start_date: pendulum.DateTime = pendulum.datetime(2018, 1, 1),
        schedule_interval: str = "@monthly",
        dataset_id: str = "google",
        schema_folder: str = default_schema_folder(),
        catchup: bool = True,
        airflow_vars=None,
        airflow_conns=None,
        schema_prefix: str = "",
    ):
        """Construct a GoogleAnalyticsTelescope instance.
        :param organisation: the Organisation of which data is processed.
        :param view_id: the view ID, obtained from the 'extra' info from the API regarding the telescope.
        :param pagepath_regex: the pagepath regex expression, obtained from the 'extra' info from the
        API regarding the telescope.
        :param dag_id: the id of the DAG, by default this is automatically generated based on the DAG_ID_PREFIX and the
        organisation name.
        :param start_date: the start date of the DAG.
        :param schedule_interval: the schedule interval of the DAG.
        :param schema_folder: the SQL schema path.
        :param catchup: whether to catchup the DAG or not.
        :param airflow_vars: list of airflow variable keys, for each variable it is checked if it exists in airflow
        :param schema_prefix: the prefix used to find the schema path.
        """
        if airflow_vars is None:
            airflow_vars = [
                AirflowVars.DATA_PATH,
                AirflowVars.PROJECT_ID,
                AirflowVars.DATA_LOCATION,
                AirflowVars.DOWNLOAD_BUCKET,
                AirflowVars.TRANSFORM_BUCKET,
            ]

        if airflow_conns is None:
            airflow_conns = [AirflowConns.OAEBU_SERVICE_ACCOUNT]

        if dag_id is None:
            dag_id = make_dag_id(self.DAG_ID_PREFIX, organisation.name)

        # set schema prefix to 'anu_press' for ANU press, custom dimensions are added in this schema.
        if schema_prefix == "":
            schema_prefix = "anu_press_" if organisation.name == self.ANU_ORG_NAME else ""

        super().__init__(
            dag_id,
            start_date,
            schedule_interval,
            dataset_id,
            schema_folder,
            catchup=catchup,
            airflow_vars=airflow_vars,
            airflow_conns=airflow_conns,
            schema_prefix=schema_prefix,
        )

        self.organisation = organisation
        self.project_id = organisation.gcp_project_id
        self.dataset_location = "us"  # TODO: add to API
        self.view_id = view_id
        self.pagepath_regex = pagepath_regex

        self.add_setup_task_chain([self.check_dependencies])
        self.add_task_chain([self.download_transform, self.upload_transformed, self.bq_load_partition, self.cleanup])

    def make_release(self, **kwargs) -> List[GoogleAnalyticsRelease]:
        """Make release instances. The release is passed as an argument to the function (TelescopeFunction) that is
        called in 'task_callable'.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are
        passed to this argument.
        :return: A list of grid release instances
        """
        # Get start and end date (end_date = release_date)
        start_date = kwargs["execution_date"]
        end_date = kwargs["next_execution_date"] - timedelta(days=1)

        logging.info(f"Start date: {start_date}, end date:{end_date}, release date: {end_date}")
        releases = [GoogleAnalyticsRelease(self.dag_id, start_date, end_date, self.organisation)]
        return releases

    def check_dependencies(self, **kwargs) -> bool:
        """Check dependencies of DAG. Add to parent method to additionally check for a view id and pagepath regex

        :return: True if dependencies are valid.
        """
        super().check_dependencies()

        if self.view_id is None or self.pagepath_regex is None:
            expected_extra = {"view_id": "the_view_id", "pagepath_regex": r"pagepath_regex"}
            raise AirflowException(
                f"View ID and/or pagepath regex is not set in 'extra' of telescope, extra example: " f"{expected_extra}"
            )
        return True

    def download_transform(self, releases: List[GoogleAnalyticsRelease], **kwargs):
        """Task to download and transform the google analytics release for a given month.

        :param releases: a list with one google analyics release.
        :return: None.
        """
        results = releases[0].download_transform(self.view_id, self.pagepath_regex)
        if not results:
            raise AirflowSkipException("No Google Analytics data available to download.")

    def bq_load_partition(self, releases: List[SnapshotRelease], **kwargs):
        """Task to load each transformed release to BigQuery.
        The table_id is set to the file name without the extension.

        :param releases: a list of releases.
        :return: None.
        """

        # Load each transformed release
        for release in releases:
            for transform_path in release.transform_files:
                transform_blob = blob_name(transform_path)
                table_id, _ = table_ids_from_path(transform_path)
                table_description = self.table_descriptions.get(table_id, "")
                bq_load_partition(
                    self.schema_folder,
                    self.project_id,
                    release.transform_bucket,
                    transform_blob,
                    self.dataset_id,
                    self.dataset_location,
                    table_id,
                    release.release_date,
                    self.source_format,
                    bigquery.table.TimePartitioningType.MONTH,
                    prefix=self.schema_prefix,
                    schema_version=self.schema_version,
                    dataset_description=self.dataset_description,
                    table_description=table_description,
                    **self.load_bigquery_table_kwargs,
                )


def initialize_analyticsreporting() -> Resource:
    """Initializes an Analytics Reporting API V4 service object.

    :return: An authorized Analytics Reporting API V4 service object.
    """
    oaebu_account_conn = BaseHook.get_connection(AirflowConns.OAEBU_SERVICE_ACCOUNT)

    scopes = ["https://www.googleapis.com/auth/analytics.readonly"]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(oaebu_account_conn.extra_dejson, scopes=scopes)

    # Build the service object.
    service = build("analyticsreporting", "v4", credentials=creds, cache_discovery=False)

    return service


def list_all_books(
    service: Resource,
    view_id: str,
    pagepath_regex: str,
    start_date: pendulum.DateTime,
    end_date: pendulum.DateTime,
    organisation_name: str,
) -> Tuple[List[dict], list]:
    """List all available books by getting all pagepaths of a view id in a given period.

    :param service: The Google Analytics Reporting service object.
    :param view_id: The view id.
    :param pagepath_regex: The regex expression for the pagepath of a book.
    :param start_date: Start date of analytics period
    :param end_date: End date of analytics period
    :param organisation_name: The organisation name.
    :return: A list with dictionaries, one for each book entry (the dict contains the pagepath, title and average time
    on page) and a list of all pagepaths.
    """
    # Get pagepath, pagetitle and average time on page for each path
    body = {
        "reportRequests": [
            {
                "viewId": view_id,
                "pageSize": 10000,
                "dateRanges": [
                    {"startDate": start_date.strftime("%Y-%m-%d"), "endDate": end_date.strftime("%Y-%m-%d"),}
                ],
                "metrics": [{"expression": "ga:avgTimeOnPage"}],
                "dimensions": [{"name": "ga:pagepath"}, {"name": "ga:pageTitle"}],
                "dimensionFilterClauses": [
                    {
                        "operator": "AND",
                        "filters": [
                            {"dimensionName": "ga:pagepath", "operator": "REGEXP", "expressions": [pagepath_regex]}
                        ],
                    }
                ],
            }
        ]
    }

    # add all 6 custom dimensions for anu press
    if organisation_name == GoogleAnalyticsTelescope.ANU_ORG_NAME:
        for i in range(1, 7):
            body["reportRequests"][0]["dimensions"].append({"name": f"ga:dimension{str(i)}"})

    reports = service.reports().batchGet(body=body).execute()
    all_book_entries = reports["reports"][0]["data"].get("rows")
    next_page_token = reports["reports"][0].get("nextPageToken")

    while next_page_token:
        body["reportRequests"][0]["pageToken"] = next_page_token
        reports = service.reports().batchGet(body=body).execute()
        book_entries = reports["reports"][0]["data"].get("rows")
        next_page_token = reports["reports"][0].get("nextPageToken")
        all_book_entries += book_entries

    # create list with just pagepaths
    if all_book_entries:
        pagepaths = [path["dimensions"][0] for path in all_book_entries]
    else:
        pagepaths = []

    return all_book_entries, pagepaths


def create_book_result_dicts(
    book_entries: List[dict], start_date: pendulum.DateTime, end_date: pendulum.DateTime, organisation_name: str
) -> Dict[dict]:
    """Create a dictionary to store results for a single book. Pagepath, title and avg time on page are already given.
    The other metrics will be added to the dictionary later.

    :param book_entries: List with dictionaries of book entries.
    :param start_date: Start date of analytics period.
    :param end_date: End date of analytics period.
    :param organisation_name: The organisation name.
    :return: Dict to store results
    """
    book_results = {}
    for entry in book_entries:
        pagepath = entry["dimensions"][0]
        pagetitle = entry["dimensions"][1]
        average_time = float(entry["metrics"][0]["values"][0])
        book_result = {
            "url": pagepath,
            "title": pagetitle,
            "start_date": start_date.strftime("%Y-%m-%d"),
            "end_date": end_date.strftime("%Y-%m-%d"),
            "average_time": average_time,
            "unique_views": {"country": {}, "referrer": {}, "social_network": {}},
            "sessions": {"country": {}, "source": {}},
        }
        # add custom dimension data for ANU Press
        if organisation_name == GoogleAnalyticsTelescope.ANU_ORG_NAME:
            # matches dimension order in 'list_all_books'
            custom_dimensions = {
                "publication_id": entry["dimensions"][2],
                "publication_type": entry["dimensions"][3],
                "publication_imprint": entry["dimensions"][4],
                "publication_group": entry["dimensions"][5],
                "publication_whole_or_part": entry["dimensions"][6],
                "publication_format": entry["dimensions"][7],
            }
            book_result = dict(book_result, **custom_dimensions)
        book_results[pagepath] = book_result

    return book_results


def get_dimension_data(
    service: Resource,
    view_id: str,
    start_date: pendulum.DateTime,
    end_date: pendulum.DateTime,
    metrics: list,
    dimension: dict,
    pagepaths: list,
) -> list:
    """Get reports data from the Google Analytics Reporting service for a single dimension and multiple metrics.
    The results are filtered by pagepaths of interest and ordered by pagepath as well.

    :param service: The Google Analytics Reporting service.
    :param view_id: The view id.
    :param start_date: The start date of the analytics period.
    :param end_date: The end date of the analytics period.
    :param metrics: List with dictionaries of metric.
    :param dimension: The dimension.
    :param pagepaths: List with pagepaths to filter and sort on.
    :return: List with reports data for dimension and metrics.
    """
    body = {
        "reportRequests": [
            {
                "viewId": view_id,
                "pageSize": 10000,
                "dateRanges": [
                    {"startDate": start_date.strftime("%Y-%m-%d"), "endDate": end_date.strftime("%Y-%m-%d"),}
                ],
                "metrics": metrics,
                "dimensions": [{"name": "ga:pagePath"}, dimension],
                "dimensionFilterClauses": [
                    {"filters": [{"dimensionName": "ga:pagePath", "operator": "IN_LIST", "expressions": pagepaths}]}
                ],
                "orderBys": [{"fieldName": "ga:pagepath"}],
            }
        ]
    }
    reports = service.reports().batchGet(body=body).execute()
    all_dimension_data = reports["reports"][0]["data"].get("rows")
    next_page_token = reports["reports"][0].get("nextPageToken")

    while next_page_token:
        body["reportRequests"][0]["pageToken"] = next_page_token
        reports = service.reports().batchGet(body=body).execute()
        dimension_data = reports["reports"][0]["data"].get("rows")
        next_page_token = reports["reports"][0].get("nextPageToken")
        all_dimension_data += dimension_data

    return all_dimension_data


def add_to_book_result_dict(book_results: dict, dimension: dict, pagepath: str, unique_views: dict, sessions: dict):
    """Add the 'unique_views' and 'sessions' results to the book results dict if these metrics are of interest for the
    current dimension.

    :param book_results: A dictionary with all book results.
    :param dimension: Current dimension for which 'unique_views' and 'sessions' data is given.
    :param pagepath: Pagepath of the book.
    :param unique_views: Number of unique views for the pagepath&dimension
    :param sessions: Number of sessions for the pagepath&dimension
    :return: None
    """
    # map the dimension name to the field name in BigQuery. The ga:dimensionX are obtained from custom ANU press
    # dimensions
    mapping = {
        "ga:country": "country",
        "ga:fullReferrer": "referrer",
        "ga:socialNetwork": "social_network",
        "ga:source": "source",
    }
    column_name = mapping[dimension["name"]]
    if column_name in ["country", "referrer", "social_network"]:
        book_results[pagepath]["unique_views"][column_name] = unique_views
    if column_name in ["country", "source"]:
        book_results[pagepath]["sessions"][column_name] = sessions


def get_reports(
    service: Resource,
    organisation_name: str,
    view_id: str,
    pagepath_regex: str,
    start_date: pendulum.DateTime,
    end_date: pendulum.DateTime,
) -> list:
    """Get reports data from the Google Analytics Reporting API.

    :param service: The Google Analytics Reporting service.
    :param organisation_name: Name of the organisation.
    :param view_id: The view id.
    :param pagepath_regex: The regex expression for the pagepath of a book.
    :param start_date: Start date of analytics period
    :param end_date: End date of analytics period
    :return: List with google analytics data for each book
    """

    # list all books
    book_entries, pagepaths = list_all_books(service, view_id, pagepath_regex, start_date, end_date, organisation_name)
    # if no books in period return empty list and raise airflow skip exception
    if not book_entries:
        return []
    # create dict with dict for each book to store results
    book_results = create_book_result_dicts(book_entries, start_date, end_date, organisation_name)

    metric_names = ["uniquePageviews", "sessions"]
    metrics = [{"expression": f"ga:{metric}"} for metric in metric_names]

    dimension_names = ["country", "fullReferrer", "socialNetwork", "source"]
    dimensions = [{"name": f"ga:{dimension}"} for dimension in dimension_names]

    # get data per dimension
    for dimension in dimensions:
        dimension_data = get_dimension_data(service, view_id, start_date, end_date, metrics, dimension, pagepaths)

        prev_pagepath = None
        unique_views = {}
        sessions = {}
        # entry is combination of book pagepath & dimension
        for entry in dimension_data:
            pagepath = entry["dimensions"][0]
            dimension_value = entry["dimensions"][1]  # e.g. 'Australia' for 'country' dimension

            if prev_pagepath and pagepath != prev_pagepath:
                add_to_book_result_dict(book_results, dimension, prev_pagepath, unique_views, sessions)

                unique_views = {}
                sessions = {}

            # add values if they are not 0
            views_metric = int(entry["metrics"][0]["values"][0])
            sessions_metric = int(entry["metrics"][0]["values"][1])
            if views_metric > 0:
                unique_views[dimension_value] = views_metric
            if sessions_metric > 0:
                sessions[dimension_value] = sessions_metric

            prev_pagepath = pagepath
        else:
            add_to_book_result_dict(book_results, dimension, prev_pagepath, unique_views, sessions)

    # transform nested dict to list of dicts
    for book, result in book_results.items():
        for field, value in result.items():
            # field is 'unique_views' or 'sessions'
            if isinstance(value, dict):
                # nested_field is 'country', 'referrer' or 'social_network'
                for nested_field, nested_value in value.items():
                    values = []
                    # k is e.g. 'Australia', v is e.g. 1
                    for k, v in nested_value.items():
                        values.append({"name": k, "value": v})
                    book_results[book][field][nested_field] = values

    # convert dict to list of results
    book_results = [book_results[k] for k in book_results]

    return book_results
