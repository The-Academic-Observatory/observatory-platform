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

import copy
import datetime
import logging
import os
from datetime import datetime
from typing import Dict, List, Tuple

import pendulum
from airflow.hooks.base_hook import BaseHook
from croniter import croniter
from googleapiclient.discovery import Resource, build
from oauth2client.service_account import ServiceAccountCredentials
from observatory.platform.telescopes.snapshot_telescope import SnapshotRelease, SnapshotTelescope
from observatory.platform.utils.airflow_utils import AirflowConns, AirflowVars
from observatory.platform.utils.telescope_utils import list_to_jsonl_gz

VIEW_ID = ''


class GoogleAnalyticsRelease(SnapshotRelease):
    def __init__(self, dag_id: str, start_date: pendulum.Pendulum, release_date: pendulum.Pendulum):
        """ Construct a GoogleAnalyticsRelease.
        :param dag_id: the id of the DAG.
        :param start_date: the start date of the download period.
        :param release_date: the end date of the download period, also used as release date for BigQuery table and
        file paths
        """
        download_files_regex = f"{dag_id}.jsonl"
        transform_files_regex = f"{dag_id}.jsonl.gz"

        super().__init__(dag_id=dag_id, release_date=release_date, download_files_regex=download_files_regex,
                         transform_files_regex=transform_files_regex)

        self.start_date = start_date
        self.end_date = release_date

    @property
    def transform_path(self) -> str:
        return os.path.join(self.transform_folder, f'{self.dag_id}.jsonl.gz')

    def download_transform(self):
        """ Downloads an individual Google Analytics release.
        :return:
        """

        service = initialize_analyticsreporting()
        results = get_reports(service, self.start_date, self.end_date)

        list_to_jsonl_gz(self.transform_path, results)


class GoogleAnalyticsTelescope(SnapshotTelescope):
    """ Google Analytics Telescope."""

    def __init__(self, dag_id: str = 'google_analytics', start_date: datetime = datetime(2015, 9, 1),
                 schedule_interval: str = '@weekly', dataset_id: str = 'google', catchup: bool = True,
                 airflow_vars=None):
        """ Construct a GoogleAnalyticsTelescope instance.
        :param dag_id: the id of the DAG.
        :param start_date: the start date of the DAG.
        :param schedule_interval: the schedule interval of the DAG.
        :param catchup: whether to catchup the DAG or not.
        :param airflow_vars: list of airflow variable keys, for each variable it is checked if it exists in airflow
        """
        if airflow_vars is None:
            airflow_vars = [AirflowVars.DATA_PATH, AirflowVars.PROJECT_ID, AirflowVars.DATA_LOCATION,
                            AirflowVars.DOWNLOAD_BUCKET, AirflowVars.TRANSFORM_BUCKET]
        super().__init__(dag_id, start_date, schedule_interval, dataset_id, catchup=catchup, airflow_vars=airflow_vars)

        self.add_setup_task_chain([self.check_dependencies])
        self.add_task_chain([self.download_transform,
                             self.upload_transformed,
                             self.bq_load,
                             self.cleanup])

    def make_release(self, **kwargs) -> List[GoogleAnalyticsRelease]:
        """ Make release instances. The release is passed as an argument to the function (TelescopeFunction) that is
        called in 'task_callable'.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are
        passed to this argument.
        :return: A list of grid release instances
        """
        # Get start and end date (release_date)
        cron_schedule = kwargs['dag'].normalized_schedule_interval
        start_date = pendulum.instance(kwargs['execution_date'])
        cron_iter = croniter(cron_schedule, start_date)
        end_date = pendulum.instance(cron_iter.get_next(datetime))

        logging.info(f'Start date: {start_date}, end date:{end_date}')
        releases = [GoogleAnalyticsRelease(self.dag_id, start_date, end_date)]
        return releases

    def download_transform(self, releases: List[GoogleAnalyticsRelease], **kwargs):
        """ Task to download the GRID releases for a given month.
        :param releases: a list of GRID releases.
        :return: None.
        """
        # Download each release
        for release in releases:
            release.download_transform()


def initialize_analyticsreporting() -> Resource:
    """ Initializes an Analytics Reporting API V4 service object.

  :return: An authorized Analytics Reporting API V4 service object.
  """
    oaebu_account_conn = BaseHook.get_connection(AirflowConns.OAEBU_SERVICE_ACCOUNT)

    scopes = ['https://www.googleapis.com/auth/analytics.readonly']
    creds = ServiceAccountCredentials.from_json_keyfile_dict(oaebu_account_conn.extra_dejson, scopes=scopes)

    # Build the service object.
    service = build('analyticsreporting', 'v4', credentials=creds)

    return service


def list_all_books(service: Resource, start_date: pendulum.Pendulum, end_date: pendulum.Pendulum) -> Tuple[list, list]:
    # Get all page paths for books
    body = {
        'reportRequests': [{
            'viewId': VIEW_ID,
            'dateRanges': [{
                'startDate': start_date.strftime("%Y-%m-%d"),
                'endDate': end_date.strftime("%Y-%m-%d"),
            }],
            "metrics": [{'expression': 'ga:avgTimeOnPage'}],
            "dimensions": [{'name': 'ga:pagepath'}, {'name': 'ga:pageTitle'}],
            "dimensionFilterClauses": [{
                "filters": [{
                    "dimensionName": "ga:pagepath",
                    "operator": "BEGINS_WITH",
                    "expressions": ["/collections/open-access/products/"]
                }]
            }]
        }]
    }
    reports = service.reports().batchGet(body=body).execute()
    book_entries = reports['reports'][0]['data'].get('rows')
    pagepaths = [path['dimensions'][0] for path in book_entries]

    return book_entries, pagepaths


def create_book_result_dicts(book_entries: list, start_date: pendulum.Pendulum, end_date: pendulum.Pendulum) -> Dict[
    dict]:
    book_results = {}
    for entry in book_entries:
        pagepath = entry['dimensions'][0]
        pagetitle = entry['dimensions'][1]
        average_time = float(entry['metrics'][0]['values'][0])
        book_result = {'url': pagepath,
                       'title': pagetitle,
                       'start_date': start_date.strftime("%Y-%m-%d"),
                       'end_date': end_date.strftime("%Y-%m-%d"),
                       'average_time': average_time,
                       'unique_views': {'country': {},
                                        'referrer': {},
                                        'social_network': {}},
                       'sessions': {'country': {},
                                    'source': {}}
                       }
        book_results[pagepath] = book_result

    return book_results


def get_dimension_data(service: Resource, start_date: pendulum.Pendulum, end_date: pendulum.Pendulum, metrics: list,
                       dimension: dict, pagepaths: list) -> list:
    body = {
        'reportRequests': [{
            'viewId': VIEW_ID,
            'dateRanges': [{
                'startDate': start_date.strftime("%Y-%m-%d"),
                'endDate': end_date.strftime("%Y-%m-%d"),
            }],
            "metrics": metrics,
            "dimensions": [{'name': 'ga:pagePath'}, dimension],
            "dimensionFilterClauses": [{
                "filters": [
                    {
                        'dimensionName': "ga:pagePath",
                        "operator": "IN_LIST",
                        "expressions": pagepaths
                    }
                ]
            }],
            "orderBys": [{"fieldName": "ga:pagepath"}]
        }]
    }
    reports = service.reports().batchGet(body=body).execute()
    dimension_data = reports['reports'][0]['data'].get('rows')
    return dimension_data


def add_to_book_result_dict(book_results: dict, dimension: dict, pagepath: str, unique_views: dict, sessions: dict):
    mapping = {'ga:country': 'country',
               'ga:fullReferrer': 'referrer',
               'ga:socialNetwork': 'social_network',
               'ga:source': 'source'}
    column_name = mapping[dimension['name']]
    if column_name in ['country', 'referrer', 'social_network']:
        book_results[pagepath]['unique_views'][column_name] = unique_views
    if column_name in ['country', 'source']:
        book_results[pagepath]['sessions'][column_name] = sessions


def merge_pagepaths_per_book(book_results: dict, pagepaths: list):
    unique_pagepaths = set([path.split('?')[0] for path in pagepaths])
    unique_book_results = {}
    paths_per_book = {}
    for path in book_results:
        unique_path = path.split('?')[0]
        if unique_path in unique_pagepaths:
            try:
                # merge average time, sum values and divide to get average when known how many pagepaths for 1 book
                paths_per_book[unique_path] += 1
                unique_book_results[unique_path]['average_time'] += book_results[path]['average_time']

                # merge unique views, sum values
                for dimension in unique_book_results[unique_path]['unique_views']:
                    current_views = unique_book_results[unique_path]['unique_views'][dimension]
                    added_views = book_results[path]['unique_views'][dimension]
                    merged_views = {k: current_views.get(k, 0) + added_views.get(k, 0) for k in
                                    set(current_views) | set(added_views)}

                    unique_book_results[unique_path]['unique_views'][dimension] = merged_views

                # merge sessions, sum values
                for dimension in unique_book_results[unique_path]['sessions']:
                    current_sessions = unique_book_results[unique_path]['sessions'][dimension]
                    added_sessions = book_results[path]['sessions'][dimension]
                    merged_sessions = {k: current_sessions.get(k, 0) + added_sessions.get(k, 0) for k in
                                       set(current_sessions) | set(added_sessions)}

                    unique_book_results[unique_path]['sessions'][dimension] = merged_sessions

            except KeyError:
                paths_per_book[unique_path] = 1
                unique_book_results[unique_path] = copy.deepcopy(book_results[path])

    # divide summed up average time by number of paths per book
    for unique_path in unique_book_results:
        unique_book_results[unique_path] = {k: v / paths_per_book[unique_path] if k == 'average_time' else v for k, v in
                                            unique_book_results[unique_path].items()}
    return unique_book_results


def get_reports(service: Resource, start_date: pendulum.Pendulum, end_date: pendulum.Pendulum) -> list:
    """ Queries the Analytics Reporting API V4.

  :param service: An authorized Analytics Reporting API V4 service object.
  :return: The Analytics Reporting API V4 response.
  """

    book_entries, pagepaths = list_all_books(service, start_date, end_date)

    book_results = create_book_result_dicts(book_entries, start_date, end_date)

    metric_names = ['uniquePageviews', 'sessions']
    metrics = [{'expression': f'ga:{metric}'} for metric in metric_names]

    # loop through dimensions
    dimension_names = ['country', 'fullReferrer', 'socialNetwork', 'source']
    dimensions = [{'name': f'ga:{dimension}'} for dimension in dimension_names]

    for dimension in dimensions:
        dimension_data = get_dimension_data(service, start_date, end_date, metrics, dimension, pagepaths)

        prev_pagepath = None
        unique_views = {}
        sessions = {}
        for entry in dimension_data:
            pagepath = entry['dimensions'][0]
            dimension_of_interest = entry['dimensions'][1]  # e.g. 'Australia' for 'country' dimension

            if prev_pagepath and pagepath != prev_pagepath:
                add_to_book_result_dict(book_results, dimension, prev_pagepath, unique_views, sessions)

                unique_views = {}
                sessions = {}

            # add values if they are not 0
            views_metric = int(entry['metrics'][0]['values'][0])
            sessions_metric = int(entry['metrics'][0]['values'][1])
            if views_metric > 0:
                unique_views[dimension_of_interest] = views_metric
            if sessions_metric > 0:
                sessions[dimension_of_interest] = sessions_metric

            prev_pagepath = pagepath
        else:
            add_to_book_result_dict(book_results, dimension, prev_pagepath, unique_views, sessions)

    # merge results of single book with different pagepaths (e.g. fbclid parameter)
    book_results = merge_pagepaths_per_book(book_results, pagepaths)

    # transform nested dict to list of dicts
    for book, result in book_results.items():
        # field is 'unique_views' or 'sessions'
        for field, value in result.items():
            if isinstance(value, dict):
                # nested_field is 'country', 'referrer' or 'social_network'
                for nested_field, nested_value in value.items():
                    values = []
                    # k is e.g. 'Australia', v is e.g. 1
                    for k, v in nested_value.items():
                        values.append({'name': k, 'value': v})
                    book_results[book][field][nested_field] = values

    # convert dict to list of results
    book_results = list(book_results.values())
    return book_results
