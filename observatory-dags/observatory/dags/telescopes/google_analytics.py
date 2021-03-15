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

import datetime
import json
import logging
import os
import re
from datetime import datetime
from shutil import copyfile
from typing import List
from zipfile import BadZipFile, ZipFile

from croniter import croniter
import pendulum
from airflow.exceptions import AirflowException
from airflow.models.taskinstance import TaskInstance
from airflow.hooks.base_hook import BaseHook
from observatory.platform.telescopes.snapshot_telescope import SnapshotRelease, SnapshotTelescope
from observatory.platform.utils.airflow_utils import AirflowVariable as Variable, AirflowVars, AirflowConns
from observatory.platform.utils.data_utils import get_file
from observatory.platform.utils.template_utils import upload_files_from_list
from observatory.platform.utils.telescope_utils import list_to_jsonl_gz
from observatory.platform.utils.url_utils import retry_session
from pendulum import Pendulum
from googleapiclient.discovery import build, Resource
from oauth2client.service_account import ServiceAccountCredentials
from google_auth_oauthlib.flow import InstalledAppFlow

VIEW_ID = ''


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


def get_reports(service: Resource, start_date: pendulum.Pendulum, end_date: pendulum.Pendulum):
    """ Queries the Analytics Reporting API V4.

  :param service: An authorized Analytics Reporting API V4 service object.
  :return: The Analytics Reporting API V4 response.
  """
    results = []
    book_title = None
    url = None
    average_time = None
    results = {'url': url,
               'title': book_title,
               'start_date': start_date.strftime("%Y-%m-%d"),
               'end_date': end_date.strftime("%Y-%m-%d"),
               'average_time': average_time,
               'views': {'country': {},
                         'referrer': {},
                         'social_network': {}},
               'sessions': {'country': {},
                            'source': {}}
    }
    # Get all page paths for books
    body = {
        'reportRequests': [{
            'viewId': VIEW_ID,
            'dateRanges': [{
                'startDate': start_date.strftime("%Y-%m-%d"),
                'endDate': end_date.strftime("%Y-%m-%d"),
            }],
            "metrics": [{'expression': 'ga:pageviews'}],
            "dimensions": [{'name': 'ga:pagepath'}]
        }]
    }
    reports = service.reports().batchGet(body=body).execute()

    metrics = ['sessions', 'uniquePageviews', 'avgTimeOnPage']
    metrics = [{'expression': f'ga:{metric}'} for metric in metrics]

    # loop through dimensions, as they are combined
    dimensions = ['country', 'fullReferrer', 'socialNetwork', 'source']
    dimensions = [{'name': f'ga:{dimension}'} for dimension in dimensions]
    for dimension in dimensions:
        body = {
            'reportRequests': [{
                'viewId': VIEW_ID,
                'dateRanges': [{
                    'startDate': start_date.strftime("%Y-%m-%d"),
                    'endDate': end_date.strftime("%Y-%m-%d"),
                }],
                "metrics": metrics,
                "dimensions": dimension,
                "dimensionFilterClauses": [{
                    "filters": [
                        {
                            'dimensionName': "ga:pagePath",
                            "operator": "EXACT",
                            "expressions": [book_title]
                        }
                    ]
                }]
            }]
        }
    # should return result, most results not returned because data is not stored that long
    # body = {
    #     "reportRequests": [{
    #         "viewId": VIEW_ID,
    #         "dateRanges": [{
    #             "endDate": "2018-01-01",
    #             "startDate": "2015-01-01"
    #         }],
    #         "metrics": [{
    #             "expression": "ga:sessions"
    #         }, ],
    #         "dimensions": [{
    #             "name": "ga:country"
    #         },
    #
    #         ],
    #         "includeEmptyRows": True
    #     }]
    # }
        reports = service.reports().batchGet(body=body).execute()
        result = reports['reports'][0]['data'].get('rows')
        if result:
            results.append(result)
    return results


# def get_report(analytics):
#     """Queries the Analytics Reporting API V4.
#
#   Args:
#     analytics: An authorized Analytics Reporting API V4 service object.
#   Returns:
#     The Analytics Reporting API V4 response.
#   """
#     metrics = ['sessions', 'uniquePageviews']
#     metrics = [{
#                    'expression': f'ga:{metric}'
#                } for metric in metrics]
#
#     # loop through dimensions, as they are combined
#     dimensions = ['country', 'fullReferrer', 'socialNetwork', 'source']
#     # dimensions = [{'name': f'ga:{dimension}'} for dimension in dimensions]
#     results = {}
#     for dimension in dimensions:
#         body = {
#             'reportRequests': [{
#                 'viewId': VIEW_ID,
#                 'dateRanges': [{
#                     'startDate': '2016-01-01',
#                     'endDate': '2017-01-01'
#                 }],
#                 "metrics": metrics,
#                 "dimensions": [{'name': f'ga:{dimension}'}],
#             }]
#         }
#         result = analytics.reports().batchGet(body=body).execute()
#         results[dimension] = result
#     return results


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

    def download(self) -> bool:
        """ Downloads an individual Google Analytics release.
        :return:
        """

        service = initialize_analyticsreporting()
        results = get_reports(service, self.start_date, self.end_date)
        # print_response(response)

        return True

    def transform(self):
        """ Transform a Google Analytics release into json lines format and gzip the result.
        :return: None.
        """

        pass

#
# def list_available_releases(service: Resource) -> List[dict]:
#     """ List all GRID records available on Figshare between two dates.
#     :param timeout: the number of seconds to wait until timing out.
#     :return: the list of GRID releases with required variables stored as a dictionary.
#     """
#
#     available_releases = {}
#
#     # Call the Google Analaytics API
#     results = service.reports().batchGet(body=body).execute()
#
#     response = retry_session().get(grid_dataset_url, timeout=timeout, headers={
#         'Accept-encoding': 'gzip'
#     })
#     response_json = json.loads(response.text)
#
#     records: List[dict] = []
#     release_articles = {}
#     for item in response_json:
#         published_date: Pendulum = pendulum.parse(item['published_date'])
#
#         if start_date <= published_date < end_date:
#             article_id = item['id']
#             title = item['title']
#
#             # Parse date:
#             # The publish date is not used as the release date because the dataset is often
#             # published after the release date
#             date_matches = re.search("([0-9]{4}\-[0-9]{2}\-[0-9]{2})", title)
#             if date_matches is None:
#                 raise ValueError(f'No release date found in GRID title: {title}')
#             release_date = pendulum.parse(date_matches[0])
#
#             try:
#                 release_articles[release_date].append(article_id)
#             except KeyError:
#                 release_articles[release_date] = [article_id]
#
#     for release_date in release_articles:
#         article_ids = release_articles[release_date]
#         records.append({
#             'article_ids': article_ids,
#             'release_date': release_date
#         })
#     return records


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
        self.add_task_chain([self.download,
                             self.upload_downloaded,
                             self.transform,
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

    def download(self, releases: List[GoogleAnalyticsRelease], **kwargs):
        """ Task to download the GRID releases for a given month.
        :param releases: a list of GRID releases.
        :return: None.
        """
        # Download each release
        for release in releases:
            release.download()

    def upload_downloaded(self, releases: List[GoogleAnalyticsRelease], **kwargs):
        """ Task to upload the downloaded GRID releases for a given month.
        :param releases: a list of GRID releases.
        :return: None.
        """
        # Upload each downloaded release
        for release in releases:
            upload_files_from_list(release.download_files, release.download_bucket)

    def transform(self, releases: List[GoogleAnalyticsRelease], **kwargs):
        """ Task to transform the GRID releases for a given month.
        :param releases: a list of GRID releases.
        :return: None.
        """
        # Transform each release
        for release in releases:
            release.transform()
