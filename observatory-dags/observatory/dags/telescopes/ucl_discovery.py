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

import csv
import logging
import os
from datetime import datetime
from typing import List
from typing import Tuple

import pendulum
import six
from airflow.utils.dates import cron_presets
from croniter import croniter
from observatory.platform.telescopes.snapshot_telescope import SnapshotRelease, SnapshotTelescope
from observatory.platform.utils.airflow_utils import AirflowVars
from observatory.platform.utils.telescope_utils import list_to_jsonl_gz
from observatory.platform.utils.template_utils import upload_files_from_list
from observatory.platform.utils.url_utils import retry_session


def get_downloads_per_country(countries_url: str) -> Tuple[list, int]:
    print(countries_url)
    response = retry_session(num_retries=5).get(countries_url)
    response_content = response.content.decode('utf-8')
    if response_content == '\n':
        return [], 0
    response_csv = csv.DictReader(response_content.splitlines())
    results = []
    total_downloads = 0
    for row in response_csv:
        download_count = int(row['count'].strip('="'))
        country_code = row['value']
        country_name = row['description'].split('</span>')[0].split('>')[-1]
        results.append({
                           'country_code': country_code,
                           'country_name': country_name,
                           'download_count': download_count
                       })
        total_downloads += download_count

    return results, total_downloads


def create_result_dict(begin_date, end_date, total_downloads, downloads_per_country, multi_row_columns,
                       single_row_columns):
    result = dict(begin_date=begin_date, end_date=end_date, total_downloads=total_downloads,
                  downloads_per_country=downloads_per_country, **multi_row_columns, **single_row_columns)
    for k, v in result.items():
        result[k] = v if v != '' else None
    return result


def normalize_schedule_interval(schedule_interval: str):
    """
    Returns Normalized Schedule Interval. This is used internally by the Scheduler to
    schedule DAGs.
    1. Converts Cron Preset to a Cron Expression (e.g ``@monthly`` to ``0 0 1 * *``)
    2. If Schedule Interval is "@once" return "None"
    3. If not (1) or (2) returns schedule_interval
    """
    if isinstance(schedule_interval, six.string_types) and schedule_interval in cron_presets:
        _schedule_interval = cron_presets.get(schedule_interval)
    elif schedule_interval == '@once':
        _schedule_interval = None
    else:
        _schedule_interval = schedule_interval
    return _schedule_interval


class UclDiscoveryRelease(SnapshotRelease):
    def __init__(self, dag_id: str, start_date: pendulum.Pendulum, release_date: pendulum.Pendulum, ):
        super().__init__(dag_id, release_date)

        self.start_date = start_date
        self.end_date = release_date

        self.list_ids_url = UclDiscoveryTelescope.LIST_IDS_URL.format(end_date=self.end_date.strftime("%Y"))
        self.countries_url = UclDiscoveryTelescope.COUNTRIES_URL.format(start_date=self.start_date.strftime("%Y%m%d"),
                                                                        end_date=self.end_date.strftime("%Y%m%d"))

    @property
    def download_path(self) -> str:
        return os.path.join(self.download_folder, f'{self.dag_id}.txt')

    @property
    def transform_path(self) -> str:
        return os.path.join(self.transform_folder, f'{self.dag_id}.jsonl')

    def download(self):
        """

        :return:
        """
        print(self.list_ids_url)
        response = retry_session(num_retries=5).get(self.list_ids_url)
        if response.status_code == 200:
            response_content = response.content.decode('utf-8')
            csv_reader = csv.DictReader(response_content.splitlines())
            try:
                next(csv_reader)
            except StopIteration:
                return False
            with open(self.download_path, 'w') as f:
                f.write(response_content)
            return True
        else:
            return False

    def transform(self):
        """

        :return:
        """
        begin_date = self.start_date.strftime("%Y-%m-%d")
        end_date = self.end_date.strftime("%Y-%m-%d")

        with open(self.download_path, 'r') as f:
            csv_reader = csv.DictReader(f)

            previous_id = None
            results = []
            multi_row_columns = {
                'creators_name_family': [],
                'creators_name_given': [],
                'subjects': [],
                'divisions': [],
                'lyricists_name_family': [],
                'lyricists_name_given': [],
                'editors_name_family': [],
                'editors_name_given': []
            }
            for row in csv_reader:
                eprintid = row['eprintid']
                # row with a new eprint id
                if previous_id != eprintid:
                    # add results of previous eprint id
                    if previous_id:
                        result = create_result_dict(begin_date, end_date, total_downloads, downloads_per_country,
                                                    multi_row_columns, single_row_columns)
                        results.append(result)
                        for column in multi_row_columns:
                            multi_row_columns[column] = []

                    # store results of current eprint id
                    single_row_columns = {
                        'eprintid': row['eprintid'],
                        'book_title': row['title'],
                        'ispublished': row['ispublished'],
                        'keywords': row['keywords'].split(', '),
                        'abstract': row['abstract'],
                        'date': row['date'],
                        'publisher': row['publisher'],
                        'official_url': row['official_url'],
                        'oa_status': row['oa_status'],
                        'language': row['language'],
                        'doi': row['doi'],
                        'isbn': row['isbn_13'],
                        'language_elements': row['language_elements'],
                        'series': row['series'],
                        'pagerange': row['pagerange'],
                        'pages': row['pages']
                    }

                    downloads_per_country, total_downloads = get_downloads_per_country(self.countries_url + eprintid)

                # append results of current eprint id
                for column in multi_row_columns:
                    # make sure that for 'name' columns a value is added for both, even if only 1 of the columns has a
                    # value
                    start_column_name = column.split('_')[0]
                    if start_column_name in ['creators', 'lyricists', 'editors']:
                        name_family = row[start_column_name + '_name.family']
                        name_given = row[start_column_name + '_name.given']
                        name = name_family + name_given
                        # if not name_family and name_given:
                        #     print('stop')
                        # if not name_given and name_family:
                        #     print('stop')
                        if name:
                            column_name = '.'.join(column.rsplit('_', 1))
                            multi_row_columns[column].append(row[column_name])
                    else:
                        if row[column]:
                            multi_row_columns[column].append(row[column])

                previous_id = eprintid

            # append results of last rows/eprint id
            result = create_result_dict(begin_date, end_date, total_downloads, downloads_per_country, multi_row_columns,
                                        single_row_columns)
            results.append(result)

        # Write list into gzipped JSON Lines file
        list_to_jsonl_gz(self.transform_path, results)


class UclDiscoveryTelescope(SnapshotTelescope):
    LIST_IDS_URL = 'https://discovery.ucl.ac.uk/cgi/search/archive/advanced/export_discovery_CSV.csv?' \
                   'screen=Search&dataset=archive&_action_export=1&output=CSV' \
                   '&exp=0|1|-date/creators_name/title|archive|-|date:date:ALL:EQ:-{end_date}|primo:primo:ANY:EQ:open' \
                   '|type:type:ANY:EQ:book|-|eprint_status:eprint_status:ANY:EQ:archive' \
                   '|metadata_visibility:metadata_visibility:ANY:EQ:show'
    COUNTRIES_URL = 'https://discovery.ucl.ac.uk/cgi/stats/get?from={start_date}&to={end_date}&irs2report=eprint' \
                    '&datatype=countries&top=countries&view=Table&limit=all&set_name=eprint&export=CSV&set_value='

    def __init__(self, dag_id: str = 'ucl_discovery', start_date: datetime = datetime(2008, 1, 1),
                 schedule_interval: str = '@monthly', dataset_id: str = 'ucl_discovery', airflow_vars: list = None):
        if airflow_vars is None:
            airflow_vars = [AirflowVars.DATA_PATH, AirflowVars.PROJECT_ID, AirflowVars.DATA_LOCATION,
                            AirflowVars.DOWNLOAD_BUCKET, AirflowVars.TRANSFORM_BUCKET]
        super().__init__(dag_id, start_date, schedule_interval, dataset_id, airflow_vars=airflow_vars)

        self.add_setup_task(self.check_dependencies)
        self.add_task_chain(
            [self.download, self.upload_downloaded, self.transform, self.upload_transformed, self.bq_load,
             self.cleanup])

    def make_release(self, **kwargs) -> List[UclDiscoveryRelease]:
        """

        :param kwargs:
        :return:
        """

        # Get start and end date (release_date)
        cron_schedule = normalize_schedule_interval(kwargs['dag'].schedule_interval)
        start_date = pendulum.instance(kwargs['dag_run'].execution_date)
        cron_iter = croniter(cron_schedule, start_date)
        end_date = pendulum.instance(cron_iter.get_next(datetime))

        logging.info(f'Start date: {start_date}, end date:{end_date}')
        releases = [UclDiscoveryRelease(self.dag_id, start_date, end_date)]
        return releases

    def download(self, releases: List[UclDiscoveryRelease], **kwargs):
        """ Task to download the GRID releases for a given month.
        :param releases: a list of GRID releases.
        :return: None.
        """
        # Download each release
        for release in releases:
            release.download()

    def upload_downloaded(self, releases: List[UclDiscoveryRelease], **kwargs):
        """ Task to upload the downloaded GRID releases for a given month.
        :param releases: a list of GRID releases.
        :return: None.
        """
        # Upload each downloaded release
        for release in releases:
            upload_files_from_list(release.download_files, release.download_bucket)

    def transform(self, releases: List[UclDiscoveryRelease], **kwargs):
        """ Task to transform the GRID releases for a given month.
        :param releases: a list of GRID releases.
        :return: None.
        """
        # Transform each release
        for release in releases:
            release.transform()
