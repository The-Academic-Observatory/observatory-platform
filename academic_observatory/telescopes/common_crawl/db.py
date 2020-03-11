# Copyright 2019 Curtin University
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

import datetime
from typing import List

from google.cloud import bigquery

from academic_observatory.telescopes.common_crawl.schema import WarcIndex, PageInfo, InstitutionIndex, WarcIndexInfo

DATE_FORMAT = '%Y-%m-%d'


def get_grid_ids(table_name: str, fetch_month: datetime.date) -> List[str]:
    """ Get the distinct GRID IDs that are in a BigQuery table.

    :param table_name: the name of the table in the format project-id.dataset-name.table-name
    :param fetch_month: the month to fetch the data from.
    :return: a list of GRID ids.
    """

    fetch_month_str = fetch_month.strftime(DATE_FORMAT)

    client = bigquery.Client()
    query = (
        f"""
            SELECT DISTINCT grid_id
            FROM `{table_name}`
            WHERE fetch_month=DATE('{fetch_month_str}')
        """
    )
    query = client.query(
        query,
        location="US",
    )

    results: List[str] = []
    for row in query:
        results.append(row["grid_id"])

    return results


def get_page_infos(table_name: str, grid_id: str, fetch_month: datetime.date) -> List[PageInfo]:
    """ Get a list of PageInfo objects for a particular GRID id and a particular month.

    :param table_name: the name of the table in the format project-id.dataset-name.table-name
    :param grid_id: the GRID id.
    :param fetch_month: the month.
    :return: a list of PageInfo objects.
    """

    fetch_month_str = fetch_month.strftime(DATE_FORMAT)

    client = bigquery.Client()
    query = (
        f"""
            SELECT *
            FROM `{table_name}`
            WHERE fetch_month=DATE('{fetch_month_str}') AND grid_id='{grid_id}'
        """
    )
    query = client.query(
        query,
        location="US",
    )

    results: List[PageInfo] = []
    for row in query:
        institution_index = InstitutionIndex.from_dict(row)
        warc_index = WarcIndex.from_dict(row)
        warc_index_info = WarcIndexInfo.from_dict(row)
        page_info = PageInfo(institution_index, warc_index, warc_index_info)
        results.append(page_info)

    return results
