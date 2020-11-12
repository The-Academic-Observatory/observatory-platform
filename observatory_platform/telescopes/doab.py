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

"""
The Directory of Open Access Books (DOAB) is a directory of open-access peer reviewed scholarly books.
Its aim is to increase discoverability of books. Academic publishers provide metadata of their Open Access books to DOAB.
It is available both through OAI-PMH harvesting and as a csv file.
"""

import csv
import json
import jsonlines
import logging
import os
import pathlib
import pendulum
import xmltodict
from datetime import datetime
from pendulum.parsing.exceptions import ParserError
from types import SimpleNamespace
from typing import Tuple, Union
from airflow.exceptions import AirflowException
from requests.exceptions import RetryError

from observatory_platform.utils.config_utils import (AirflowVar,
                                                     SubFolder)
from observatory_platform.utils.telescope_stream import (StreamTelescope, StreamRelease)
from observatory_platform.utils.url_utils import retry_session, get_ao_user_agent


def create_release(start_date: pendulum.Pendulum, end_date: pendulum.Pendulum, telescope: SimpleNamespace,
                   first_release: bool = False) -> 'DoabRelease':
    """ Create a release based on the start and end date.

    :param start_date: Start date of this run
    :param end_date: End date of this run
    :param telescope: Contains telescope properties
    :param first_release: Whether this is the first release to be obtained
    :return: Release instance
    """
    release = DoabRelease(start_date, end_date, telescope, first_release)
    return release


def download_oai_pmh(release: 'DoabRelease') -> bool:
    """ Download DOAB metadata using an OAI-PMH harvester.

    :param release: Release instance
    :return: True if download is successful else False
    """
    logging.info('Downloading OAI-PMH')
    url = release.telescope.oai_pmh_url.format(start_date=release.start_date.strftime("%Y-%m-%d"), 
                                               end_date=release.end_date.strftime("%Y-%m-%d"))
    # check if cursor files exist from a previous failed request
    if os.path.isfile(release.token_path):
        # retrieve token
        with open(release.token_path, 'r') as f:
            next_token = json.load(f)
        # delete file
        pathlib.Path(release.token_path).unlink()
        # extract entries
        success, next_token, total_entries = extract_entries(url, release.oai_pmh_path, next_token)
    # if entries download path exists but no token file, the previous request has finished & successful
    elif os.path.isfile(release.oai_pmh_path):
        success = True
        next_token = None
        total_entries = 'See previous successful attempt'
    # first time request
    else:
        # extract events
        success, next_token, total_entries = extract_entries(url, release.oai_pmh_path)

    if not success:
        with open(release.token_path, 'w') as f:
            json.dump(next_token, f)
        raise AirflowException(f'Download unsuccessful, status: {success}')

    logging.info(f'OAI-PMH successful, total entries={total_entries}')
    return True if success and total_entries != '0' else False


def download_csv(release: 'DoabRelease') -> bool:
    """ Download DOAB metadata from a csv file.

    :param release: Release instance
    :return: True if download is successful
    """
    logging.info('Downloading csv')
    headers = {
        'User-Agent': f'{get_ao_user_agent()}'
    }
    response = retry_session().get(release.telescope.csv_url, headers=headers)
    if response.status_code == 200:
        with open(release.csv_path, 'w') as f:
            f.write(response.content.decode('utf-8'))
        logging.info('Download csv successful')
        return True
    else:
        raise AirflowException(f'Download csv unsuccessful, {response.text}')


def download(release: 'DoabRelease') -> bool:
    """ Download both using the OAI-PMH harvester and from a csv file.

    :param release: Release instance
    :return: True if both downloads are successful
    """
    success_oai_pmh = download_oai_pmh(release)
    success_csv = download_csv(release)

    logging.info(f"OAI-PMH status: {success_oai_pmh}, CSV status: {success_csv}")
    return True if success_oai_pmh and success_csv else False


def transform(release: 'DoabRelease'):
    """ For each row, combine the OAI-PMH and csv results into one json object.
    The column names are structure are slightly changed to make it readable by BigQuery.

    :param release: Release instance
    :return: None
    """
    # create dict of data in summary xml file
    oai_pmh_entries = []
    with open(release.oai_pmh_path, 'r') as f:
        for line in f:
            entries_page = json.loads(line)
            oai_pmh_entries.append(entries_page)

    # create global dict with csv entries of specific columns
    global csv_entries
    with open(release.csv_path, 'r') as f:
        csv_entries = {row['ISBN'].strip().split(' ')[0]: {'ISSN': row['ISSN'], 'Volume': row['Volume'], 'Pages': row[
            'Pages'], 'Series title': row['Series title'], 'Added on date': row['Added on date'], 'Subjects': row[
            'Subjects']} for row in csv.DictReader(f, skipinitialspace=True)}

    # change keys & delete some values
    entries = change_keys(oai_pmh_entries, convert)

    # write out the transformed data
    with open(release.transform_path, 'w') as json_out:
        with jsonlines.Writer(json_out) as writer:
            for entry_page in entries:
                for doab_entry in entry_page:
                    writer.write_all([doab_entry])


class DoabTelescope(StreamTelescope):
    telescope = SimpleNamespace(dag_id='doab', dataset_id='doab', main_table_id='doab',
                                schedule_interval='@weekly', start_date=datetime(2018, 5, 14),
                                partition_table_id='doab_partitions', description='the description',
                                queue='default', max_retries=3, bq_merge_days=0,
                                merge_partition_field='header.identifier',
                                updated_date_field='header.datestamp',
                                download_ext='json', extract_ext='json', transform_ext='jsonl',
                                airflow_vars=[AirflowVar.data_path.get(), AirflowVar.project_id.get(),
                                              AirflowVar.data_location.get(),
                                              AirflowVar.download_bucket_name.get(),
                                              AirflowVar.transform_bucket_name.get()],
                                airflow_conns=[], schema_version='',
                                oai_pmh_url='https://www.doabooks.org/oai?verb=ListRecords&metadataPrefix=oai_dc'
                                             '&from={start_date}&until={end_date}',
                                csv_url='http://www.doabooks.org/doab?func=csv')

    # optional
    create_release_custom = create_release

    # required
    download_custom = download
    transform_custom = transform


class DoabRelease(StreamRelease):
    @property
    def oai_pmh_path(self) -> str:
        return self.get_path(SubFolder.downloaded, 'oai_pmh', 'json')

    @property
    def csv_path(self) -> str:
        return self.get_path(SubFolder.downloaded, 'doab', 'csv')

    @property
    def token_path(self) -> str:
        return self.get_path(SubFolder.downloaded, "continuation_token", "txt")


def extract_entries(url: str, entries_path: str, next_token: str = None, success: bool = True) -> \
        Tuple[bool, Union[str, None], Union[str, None]]:
    """ Extract the OAI-PMH results from the given url until no new token is returned or a RetryError occurs. The
    extracted entries are appended to a json file, with 1 list per request.
    :param url: The doab OAI-PMH url
    :param entries_path: Path to the file in which events are stored
    :param next_token: The next resumptionToken, this is in the response of the api
    :param success: Whether all events were extracted successfully or an error occurred
    :return: success, next_cursor and number of total events
    """
    headers = {
        'User-Agent': f'{get_ao_user_agent()}'
    }

    tmp_url = url.split('&')[0] + f'&resumptionToken={next_token}' if next_token else url
    print(tmp_url)
    try:
        response = retry_session().get(tmp_url, headers=headers)
    except RetryError:
        return False, next_token, None
    if response.status_code == 200:
        response_dict = xmltodict.parse(response.content.decode('utf-8'))
        try:
            total_entries = response_dict['OAI-PMH']['ListRecords']['resumptionToken']['@completeListSize']
            entries = response_dict['OAI-PMH']['ListRecords']['record']
        except KeyError:
            return True, None, '0'
        try:
            next_token = response_dict['OAI-PMH']['ListRecords']['resumptionToken']['#text']
        except KeyError:
            next_token = None
        # write entries so far
        with open(entries_path, 'a') as f:
            with jsonlines.Writer(f) as writer:
                writer.write_all([entries])
        if next_token:
            success, next_token, total_entries = extract_entries(url, entries_path, next_token, success)
            return success, next_token, total_entries
        else:
            return True, None, total_entries
    else:
        raise ConnectionError(f"Error requesting url: {url}, response: {response.text}")


def convert(k):
    if len(k.split(':')) > 1:
        k = k.split(':')[1]
    if k.startswith('@') or k.startswith('#'):
        k = k[1:]
    k = k.replace('-', '_')
    k = k.replace(' ', '_')
    return k.lower()


def change_keys(obj, convert):
    """ Recursively goes through the dictionary obj, replaces keys with the convert function, adds info from the csv
    file based on the ISBN and other small modifications.

    :param obj: object, can be of any type
    :param convert: convert function
    :return: Updated dictionary
    """
    if isinstance(obj, (str, int, float)):
        return obj
    if isinstance(obj, dict):
        new = obj.__class__()
        for k, v in list(obj.items()):
            if k.startswith('@xmlns') or k.startswith('@xsi'):
                pass
            elif k == 'metadata':
                oai_dc = change_keys(v['oai_dc:dc'], convert)
                new['metadata'] = oai_dc
                for identifier in oai_dc['identifier']:
                    if 'ISBN' in identifier:
                        isbn = oai_dc['identifier'][2].split(' ')[1]
                        try:
                            csv_dict = csv_entries[isbn]
                            csv_dict = change_keys(csv_dict, convert)
                            new['csv'] = csv_dict
                            break
                        except KeyError:
                            pass
            # check valid datetime string
            elif k == 'Added on date':
                try:
                    pendulum.parse(v)
                    new[convert(k)] = change_keys(v, convert)
                except ParserError:
                    pass
            # store value of repeated field in list
            elif k in ['setSpec', 'dc:identifier', 'dc:creator', 'dc:language', 'dc:subject', 'Subjects']:
                if k == 'Subjects':
                    v = v.split('; ')
                v = v if isinstance(v, list) else [v]
                v = list(filter(None, v))
                new[convert(k)] = change_keys(v, convert)
            else:
                new[convert(k)] = change_keys(v, convert)
    elif isinstance(obj, (list, set, tuple)):
        new = obj.__class__(change_keys(v, convert) for v in obj)
    else:
        return obj
    return new