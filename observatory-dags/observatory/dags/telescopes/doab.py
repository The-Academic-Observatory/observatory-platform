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
import logging
import os
import pathlib
from datetime import datetime
from types import SimpleNamespace
from typing import Tuple, Union

import jsonlines
import pendulum
import xmltodict
from airflow.exceptions import AirflowException
from airflow.models.taskinstance import TaskInstance
from observatory.platform.telescopes.stream_telescope import StreamRelease, StreamTelescope
from observatory.platform.utils.telescope_utils import list_to_jsonl_gz
from observatory.platform.utils.template_utils import upload_files_from_list
from observatory.platform.utils.airflow_utils import AirflowVars
from observatory.platform.utils.url_utils import get_ao_user_agent, retry_session
from pendulum.parsing.exceptions import ParserError
from requests.exceptions import RetryError


# def create_release(start_date: pendulum.Pendulum, end_date: pendulum.Pendulum, telescope: SimpleNamespace,
#                    first_release: bool = False) -> 'DoabRelease':
#     """ Create a release based on the start and end date.
#
#     :param start_date: Start date of this run
#     :param end_date: End date of this run
#     :param telescope: Contains telescope properties
#     :param first_release: Whether this is the first release to be obtained
#     :return: Release instance
#     """
#     release = DoabRelease(start_date, end_date, telescope, first_release)
#     return release


# def download(release: 'DoabRelease') -> bool:
#     """ Download both using the OAI-PMH harvester and from a csv file.
#
#     :param release: Release instance
#     :return: True if both downloads are successful
#     """
#     success_oai_pmh = download_oai_pmh(release)
#     success_csv = download_csv(release)
#
#     logging.info(f"OAI-PMH status: {success_oai_pmh}, CSV status: {success_csv}")
#     return True if success_oai_pmh and success_csv else False


class DoabRelease(StreamRelease):
    def __init__(self, dag_id: str, start_date: pendulum.Pendulum, end_date: pendulum.Pendulum, first_release: bool):

        download_files_regex = r"\b(oai_pmh.json|doab.csv)\b"

        super().__init__(dag_id, start_date, end_date, first_release, download_files_regex=download_files_regex)

        # returns records modified or added between start and end date
        self.oai_pmh_url = 'https://directory.doabooks.org/oai/request?verb=ListRecords&metadataPrefix=oai_dc&from=' \
                           '{start_date}&until={end_date}'.format(start_date=self.start_date.strftime("%Y-%m-%d"),
                                                                  end_date=self.end_date.strftime("%Y-%m-%d"))
        self.csv_url = 'https://directory.doabooks.org/download-export?format=csv'

    @property
    def oai_pmh_path(self) -> str:
        return os.path.join(self.download_folder, 'oai_pmh.json')

    @property
    def csv_path(self) -> str:
        return os.path.join(self.download_folder, 'doab.csv')

    @property
    def token_path(self) -> str:
        return os.path.join(self.download_folder, "continuation_token.txt")

    @property
    def transform_path(self) -> str:
        return os.path.join(self.transform_folder, 'doab.jsonl.gz')

    def download(self):
        success_oai_pmh = self.download_oai_pmh()
        success_csv = self.download_csv()

        logging.info(f"OAI-PMH status: {success_oai_pmh}, CSV status: {success_csv}")
        return True if success_oai_pmh and success_csv else False

    def download_oai_pmh(self) -> bool:
        """ Download DOAB metadata using an OAI-PMH harvester.

        :param release: Release instance
        :return: True if download is successful else False
        """
        logging.info('Downloading OAI-PMH')
        url = self.oai_pmh_url
        # check if cursor files exist from a previous failed request
        if os.path.isfile(self.token_path):
            # retrieve token
            with open(self.token_path, 'r') as f:
                next_token = json.load(f)
            # delete file
            pathlib.Path(self.token_path).unlink()
            # extract entries
            success, next_token, total_entries = extract_entries(url, self.oai_pmh_path, next_token)
        # if entries download path exists but no token file, the previous request has finished & successful
        elif os.path.isfile(self.oai_pmh_path):
            success = True
            next_token = None
            total_entries = 'See previous successful attempt'
        # first time request
        else:
            # extract events
            success, next_token, total_entries = extract_entries(url, self.oai_pmh_path)

        if not success:
            with open(self.token_path, 'w') as f:
                json.dump(next_token, f)
            raise AirflowException(f'Download unsuccessful, status: {success}')

        logging.info(f'OAI-PMH successful, total entries={total_entries}')
        return True if success and total_entries != '0' else False

    def download_csv(self) -> bool:
        """ Download DOAB metadata from a csv file.

        :param release: Release instance
        :return: True if download is successful
        """
        logging.info('Downloading csv')
        headers = {
            'User-Agent': f'{get_ao_user_agent()}'
        }
        response = retry_session().get(self.csv_url, headers=headers)
        if response.status_code == 200:
            with open(self.csv_path, 'w') as f:
                f.write(response.content.decode('utf-8'))
            logging.info('Download csv successful')
            return True
        else:
            raise AirflowException(f'Download csv unsuccessful, {response.text}')

    def transform(self):
        """ For each row, combine the OAI-PMH and csv results into one json object.
        The column names are structure are slightly changed to make it readable by BigQuery.

        :param release: Release instance
        :return: None
        """
        # create dict of data in summary xml file
        oai_pmh_entries = []
        with open(self.oai_pmh_path, 'r') as f:
            for line in f:
                entries_page = json.loads(line)
                oai_pmh_entries.append(entries_page)

        # create global dict with csv entries of specific columns
        global csv_entries
        with open(self.csv_path, 'r') as f:
            csv_entries = {row['ISBN'].strip().split(' ')[0]: {
                'ISSN': row['ISSN'],
                'Volume': row['Volume'],
                'Pages': row['Pages'],
                'Series title': row['Series title'],
                'Added on date': row['Added on date'],
                'Subjects': row['Subjects']
            } for row in csv.DictReader(f, skipinitialspace=True)}

        # change keys & delete some values
        entries = change_keys(oai_pmh_entries, convert)

        # write out the transformed data
        list_to_jsonl_gz(self.transform_path, entries)
        # with open(self.transform_path, 'w') as json_out:
        #     with jsonlines.Writer(json_out) as writer:
        #         for entry_page in entries:
        #             for doab_entry in entry_page:
        #                 writer.write_all([doab_entry])


class DoabTelescope(StreamTelescope):
    """
    DOAB telescope
    """
    def __init__(self, dag_id: str = 'doab', start_date: datetime = datetime(2018, 5, 14), schedule_interval: str = '@weekly',
                 dataset_id: str = 'doab', merge_partition_field: str = 'header.identifier',
                 updated_date_field: str = 'header.datestamp', bq_merge_days: int = 7, airflow_vars: list = None):
        if airflow_vars is None:
            airflow_vars = [AirflowVars.DATA_PATH, AirflowVars.PROJECT_ID, AirflowVars.DATA_LOCATION,
                            AirflowVars.DOWNLOAD_BUCKET, AirflowVars.TRANSFORM_BUCKET]
        super().__init__(dag_id, start_date, schedule_interval, dataset_id, merge_partition_field, updated_date_field,
                         bq_merge_days, airflow_vars=airflow_vars)

        self.add_setup_task_chain([self.check_dependencies,
                                   self.get_release_info])
        self.add_task_chain([self.download,
                             self.upload_downloaded,
                             self.transform,
                             self.upload_transformed,
                             self.bq_load_partition])
        self.add_task_chain(self.make_operators([self.bq_delete_old,
                                                 self.bq_append_new,
                                                 self.cleanup]))

    def make_release(self, **kwargs) -> 'DoabRelease':
        # Make Release instance
        ti: TaskInstance = kwargs['ti']
        start_date, end_date, first_release = ti.xcom_pull(key=DoabTelescope.RELEASE_INFO,
                                                           include_prior_dates=True)

        release = DoabRelease(self.dag_id, start_date, end_date, first_release)
        return release

    def download(self, release: DoabRelease, **kwargs):
        """ Task to download the OapenMetadataRelease release.
        :param release: an OapenMetadataRelease instance.
        :return: None.
        """
        # Download release
        release.download()

    def upload_downloaded(self, release: DoabRelease, **kwargs):
        """ Task to upload the downloadeded OapenMetadataRelease release.
        :param release: an OapenMetadataRelease instance.
        :return: None.
        """
        # Upload each downloaded release
        upload_files_from_list(release.download_files, release.download_bucket)

    def transform(self, release: DoabRelease, **kwargs):
        """ Task to transform the OapenMetadataRelease release.
        :param release: an OapenMetadataRelease instance.
        :return: None.
        """
        # Transform each release
        release.transform()

#
#
# class DoabTelescope(StreamTelescope):
#     telescope = SimpleNamespace(dag_id='doab', dataset_id='doab2', main_table_id='doab',
#                                 schedule_interval='@weekly', start_date=datetime(2018, 5, 14),
#                                 partition_table_id='doab_partitions', description='the description',
#                                 queue='default', max_retries=3, bq_merge_days=0,
#                                 merge_partition_field='header.identifier',
#                                 updated_date_field='header.datestamp',
#                                 download_ext='json', extract_ext='json', transform_ext='jsonl',
#                                 airflow_vars=[AirflowVar.data_path.get(), AirflowVar.project_id.get(),
#                                               AirflowVar.data_location.get(),
#                                               AirflowVar.download_bucket_name.get(),
#                                               AirflowVar.transform_bucket_name.get()],
#                                 airflow_conns=[], schema_version='',
#                                 oai_pmh_url='https://www.doabooks.org/oai?verb=ListRecords&metadataPrefix=oai_dc'
#                                              '&from={start_date}&until={end_date}',
#                                 csv_url='http://www.doabooks.org/doab?func=csv')
#
#     # optional
#     create_release_custom = create_release
#
#     # required
#     download_custom = download
#     transform_custom = transform

#
# class DoabRelease(StreamRelease):
#     @property
#     def oai_pmh_path(self) -> str:
#         return self.get_path(SubFolder.downloaded, 'oai_pmh', 'json')
#
#     @property
#     def csv_path(self) -> str:
#         return self.get_path(SubFolder.downloaded, 'doab', 'csv')
#
#     @property
#     def token_path(self) -> str:
#         return self.get_path(SubFolder.downloaded, "continuation_token", "txt")



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
            elif k == 'dc:identifier':
                v_list = []
                for identifier in v:
                    id_type = identifier.split(':')[0]
                    if id_type == 'ISBN' or id_type == 'DOI':
                        id_value = identifier.split(':')[1].strip().strip(';')
                    else:
                        id_value = identifier
                    id_dict = {'type': id_type, 'value': id_value}
                    v_list.append(id_dict)
                v = v_list
                new[convert(k)] = change_keys(v, convert)
            elif k == 'metadata':
                oai_dc = change_keys(v['oai_dc:dc'], convert)
                new['metadata'] = oai_dc
                isbn = next((identifier for identifier in oai_dc['identifier'] if identifier['type'] == "ISBN"), None)
                if isbn:
                    try:
                        csv_dict = csv_entries[isbn['value']]
                        csv_dict = change_keys(csv_dict, convert)
                        new['csv'] = csv_dict
                        break
                    except KeyError:
                        pass
                else:
                    logging.info(f"Could not find matching isbn for {oai_dc['identifier']}")
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