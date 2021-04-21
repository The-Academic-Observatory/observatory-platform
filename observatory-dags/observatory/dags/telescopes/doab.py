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
from typing import Tuple

import pendulum
from airflow.exceptions import AirflowException
from airflow.models.taskinstance import TaskInstance
from observatory.platform.telescopes.stream_telescope import StreamRelease, StreamTelescope
from observatory.platform.utils.airflow_utils import AirflowVars
from observatory.platform.utils.telescope_utils import convert, list_to_jsonl_gz
from observatory.platform.utils.template_utils import upload_files_from_list
from observatory.platform.utils.url_utils import get_ao_user_agent, retry_session


class DoabRelease(StreamRelease):
    def __init__(self, dag_id: str, start_date: pendulum.Pendulum, end_date: pendulum.Pendulum, first_release: bool):

        download_files_regex = "doab.csv"
        transform_files_regex = "doab.jsonl.gz"
        super().__init__(dag_id, start_date, end_date, first_release, download_files_regex=download_files_regex,
                         transform_files_regex=transform_files_regex)

    @property
    def csv_path(self) -> str:
        return os.path.join(self.download_folder, 'doab.csv')

    @property
    def transform_path(self) -> str:
        return os.path.join(self.transform_folder, 'doab.jsonl.gz')

    def download(self) -> bool:
        """ Download DOAB CSV.
        :return: True if download is successful
        """
        logging.info(f'Downloading csv from url: {DoabTelescope.CSV_URL}')
        headers = {
            'User-Agent': f'{get_ao_user_agent()}'
        }
        response = retry_session().get(DoabTelescope.CSV_URL, headers=headers)
        if response.status_code == 200:
            with open(self.csv_path, 'w') as f:
                f.write(response.content.decode('utf-8'))
            logging.info(f'Downloaded csv successful to {self.csv_path}')
            return True
        else:
            raise AirflowException(f'Download csv unsuccessful, {response.text}')

    def transform(self):
        """ Transform the doab csv file by storing in a jsonl format and restructuring lists/dicts.
        Values of field names with '.' are formatted into nested dictionaries.
        Values of field names in list_fields are split on - , ; and ||.
        Values of the field 'dc.subject.classification' are parsed to create a custom field 'classification_code'.
        See our readthedocs for an example row before and after transformation
        :return: None
        """
        with open(self.csv_path, 'r') as f:
            csv_entries = [{k: v for k, v in row.items()} for row in csv.DictReader(f, skipinitialspace=True)]

        nested_fields = get_nested_fieldnames(csv_entries)
        # values of these fields should be transformed to a list
        # list_fields = set()
        # for entry in csv_entries:
        #     for k, v in entry.items():
        #         if '||' in v:
        #             list_fields.add(k)
        list_fields = {'BITSTREAM Download URL', 'BITSTREAM License', 'BITSTREAM Webshop URL', 'dc.contributor',
                       'dc.contributor.author', 'dc.contributor.editor', 'dc.contributor.other', 'dc.date.available', 'dc.date.issued',
                       'dc.date.submitted', 'dc.dateSubmitted', 'dc.description.abstract', 'dc.description.provenance',
                       'dc.grantproject', 'dc.identifier', 'dc.language', 'dc.notes', 'dc.number', 'dc.redirect',
                       'dc.relation.ispartofseries', 'dc.relationisFundedBy', 'dc.subject', 'dc.subject.classification',
                       'dc.subject.other', 'dc.title', 'dc.type', 'oapen.collection', 'oapen.grant.number',
                       'oapen.grant.program', 'oapen.imprint', 'oapen.relation.hasChapter_dc.title',
                       'oapen.relation.isFundedBy', 'oapen.relation.isFundedBy_grantor.name',
                       'oapen.relation.isPartOfBook', 'oapen.relation.isPublishedBy_publisher.name',
                       'oapen.relation.isPublisherOf', 'oapen.relation.isbn', 'peerreview.publish.responsibility',
                       'peerreview.review.type', 'peerreview.reviewer.type'}
        entries = transform_dict(csv_entries, convert, nested_fields, list_fields)

        # Transform release into JSON Lines format saving in memory buffer
        # Save in memory buffer to gzipped file
        list_to_jsonl_gz(self.transform_path, entries)


class DoabTelescope(StreamTelescope):
    """
    The Directory of Open Access Books (DOAB) is a directory of open-access peer reviewed scholarly books.
    Its aim is to increase discoverability of books. Academic publishers provide metadata of their Open Access books
    to DOAB.
    """

    CSV_URL = 'https://directory.doabooks.org/download-export?format=csv'

    def __init__(self, dag_id: str = 'doab', start_date: datetime = datetime(2018, 5, 14),
                 schedule_interval: str = '@monthly', dataset_id: str = 'doab', merge_partition_field: str = 'id',
                 updated_date_field: str = 'dc.date.accessioned', bq_merge_days: int = 7, airflow_vars: list = None):
        if airflow_vars is None:
            airflow_vars = [AirflowVars.DATA_PATH, AirflowVars.PROJECT_ID, AirflowVars.DATA_LOCATION,
                            AirflowVars.DOWNLOAD_BUCKET, AirflowVars.TRANSFORM_BUCKET]
        super().__init__(dag_id, start_date, schedule_interval, dataset_id, merge_partition_field, updated_date_field,
                         bq_merge_days, airflow_vars=airflow_vars)

        self.add_setup_task_chain([self.check_dependencies,
                                   self.get_release_info])
        self.add_task_chain(
            [self.download,
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
        start_date, end_date, first_release = ti.xcom_pull(key=DoabTelescope.RELEASE_INFO, include_prior_dates=True)

        release = DoabRelease(self.dag_id, start_date, end_date, first_release)
        return release

    def download(self, release: DoabRelease, **kwargs):
        """ Task to download the Doab release.
        :param release: a DoabRelease instance.
        :return: None.
        """
        # Download release
        release.download()

    def upload_downloaded(self, release: DoabRelease, **kwargs):
        """ Task to upload the Doab release.
        :param release: a DoabRelease instance.
        :return: None.
        """
        # Upload each downloaded release
        upload_files_from_list(release.download_files, release.download_bucket)

    def transform(self, release: DoabRelease, **kwargs):
        """ Task to transform the Doab release.
        :param release: an DoabRelease instance.
        :return: None.
        """
        # Transform each release
        release.transform()


def get_nested_fieldnames(csv_entries: dict) -> set:
    """ Fieldnames with '.' should be converted to nested dictionaries. This function will return a set of
    fieldnames for nested dictionaries from the highest to second lowest levels.
    E.g these fieldnames: dc.date.available, dc.date.issued, dc.description
    Will give this set: dc, dc.date, dc.description

    :param csv_entries: Dictionary with csv entries
    :return: Set of field names which should be converted to nested fields
    """
    keys = csv_entries[0].keys()
    nested_fields = set()
    for key in keys:
        # split string in two, starting from the right
        split_key = key.rsplit('.', 1)
        # add key to set if there is at least one '.' in key name
        if len(split_key) > 1:
            nested_fields.add(split_key[0])
    return nested_fields


def transform_value_to_list(k: str, v: str) -> Tuple[list, list]:
    """ Takes a key and value from the dictionary of csv entries. The value is always in a string format. Based on the
    key name (k) the delimiter in the the value (v) is replaced with '||'. All values are then split on '||' so they
    are transformed into a list. The values of 'dc.subject.classification' are parsed and stored in a variable,
    so they can later be added to a custom key of the csv entries dictionary.

    :param k: Dictionary key
    :param v: Dictionary value (string)
    :return: Dictionary value (list) and classification code info.
    """
    # Get classification code for custom added column
    classification_code = []
    if k == 'BITSTREAM ISBN':
        v = v.replace('-', '')
    v = list(dict.fromkeys([x.strip() for x in v.split('||')]))
    if k == 'dc.date.issued':
        v = [pendulum.parse(date).to_date_string() for date in v]
    if k == 'dc.subject.classification':
        for c in v:
            if c.startswith('bic Book Industry Communication::'):
                code = c.split('::')[-1].split(' ')[0]
                classification_code.append(code)
            else:
                classification_code.append(c)
        classification_code = list(dict.fromkeys(classification_code))
    return v, classification_code


def transform_key_to_nested_dict(k: str, v, nested_fields: set, list_fields: set, classification_code: list, new: dict):
    """ Takes a dictionary key and value. The key is split on '.', a nested dictionary is created and the value will be
    added to the most nested level. The dictionary is updated in place so it is not returned.
    For example first k = 'dc.date.issued', the dictionary = {'dc': {'date': {'issued': v1}}
    Second k = 'dc.date.accessed', the dictionary = {'dc': {'date': {'issued': v1, 'accessed': v2}}

    :param k: Dictionary key
    :param v: Dictionary value
    :param nested_fields: Set of field names for which the values should be a nested dictionary
    :param list_fields: Set of field names for which the values should be a list
    :param classification_code: List of classification code abbreviations
    :param new: New, updated dictionary
    :return: None.
    """
    # Get all (nested) fields of a specific key
    fields = k.split('.')
    tmp = new
    # Create one dictionary for each field
    for key in fields:
        key = convert(key)
        try:
            tmp[key]
        except KeyError:
            # Add the value to the most nested level
            if key == fields[-1]:
                tmp[key] = transform_dict(v, convert, nested_fields, list_fields)
                if key == 'classification':
                    # Add classification code column
                    tmp['classification_code'] = transform_dict(classification_code, convert, nested_fields,
                                                                list_fields)
            # Create empty dictionary for key
            else:
                tmp[key] = {}
        tmp = tmp[key]


def transform_dict(obj, convert, nested_fields, list_fields):
    """ Recursively goes through the dictionary obj, replaces keys with the convert function.
    :param obj: object, can be of any type
    :param convert: convert function
    :param nested_fields: fields
    :param list_fields:
    :return: Updated dictionary
    """
    if isinstance(obj, (str, int, float)):
        return obj
    if isinstance(obj, dict):
        new = obj.__class__()
        for k, v in list(obj.items()):
            classification_code = []
            if not v:
                continue
            if k in list_fields:
                v, classification_code = transform_value_to_list(k, v)
            # change key name for top level of nested dictionary
            if k in nested_fields:
                k = k + '.value'
            # parse key/value of lower levels of nested dictionary
            if k.rsplit('.', 1)[0] in nested_fields:
                transform_key_to_nested_dict(k, v, nested_fields, list_fields, classification_code, new)
            else:
                new[convert(k)] = transform_dict(v, convert, nested_fields, list_fields)
    elif isinstance(obj, (list, set, tuple)):
        new = obj.__class__(transform_dict(v, convert, nested_fields, list_fields) for v in obj)
    else:
        return obj
    return new
