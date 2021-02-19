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
from observatory.platform.telescopes.stream_telescope import (StreamRelease, StreamTelescope)
from observatory.platform.utils.airflow_utils import AirflowVars
from observatory.platform.utils.telescope_utils import convert, list_to_jsonl_gz
from observatory.platform.utils.template_utils import upload_files_from_list
from observatory.platform.utils.url_utils import get_ao_user_agent, retry_session


class OapenMetadataRelease(StreamRelease):
    def __init__(self, dag_id: str, start_date: pendulum.Pendulum, end_date: pendulum.Pendulum, first_release: bool):
        """ Construct a OapenMetadataRelease instance
        :param dag_id: the id of the DAG.
        :param start_date: the start_date of the release.
        :param end_date: the end_date of the release.
        :param first_release: whether this is the first release that is processed for this DAG
        """
        super().__init__(dag_id, start_date, end_date, first_release)

    @property
    def csv_path(self) -> str:
        """ Path to store the original oapen metadata csv file"""
        return os.path.join(self.download_folder, 'oapen_metadata.csv')

    @property
    def transform_path(self) -> str:
        """ Path to store the transformed oapen metadata file"""
        return os.path.join(self.transform_folder, 'metadata.jsonl.gz')

    def download(self) -> bool:
        """ Download Oapen metadata CSV.
        :return: True if download is successful
        """
        logging.info(f'Downloading csv from url: {OapenMetadataTelescope.CSV_URL}')
        headers = {
            'User-Agent': f'{get_ao_user_agent()}'
        }
        response = retry_session().get(OapenMetadataTelescope.CSV_URL, headers=headers)
        if response.status_code == 200:
            with open(self.csv_path, 'w') as f:
                f.write(response.content.decode('utf-8'))
            logging.info(f'Downloaded csv successful to {self.csv_path}')
            return True
        else:
            raise AirflowException(f'Download csv unsuccessful, {response.text}')

    def transform(self):
        """ Transform the oapen metadata csv file by storing in a jsonl format and restructuring lists/dicts.
        Values of field names with '.' are formatted into nested dictionaries.
        Values of field names in list_fields are split on - , ; and ||.
        Values of the field 'dc.subject.classification' are parsed to create a custom field 'classification_code'.
        See our readthedocs for an example row before and after transformation
        :return: None
        """
        with open(self.csv_path, 'r') as f:
            csv_entries = [{k: v for k, v in row.items()} for row in csv.DictReader(f, skipinitialspace=True)]

        # fieldnames with '.' are nested dictionaries
        nested_fields = set([key.rsplit('.', 1)[0] for key in csv_entries[0].keys() if len(key.rsplit('.', 1)) > 1])
        # values of these fields should be transformed to a list
        list_fields = {'oapen.grant.number', 'oapen.grant.acronym', 'oapen.relation.hasChapter_dc.title',
                       'dc.subject.classification', 'oapen.grant.program', 'oapen.redirect', 'oapen.imprint',
                       'dc.subject.other', 'oapen.notes', 'dc.title', 'collection', 'dc.relation.ispartofseries',
                       'BITSTREAM Download URL', 'BITSTREAM ISBN', 'oapen.collection', 'dc.contributor.author',
                       'oapen.remark.public', 'oapen.relation.isPublisherOf', 'dc.contributor.other',
                       'dc.contributor.editor', 'dc.relation.isreplacedbydouble', 'dc.date.issued',
                       'dc.description.abstract', 'BITSTREAM Webshop URL', 'dc.type', 'dc.identifier.isbn',
                       'dc.description.provenance', 'oapen.grant.project', 'oapen.relation.isbn', 'dc.identifier',
                       'dc.date.accessioned', 'BITSTREAM License', 'oapen.relation.isFundedBy_grantor.name',
                       'dc.relation.isnodouble', 'dc.language', 'grantor.number'}
        entries = transform_dict(csv_entries, convert, nested_fields, list_fields)

        # Transform release into JSON Lines format saving in memory buffer
        # Save in memory buffer to gzipped file
        list_to_jsonl_gz(self.transform_path, entries)


class OapenMetadataTelescope(StreamTelescope):
    """ Oapen Metadata telescope """
    CSV_URL = 'https://library.oapen.org/download-export?format=csv'

    def __init__(self, dag_id: str = 'oapen_metadata', start_date: datetime = datetime(2018, 5, 14),
                 schedule_interval: str = '@weekly', dataset_id: str = 'oapen', merge_partition_field: str = 'id',
                 updated_date_field: str = 'dc.date.available', bq_merge_days: int = 7, schema_prefix: str =
                 'oapen_', airflow_vars=None):
        """ Construct a OapenMetadataTelescope instance.
        :param dag_id: the id of the DAG.
        :param start_date: the start date of the DAG.
        :param schedule_interval: the schedule interval of the DAG.
        :param dataset_id: the dataset id.
        :param merge_partition_field: the BigQuery field used to match partitions for a merge
        :param updated_date_field: the BigQuery field used to determine newest entry for a merge
        :param bq_merge_days: how often partitions should be merged (every x days)
        :param schema_prefix: the prefix used to find the schema path
        :param airflow_vars: list of airflow variable keys, for each variable it is checked if it exists in airflow
        """
        if airflow_vars is None:
            airflow_vars = [AirflowVars.DATA_PATH, AirflowVars.PROJECT_ID, AirflowVars.DATA_LOCATION,
                            AirflowVars.DOWNLOAD_BUCKET, AirflowVars.TRANSFORM_BUCKET]
        super().__init__(dag_id, start_date, schedule_interval, dataset_id, merge_partition_field,
                         updated_date_field, bq_merge_days, schema_prefix=schema_prefix, airflow_vars=airflow_vars)

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

    def make_release(self, **kwargs) -> 'OapenMetadataRelease':
        # Make Release instance
        ti: TaskInstance = kwargs['ti']
        start_date, end_date, first_release = ti.xcom_pull(key=OapenMetadataTelescope.RELEASE_INFO,
                                                           include_prior_dates=True)

        release = OapenMetadataRelease(self.dag_id, start_date, end_date, first_release)
        return release

    def download(self, release: OapenMetadataRelease, **kwargs):
        """ Task to download the OapenMetadataRelease release.
        :param release: an OapenMetadataRelease instance.
        :return: None.
        """
        # Download release
        release.download()

    def upload_downloaded(self, release: OapenMetadataRelease, **kwargs):
        """ Task to upload the downloadeded OapenMetadataRelease release.
        :param release: an OapenMetadataRelease instance.
        :return: None.
        """
        # Upload each downloaded release
        upload_files_from_list(release.download_files, release.download_bucket)

    def transform(self, release: OapenMetadataRelease, **kwargs):
        """ Task to transform the OapenMetadataRelease release.
        :param release: an OapenMetadataRelease instance.
        :return: None.
        """
        # Transform each release
        release.transform()


def transform_value_to_list(k: str, v: str) -> Tuple[list, list]:
    # Get classification code for custom added column
    classification_code = []
    if k == 'BITSTREAM ISBN':
        v = v.replace('-', '')
    if k == 'dc.subject.other':
        v = v.replace(';', '||')
        v = v.replace(',', '||')
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


def transform_key_to_nested_dict(k: str, v, nested_fields: set, list_fields: set, classification_code: list, new):
    fields = k.split('.')
    tmp = new
    for key in fields:
        key = convert(key)
        try:
            tmp[key]
        except KeyError:
            if key == fields[-1]:
                tmp[key] = transform_dict(v, convert, nested_fields, list_fields)
                if key == 'classification':
                    # Add classification code column
                    tmp['classification_code'] = transform_dict(classification_code, convert, nested_fields,
                                                                list_fields)
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
            if v == '':
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
