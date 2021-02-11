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

import jsonlines
import pendulum
from airflow.exceptions import AirflowException
from airflow.models.taskinstance import TaskInstance
from observatory.platform.telescopes.stream_telescope import (StreamRelease, StreamTelescope)
from observatory.platform.utils.airflow_utils import AirflowVars
from observatory.platform.utils.telescope_utils import convert
from observatory.platform.utils.template_utils import upload_files_from_list
from observatory.platform.utils.url_utils import get_ao_user_agent, retry_session


# TODO
# isbn have '-' sometimes, so not an integer, leave in?

class OapenMetadataRelease(StreamRelease):
    def __init__(self, dag_id: str, start_date: pendulum.Pendulum, end_date: pendulum.Pendulum, first_release: bool):
        super().__init__(dag_id, start_date, end_date, first_release)

    @property
    def csv_path(self) -> str:
        """ Path to store the original oapen metadata csv file"""
        return os.path.join(self.download_folder, 'oapen_metadata.csv')

    @property
    def transform_path(self) -> str:
        """ Path to store the transformed oapen metadata file"""
        return os.path.join(self.transform_folder, 'metadata.jsonl')

    def download(self) -> bool:
        """ Download Oapen metadata CSV.
        :return: True if download is successful
        """
        logging.info('Downloading csv')
        headers = {
            'User-Agent': f'{get_ao_user_agent()}'
        }
        response = retry_session().get(OapenMetadataTelescope.CSV_URL, headers=headers)
        if response.status_code == 200:
            with open(self.csv_path, 'w') as f:
                f.write(response.content.decode('utf-8'))
            logging.info('Download csv successful')
            return True
        else:
            raise AirflowException(f'Download csv unsuccessful, {response.text}')

    def transform(self):
        """ Transform the oapen metadata csv file by storing in a jsonl format and restructuring lists/dicts
        :return: None
        """
        with open(self.csv_path, 'r') as f:
            csv_entries = [{k: v for k, v in row.items()} for row in csv.DictReader(f, skipinitialspace=True)]

        # restructure entries dict
        nested_fields = set([key.rsplit('.', 1)[0] for key in csv_entries[0].keys() if len(key.rsplit('.', 1)) > 1])
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
        entries = change_keys(csv_entries, convert, nested_fields, list_fields)

        # write out the transformed data
        with open(self.transform_path, 'w') as json_out:
            with jsonlines.Writer(json_out) as writer:
                for entry in entries:
                    writer.write_all([entry])


class OapenMetadataTelescope(StreamTelescope):
    """ Oapen Metadata telescope """
    DAG_ID = 'oapen_metadata'
    CSV_URL = 'https://library.oapen.org/download-export?format=csv'

    def __init__(self):
        self.dag_id = OapenMetadataTelescope.DAG_ID
        self.start_date = datetime(2018, 5, 14)
        self.schedule_interval = '@weekly'
        self.dataset_id = 'oapen'
        self.merge_partition_field = 'id'
        self.updated_date_field = 'dc.date.available'
        self.bq_merge_days = 0

        self.description = 'oapen metadata telescope'
        self.schema_prefix = 'oapen_'
        self.airflow_vars = [AirflowVars.DATA_PATH, AirflowVars.PROJECT_ID, AirflowVars.DATA_LOCATION,
                             AirflowVars.DOWNLOAD_BUCKET, AirflowVars.TRANSFORM_BUCKET]

        super().__init__(self.dag_id, self.start_date, self.schedule_interval, self.dataset_id,
                         self.merge_partition_field, self.updated_date_field, self.bq_merge_days,
                         schema_prefix=self.schema_prefix, airflow_vars=self.airflow_vars)

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

    @staticmethod
    def make_release(**kwargs) -> 'OapenMetadataRelease':
        # Make Release instance
        ti: TaskInstance = kwargs['ti']
        start_date, end_date, first_release = ti.xcom_pull(key=OapenMetadataTelescope.RELEASE_INFO,
                                                           include_prior_dates=True)

        release = OapenMetadataRelease(OapenMetadataTelescope.DAG_ID, start_date, end_date, first_release)
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


def change_keys(obj, convert, nested_fields, list_fields):
    """ Recursively goes through the dictionary obj, replaces keys with the convert function.
    :param obj: object, can be of any type
    :param convert: convert function
    :param nested_fields: nested fields
    :return: Updated dictionary
    """
    if isinstance(obj, (str, int, float)):
        return obj
    if isinstance(obj, dict):
        new = obj.__class__()
        for k, v in list(obj.items()):
            if v == '':
                continue
            if k in list_fields:
                if k == 'BITSTREAM ISBN':
                    v = v.replace('-', '')
                if k == 'dc.subject.other':
                    v = v.replace(';', '||')
                    v = v.replace(',', '||')
                v = list(set([x.strip() for x in v.split('||')]))
                if k == 'dc.date.issued':
                    v = [pendulum.parse(date).to_date_string() for date in v]
                if k == 'dc.subject.classification':
                    # Get classification code for custom added column
                    classification_code = []
                    # tmp_list = []
                    for c in v:
                        if c.startswith('bic Book Industry Communication::'):
                            code = c.split('::')[-1].split(' ')[0]
                            classification_code.append(
                                code)  # c = c[len('bic Book Industry Communication::'):]  # c_list = c.split('::')
                            # tmp_list += c_list
                        else:
                            classification_code.append(c)
                    classification_code = list(
                        set(classification_code))  #         tmp_list.append(c)  # v = list(set(tmp_list))
            if k in nested_fields:
                k = k + '.value'
            if k.rsplit('.', 1)[0] in nested_fields:
                fields = k.split('.')
                tmp = new
                for key in fields:
                    key = convert(key)
                    try:
                        tmp[key]
                    except KeyError:
                        if key == fields[-1]:
                            tmp[key] = change_keys(v, convert, nested_fields, list_fields)
                            if key == 'classification':
                                # Add classification code column
                                tmp['classification_code'] = change_keys(classification_code, convert, nested_fields,
                                                                         list_fields)
                        else:
                            tmp[key] = {}
                    tmp = tmp[key]
            else:
                new[convert(k)] = change_keys(v, convert, nested_fields, list_fields)
    elif isinstance(obj, (list, set, tuple)):
        new = obj.__class__(change_keys(v, convert, nested_fields, list_fields) for v in obj)
    else:
        return obj
    return new
