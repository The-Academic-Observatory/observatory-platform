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

# Author: Aniek Roelofs, Richard Hosking

import os
import json
import pendulum
import subprocess
import pathlib
import shutil
import logging
import requests
import functools
import xml.etree.ElementTree as ET
from pendulum import Pendulum

from airflow import settings
from airflow.exceptions import AirflowException
from airflow.models import Connection
from airflow.models import Variable
from airflow.models.taskinstance import TaskInstance
from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook

from academic_observatory.utils.url_utils import retry_session
from academic_observatory.utils.data_utils import get_file
from academic_observatory.utils.config_utils import ObservatoryConfig, SubFolder, telescope_path, bigquery_schema_path, \
    debug_file_path, is_vendor_google


def xcom_pull_messages(ti):
    # Pull messages
    msgs_in = ti.xcom_pull(key=FundrefTelescope.XCOM_MESSAGES_NAME, task_ids=FundrefTelescope.TASK_ID_LIST,
                           include_prior_dates=False)
    config_dict = ti.xcom_pull(key=FundrefTelescope.XCOM_CONFIG_NAME, task_ids=FundrefTelescope.TASK_ID_SETUP,
                               include_prior_dates=False)
    environment = config_dict['environment']
    bucket = config_dict['bucket']
    project_id = config_dict['project_id']
    return msgs_in, environment, bucket, project_id


def list_releases(telescope_url):
    """

    :param telescope_url:
    :return: snapshot_dict with 'url' and 'release_date' in format 'YYYY-MM-DD'
    """
    snapshot_dict = {}

    response = retry_session().get(telescope_url).text

    if response:
        json_response = json.loads(response)

        for release in json_response:
            for source in release['assets']['sources']:
                if source['format'] == 'tar.gz':
                    release_date = pendulum.parse(release['released_at']).to_date_string()
                    snapshot_dict[source['url']] = release_date

    return snapshot_dict


def table_name(date: str):
    table_name = f"{FundrefTelescope.DAG_ID}_{date}".replace('-', '_')

    return table_name


def filepath_download(date: str):
    compressed_file_name = f"{FundrefTelescope.DAG_ID}_{date}.tar.gz".replace('-', '_')
    download_dir = telescope_path(FundrefTelescope.DAG_ID, SubFolder.downloaded)
    path = os.path.join(download_dir, compressed_file_name)

    return path


def filepath_extract(date: str):
    decompressed_file_name = f"{FundrefTelescope.DAG_ID}_{date}.rdf".replace('-', '_')
    extract_dir = telescope_path(FundrefTelescope.DAG_ID, SubFolder.extracted)
    path = os.path.join(extract_dir, decompressed_file_name)

    return path


def filepath_transform(date: str):
    decompressed_file_name = f"{FundrefTelescope.DAG_ID}_{date}.rdf".replace('-', '_')
    transform_dir = telescope_path(FundrefTelescope.DAG_ID, SubFolder.transformed)
    path = os.path.join(transform_dir, decompressed_file_name)

    return path


def filepath_geonames():
    file_name = FundrefTelescope.GEONAMES_FILE_NAME
    transform_dir = telescope_path(FundrefTelescope.DAG_ID, SubFolder.transformed)
    path = os.path.join(transform_dir, file_name)

    return path


def download_release(url: str, date: str):
    # add browser agent to prevent 403/forbidden error.
    header = {'User-Agent': 'Mozilla/5.0'}
    filename = filepath_download(date)
    with requests.get(url, headers=header, stream=True) as response:
        with open(filename, 'wb') as file:
            shutil.copyfileobj(response.raw, file)


def download_geonames_dump():
    filename = filepath_geonames().strip('.txt')
    filedir = os.path.dirname(filename)
    get_file(fname=filename, origin=FundrefTelescope.GEONAMES_URL, cache_subdir=filedir,
             extract=True, archive_format='zip')


def geonames_dump(geonames_dump_path):
    geonames_dict = {}
    with open(geonames_dump_path, 'r') as file:
        for line in file:
            id = line.split('\t')[0]
            name = line.split('\t')[1]
            country_code = line.split('\t')[8]
            geonames_dict[id] = (name, country_code)
    return geonames_dict


def get_geoname_data(nested, geoname_dict):
    geonameid = nested.attrib['{http://www.w3.org/1999/02/22-rdf-syntax-ns#}resource'].split('sws.geonames.org')[-1].strip('/')
    try:
        name = geoname_dict[geonameid][0]
        country_code = geoname_dict[geonameid][1]
    except KeyError:
        name = nested.attrib['{http://www.w3.org/1999/02/22-rdf-syntax-ns#}resource']
        country_code = None
        print(f'KeyError for {geonameid}')
    return name, country_code


def new_funder_template():
    """ Helper Function for creating a new Funder.
    :return: a blank funder object.
    """
    return {
        'funder': None,
        'pre_label': None,
        'alt_label': [],
        'narrower': [],
        'broader': [],
        'modified': None,
        'created': None,
        'funding_body_type': None,
        'funding_body_sub_type': None,
        'region': None,
        'country': None,
        'country_code': None,
        'state': None,
        'tax_id': None,
        'continuation_of': [],
        'renamed_as': [],
        'replaces': [],
        'affil_with': [],
        'merged_with': [],
        'incorporated_into': [],
        'is_replaced_by': [],
        'incorporates': [],
        'split_into': [],
        'status': None,
        'merger_of': [],
        'split_from': None,
        'formly_known_as': None,
        'notation': None
    }


def parse_fundref_registry_rdf(file_name, geonames_file_path):
    """ Helper function to parse a fundref registry rdf file and to return a python list containing each funder.
    :param file_name: the filename of the registry.rdf file to be parsed.
    :return: A python list containing all the funders parsed from the input rdf
    """
    funders = []
    funders_by_key = {}

    tree = ET.parse(file_name)
    root = tree.getroot()

    geoname_dict = geonames_dump(geonames_file_path)

    for record in root:
        if record.tag == "{http://www.w3.org/2004/02/skos/core#}ConceptScheme":
            for nested in record:
                if nested.tag == '{http://www.w3.org/2004/02/skos/core#}hasTopConcept':
                    funder_id = nested.attrib['{http://www.w3.org/1999/02/22-rdf-syntax-ns#}resource']
                    funders_by_key[funder_id] = new_funder_template()

        if record.tag == "{http://www.w3.org/2004/02/skos/core#}Concept":
            funder_id = record.attrib['{http://www.w3.org/1999/02/22-rdf-syntax-ns#}about']
            funder = funders_by_key[funder_id]
            funder['funder'] = funder_id
            geoname_country_code = None
            for nested in record:

                if nested.tag == '{http://www.w3.org/2004/02/skos/core#}inScheme':
                    continue

                elif nested.tag == '{http://www.w3.org/2008/05/skos-xl#}prefLabel':
                    funder['pre_label'] = nested[0][0].text

                elif nested.tag == '{http://www.w3.org/2008/05/skos-xl#}altLabel':
                    funder['alt_label'].append(nested[0][0].text)

                elif nested.tag == '{http://www.w3.org/2004/02/skos/core#}narrower':
                    funder['narrower'].append(nested.attrib['{http://www.w3.org/1999/02/22-rdf-syntax-ns#}resource'])

                elif nested.tag == '{http://www.w3.org/2004/02/skos/core#}broader':
                    funder['broader'].append(nested.attrib['{http://www.w3.org/1999/02/22-rdf-syntax-ns#}resource'])

                elif nested.tag == '{http://purl.org/dc/terms/}modified':
                    funder['modified'] = nested.text

                elif nested.tag == '{http://purl.org/dc/terms/}created':
                    funder['created'] = nested.text

                elif nested.tag == '{http://data.crossref.org/fundingdata/xml/schema/grant/grant-1.2/}fundingBodySubType':
                    funder['funding_body_type'] = nested.text

                elif nested.tag == '{http://data.crossref.org/fundingdata/xml/schema/grant/grant-1.2/}fundingBodyType':
                    funder['funding_body_sub_type'] = nested.text

                elif nested.tag == '{http://data.crossref.org/fundingdata/xml/schema/grant/grant-1.2/}region':
                    funder['region'] = nested.text

                elif nested.tag == '{http://data.crossref.org/fundingdata/xml/schema/grant/grant-1.2/}country':
                    geoname_name, geoname_country_code = get_geoname_data(nested, geoname_dict)
                    funder['country'] = geoname_name

                elif nested.tag == '{http://data.crossref.org/fundingdata/xml/schema/grant/grant-1.2/}state':
                    geoname_name, _ = get_geoname_data(nested, geoname_dict)
                    funder['state'] = geoname_name

                elif nested.tag == '{http://schema.org/}address':
                    if geoname_country_code is None:
                        print("Don't know country_code, referenced before country")
                        funder['country_code'] = nested[0][0].text
                    else:
                        funder['country_code'] = geoname_country_code

                elif nested.tag == '{http://data.crossref.org/fundingdata/xml/schema/grant/grant-1.2/}taxId':
                    funder['tax_id'] = nested.text

                elif nested.tag == '{http://data.crossref.org/fundingdata/xml/schema/grant/grant-1.2/}continuationOf':
                    funder['continuation_of'].append(
                        nested.attrib['{http://www.w3.org/1999/02/22-rdf-syntax-ns#}resource'])

                elif nested.tag == '{http://data.crossref.org/fundingdata/xml/schema/grant/grant-1.2/}renamedAs':
                    funder['renamed_as'].append(nested.attrib['{http://www.w3.org/1999/02/22-rdf-syntax-ns#}resource'])

                elif nested.tag == '{http://purl.org/dc/terms/}replaces':
                    funder['replaces'].append(nested.attrib['{http://www.w3.org/1999/02/22-rdf-syntax-ns#}resource'])

                elif nested.tag == '{http://data.crossref.org/fundingdata/xml/schema/grant/grant-1.2/}affilWith':
                    funder['affil_with'].append(nested.attrib['{http://www.w3.org/1999/02/22-rdf-syntax-ns#}resource'])

                elif nested.tag == '{http://data.crossref.org/fundingdata/xml/schema/grant/grant-1.2/}mergedWith':
                    funder['merged_with'].append(nested.attrib['{http://www.w3.org/1999/02/22-rdf-syntax-ns#}resource'])

                elif nested.tag == '{http://data.crossref.org/fundingdata/xml/schema/grant/grant-1.2/}incorporatedInto':
                    funder['incorporated_into'].append(
                        nested.attrib['{http://www.w3.org/1999/02/22-rdf-syntax-ns#}resource'])

                elif nested.tag == '{http://purl.org/dc/terms/}isReplacedBy':
                    funder['is_replaced_by'].append(
                        nested.attrib['{http://www.w3.org/1999/02/22-rdf-syntax-ns#}resource'])

                elif nested.tag == '{http://data.crossref.org/fundingdata/xml/schema/grant/grant-1.2/}incorporates':
                    funder['incorporates'].append(
                        nested.attrib['{http://www.w3.org/1999/02/22-rdf-syntax-ns#}resource'])

                elif nested.tag == '{http://data.crossref.org/fundingdata/xml/schema/grant/grant-1.2/}splitInto':
                    funder['split_into'].append(nested.attrib['{http://www.w3.org/1999/02/22-rdf-syntax-ns#}resource'])

                elif nested.tag == '{http://data.crossref.org/fundingdata/terms}status':
                    funder['status'] = nested.attrib['{http://www.w3.org/1999/02/22-rdf-syntax-ns#}resource']

                elif nested.tag == '{http://data.crossref.org/fundingdata/xml/schema/grant/grant-1.2/}mergerOf':
                    funder['merger_of'].append(nested.attrib['{http://www.w3.org/1999/02/22-rdf-syntax-ns#}resource'])

                elif nested.tag == '{http://data.crossref.org/fundingdata/xml/schema/grant/grant-1.2/}splitFrom':
                    funder['split_from'] = nested.attrib['{http://www.w3.org/1999/02/22-rdf-syntax-ns#}resource']

                elif nested.tag == '{http://data.crossref.org/fundingdata/xml/schema/grant/grant-1.2/}formerlyKnownAs':
                    funder['formly_known_as'] = nested.attrib['{http://www.w3.org/1999/02/22-rdf-syntax-ns#}resource']

                elif nested.tag == '{http://www.w3.org/2004/02/skos/core#}notation':
                    funder['notation'] = nested.text

                else:
                    print(nested)

            funders.append(funder)

    for funder in funders:
        children, returned_depth = recursive_funders(funders_by_key, funder, 0, 'narrower')
        funder["children"] = children
        funder['bottom'] = len(children) > 0

        parent, returned_depth = recursive_funders(funders_by_key, funder, 0, 'broader')
        funder["parents"] = parent
        funder['top'] = len(parent) > 0

    return funders


def recursive_funders(funders_by_key, funder, depth, direction):
    starting_depth = depth

    children = []

    for funder_id in funder[direction]:
        sub_funder = funders_by_key[funder_id]
        name = sub_funder['pre_label']

        returned, returned_depth = recursive_funders(funders_by_key, sub_funder, starting_depth + 1, direction)

        if direction == "narrower":
            child = {'funder': funder_id, 'name': name, 'children': returned}
        else:
            child = {'funder': funder_id, 'name': name, 'parent': returned}
        children.append(child)

        if returned_depth > depth:
            depth = returned_depth
    return children, depth


def write_list_to_jsonl(list, jsonl_path):
    with open(jsonl_path, 'w') as jsonl_out:
        jsonl_out.write('\n'.join(json.dumps(obj) for obj in list))


class FundrefTelescope:
    # example:
    TELESCOPE_URL = 'https://gitlab.com/api/v4/projects/crossref%2Fopen_funder_registry/releases'
    TELESCOPE_DEBUG_URL = 'debug_fundref_url'
    GEONAMES_FILE_NAME = 'allCountries.txt'
    GEONAMES_URL = 'https://download.geonames.org/export/dump/allCountries.zip'
    SCHEMA_FILE_PATH = bigquery_schema_path('fundref.json')
    DEBUG_FILE_PATH = debug_file_path('fundref.tar.gz')

    DAG_ID = 'fundref'
    DATASET_ID = DAG_ID
    XCOM_MESSAGES_NAME = "messages"
    XCOM_CONFIG_NAME = "config"
    TASK_ID_SETUP = "check_setup_requirements"
    TASK_ID_LIST = f"list_{DAG_ID}_releases"
    TASK_ID_STOP = f"stop_{DAG_ID}_workflow"
    TASK_ID_DOWNLOAD = f"download_{DAG_ID}_releases"
    TASK_ID_DECOMPRESS = f"decompress_{DAG_ID}_releases"
    TASK_ID_GEONAMES = "get_geonames_dump"
    TASK_ID_TRANSFORM = f"transform_{DAG_ID}_releases"
    TASK_ID_UPLOAD = f"upload_{DAG_ID}_releases"
    TASK_ID_BQ_LOAD = f"bq_load_{DAG_ID}_releases"
    TASK_ID_CLEANUP = f"cleanup_{DAG_ID}_releases"

    @staticmethod
    def setup_requirements(**kwargs):
        invalid_list = []
        config_dict = {}

        if is_vendor_google():
            default_config = None
        else:
            default_config = ObservatoryConfig.CONTAINER_DEFAULT_PATH
        config_path = Variable.get('CONFIG_PATH', default_var=default_config)

        if config_path is None:
            print("'CONFIG_FILE' airflow variable not set, please set in UI")

        config_valid, config_validator, config = ObservatoryConfig.load(config_path)
        if not config_valid:
            invalid_list.append(f'Config file not valid: {config_validator}')

        if invalid_list:
            for invalid_reason in invalid_list:
                print("-", invalid_reason, "\n\n")
                raise AirflowException
        else:
            config_dict['environment'] = config.environment.value
            config_dict['bucket'] = config.bucket_name
            config_dict['project_id'] = config.project_id
            # Push messages
            ti: TaskInstance = kwargs['ti']
            ti.xcom_push(FundrefTelescope.XCOM_CONFIG_NAME, config_dict)

    @staticmethod
    def list_releases_last_month(**kwargs):
        # Pull messages
        msgs_in, environment, bucket, project_id = xcom_pull_messages(kwargs['ti'])

        if environment == 'dev':
            msgs_out = [{'url': FundrefTelescope.TELESCOPE_DEBUG_URL, 'date': '3000-01-01'}]
            # Push messages
            ti: TaskInstance = kwargs['ti']
            ti.xcom_push(FundrefTelescope.XCOM_MESSAGES_NAME, msgs_out)
            return FundrefTelescope.TASK_ID_DOWNLOAD if msgs_out else FundrefTelescope.TASK_ID_STOP

        execution_date = kwargs['execution_date']
        next_execution_date = kwargs['next_execution_date']
        releases_dict = list_releases(FundrefTelescope.TELESCOPE_URL)
        logging.info(f'All releases:\n{releases_dict}\n')

        bq_hook = BigQueryHook()
        # Select the releases that were published on or after the execution_date and before the next_execution_date
        msgs_out = []
        logging.info('All releases between this and next execution date:')
        for release_url in releases_dict:
            release_date = releases_dict[release_url]
            published_date: Pendulum = pendulum.parse(release_date)

            if execution_date <= published_date < next_execution_date:
                logging.info(f'{release_url, release_date}')
                table_exists = bq_hook.table_exists(
                    project_id=project_id,
                    dataset_id=FundrefTelescope.DATASET_ID,
                    table_id=table_name(release_date)
                )
                if table_exists:
                    logging.info(f'Table exists: {project_id}.{FundrefTelescope.DATASET_ID}.{table_name(release_date)}')
                else:
                    logging.info("Table doesn't exist yet, processing this workflow")
                    msgs_out.append({'url': release_url, 'date': release_date})

        # Push messages
        ti: TaskInstance = kwargs['ti']
        ti.xcom_push(FundrefTelescope.XCOM_MESSAGES_NAME, msgs_out)
        return FundrefTelescope.TASK_ID_DOWNLOAD if msgs_out else FundrefTelescope.TASK_ID_STOP

    @staticmethod
    def download_releases_local(**kwargs):
        # Pull messages
        msgs_in, environment, bucket, project_id = xcom_pull_messages(kwargs['ti'])

        for msg_in in msgs_in:
            if environment == 'dev':
                shutil.copy(FundrefTelescope.DEBUG_FILE_PATH, filepath_download(msg_in['date']))

            else:
                download_release(msg_in['url'], msg_in['date'])

    @staticmethod
    def decompress_release(**kwargs):
        # Pull messages
        msgs_in, environment, bucket, project_id = xcom_pull_messages(kwargs['ti'])

        for msg_in in msgs_in:
            cmd = f"registry_path=$(tar -ztf {filepath_download(msg_in['date'])} | grep -m1 '/registry.rdf'); " \
                  f"tar -xOzf {filepath_download(msg_in['date'])} $registry_path > {filepath_extract(msg_in['date'])}"

            p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                 executable='/bin/bash')
            stdout, stderr = p.communicate()
            if stdout:
                logging.info(stdout)
            if stderr:
                raise AirflowException(f"bash command failed for {msg_in}: {stderr}")

    @staticmethod
    def geonames_dump(**kwargs):
        msgs_in, environment, bucket, project_id = xcom_pull_messages(kwargs['ti'])

        gcs_hook = GoogleCloudStorageHook()
        geonames_exists = gcs_hook.exists(
                            bucket=bucket,
                            object=FundrefTelescope.GEONAMES_FILE_NAME
                         )

        if geonames_exists:
            gcs_hook.download(
                bucket=bucket,
                object=FundrefTelescope.GEONAMES_FILE_NAME,
                filename=filepath_geonames()
            )

        else:
            # download geonames
            download_geonames_dump()

            # upload to gcs for future use
            gcs_hook.upload(
                bucket=bucket,
                object=FundrefTelescope.GEONAMES_FILE_NAME,
                filename=filepath_geonames()
            )

    @staticmethod
    def transform_release(**kwargs):
        # Pull messages
        msgs_in, environment, bucket, project_id = xcom_pull_messages(kwargs['ti'])

        for msg_in in msgs_in:
            funders_list = parse_fundref_registry_rdf(filepath_extract(msg_in['date']), filepath_geonames())
            write_list_to_jsonl(funders_list, filepath_transform(msg_in['date']))

    @staticmethod
    def upload_release_to_gcs(**kwargs):
        # Pull messages
        msgs_in, environment, bucket, project_id = xcom_pull_messages(kwargs['ti'])

        gcs_hook = GoogleCloudStorageHook()

        for msg_in in msgs_in:
            gcs_hook.upload(
                bucket=bucket,
                object=os.path.basename(filepath_transform(msg_in['date'])),
                filename=filepath_transform(msg_in['date'])
            )

    @staticmethod
    def load_release_to_bq(**kwargs):
        # Add a project id to the bigquery connection, prevents error 'ValueError: INTERNAL: No default project is
        # specified'
        msgs_in, environment, bucket, project_id = xcom_pull_messages(kwargs['ti'])

        if environment == 'dev':
            session = settings.Session()
            bq_conn = Connection(
                conn_id='bigquery_custom',
                conn_type='google_cloud_platform',
            )

            conn_extra_json = json.dumps({'extra__google_cloud_platform__project': project_id})
            bq_conn.set_extra(conn_extra_json)

            session.query(Connection).filter(Connection.conn_id == bq_conn.conn_id).delete()
            session.add(bq_conn)
            session.commit()

            bq_hook = BigQueryHook(bigquery_conn_id='bigquery_custom')
        else:
            bq_hook = BigQueryHook()

        with open(FundrefTelescope.SCHEMA_FILE_PATH, 'r') as json_file:
            schema_fields = json.loads(json_file.read())

        conn = bq_hook.get_conn()
        cursor = conn.cursor()

        # Pull messages
        ti: TaskInstance = kwargs['ti']
        msgs_in = ti.xcom_pull(key=FundrefTelescope.XCOM_MESSAGES_NAME, task_ids=FundrefTelescope.TASK_ID_LIST, include_prior_dates=False)
        for msg_in in msgs_in:
            try:
                cursor.create_empty_dataset(
                    dataset_id=FundrefTelescope.DATASET_ID,
                    project_id=project_id
                )
            except AirflowException as e:
                print(f"Dataset already exists, continuing to create table. See traceback:\n{e}")

            cursor.run_load(
                destination_project_dataset_table=f"{FundrefTelescope.DATASET_ID}.{table_name(msg_in['date'])}",
                source_uris=f"gs://{bucket}/{os.path.basename(filepath_transform(msg_in['date']))}",
                schema_fields=schema_fields,
                autodetect=False,
                source_format='NEWLINE_DELIMITED_JSON',
            )

    @staticmethod
    def cleanup_releases(**kwargs):
        # Pull messages
        msgs_in, environment, bucket, project_id = xcom_pull_messages(kwargs['ti'])

        try:
            pathlib.Path(filepath_geonames()).unlink()
        except FileNotFoundError as e:
            logging.warning(f"No such file or directory {filepath_geonames()}: {e}")

        for msg_in in msgs_in:
            try:
                pathlib.Path(filepath_download(msg_in['date'])).unlink()
            except FileNotFoundError as e:
                logging.warning(f"No such file or directory {filepath_download(msg_in['date'])}: {e}")

            try:
                pathlib.Path(filepath_extract(msg_in['date'])).unlink()
            except FileNotFoundError as e:
                logging.warning(f"No such file or directory {filepath_extract(msg_in['date'])}: {e}")

            try:
                pathlib.Path(filepath_transform(msg_in['date'])).unlink()
            except FileNotFoundError as e:
                logging.warning(f"No such file or directory {filepath_transform(msg_in['date'])}: {e}")