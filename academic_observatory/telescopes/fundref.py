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
import json
import logging
import os
import pathlib
import requests
import shutil
import subprocess
import xml.etree.ElementTree as ET
from typing import Tuple

import pendulum
from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.models.taskinstance import TaskInstance
from google.api_core.exceptions import NotFound
from google.cloud import storage
from google.cloud.bigquery import SourceFormat
from pendulum import Pendulum

from academic_observatory.utils.config_utils import (
    is_composer,
    find_schema,
    ObservatoryConfig,
    SubFolder,
    schema_path,
    telescope_path,
)
from academic_observatory.utils.data_utils import get_file
from academic_observatory.utils.gc_utils import (
    bigquery_partitioned_table_id,
    create_bigquery_dataset,
    download_blob_from_cloud_storage,
    load_bigquery_table,
    upload_file_to_cloud_storage
)
from academic_observatory.utils.proc_utils import wait_for_process
from academic_observatory.utils.url_utils import retry_session
from tests.academic_observatory.config import test_fixtures_path


def xcom_pull_info(ti: TaskInstance) -> Tuple[list, str, str, str]:
    """
    Pulls xcom messages, releases_list and config_dict.
    Parses retrieved config_dict and returns those values next to releases_list.

    :param ti:
    :return: releases_list, environment, bucket, project_id
    """
    releases_list = ti.xcom_pull(key=FundrefTelescope.XCOM_MESSAGES_NAME, task_ids=FundrefTelescope.TASK_ID_LIST,
                                 include_prior_dates=False)
    config_dict = ti.xcom_pull(key=FundrefTelescope.XCOM_CONFIG_NAME, task_ids=FundrefTelescope.TASK_ID_SETUP,
                               include_prior_dates=False)
    environment = config_dict['environment']
    bucket = config_dict['bucket']
    project_id = config_dict['project_id']
    return releases_list, environment, bucket, project_id


def list_releases(telescope_url):
    """
    List all available fundref releases

    :param telescope_url:
    :return: list of dictionaries that contain 'url' and 'release_date' in format 'YYYY-MM-DD'
    """
    releases_list = []

    response = retry_session().get(telescope_url).text

    if response:
        json_response = json.loads(response)

        for release in json_response:
            release_dict = {}
            for source in release['assets']['sources']:
                if source['format'] == 'tar.gz':
                    release_date = pendulum.parse(release['released_at']).to_date_string()
                    release_dict['date'] = release_date
                    release_dict['url'] = source['url']
                    releases_list.append(release_dict)

    return releases_list


def download_release(fundref_release: 'FundrefRelease') -> str:
    """
    Downloads release from url.

    :param fundref_release: Instance of FundrefRelease class
    """
    file_path = fundref_release.get_filepath_download()

    # Download
    logging.info(f"Downloading file: {file_path}, url: {fundref_release.url}")
    # add browser agent to prevent 403/forbidden error.
    header = {'User-Agent': 'Mozilla/5.0'}
    with requests.get(fundref_release.url, headers=header, stream=True) as response:
        with open(file_path, 'wb') as file:
            shutil.copyfileobj(response.raw, file)

    return file_path


def decompress_release(fundref_release: 'FundrefRelease') -> str:
    """
    Decompresses release.

    :param fundref_release: Instance of FundrefRelease class
    """
    logging.info(f"Extracting file: {fundref_release.filepath_download}")
    # tar file contains both README.md and registry.rdf, use tar -ztf to get path of 'registry.rdf'
    # Use this path to extract only registry.rdf to a new file.
    cmd = f"registry_path=$(tar -ztf {fundref_release.filepath_download} | grep -m1 '/registry.rdf'); " \
          f"tar -xOzf {fundref_release.filepath_download} $registry_path > {fundref_release.filepath_extract}"
    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                         executable='/bin/bash')
    stdout, stderr = wait_for_process(p)
    if stdout:
        logging.info(stdout)
    if stderr:
        raise AirflowException(f"bash command failed for {fundref_release.url}: {stderr}")
    logging.info(f"File extracted to: {fundref_release.filepath_extract}")

    return fundref_release.filepath_extract


def transform_release(fundref_release: 'FundrefRelease') -> str:
    """
    Transform release by parsing the raw rdf file, transforming it into a json file and replacing geoname associated
    ids with their geoname name.

    :param fundref_release: Instance of FundrefRelease class
    """
    funders_list = parse_fundref_registry_rdf(fundref_release.filepath_extract)
    with open(fundref_release.filepath_transform, 'w') as jsonl_out:
        jsonl_out.write('\n'.join(json.dumps(obj) for obj in funders_list))
    logging.info(f'Success transforming release: {fundref_release.url}')

    return fundref_release.filepath_transform


class FundrefRelease:
    def __init__(self, url, date):
        self.url = url
        self.date = date
        self.filepath_download = self.get_filepath_download()
        self.filepath_extract = self.get_filepath_extract()
        self.filepath_transform = self.get_filepath_transform()

    def get_filepath_download(self) -> str:
        """
        Gives path to downloaded release.

        :return: absolute file path
        """
        compressed_file_name = f"{FundrefTelescope.DAG_ID}_{self.date}.tar.gz".replace('-', '_')
        download_dir = telescope_path(FundrefTelescope.DAG_ID, SubFolder.downloaded)
        path = os.path.join(download_dir, compressed_file_name)

        return path

    def get_filepath_extract(self) -> str:
        """
        Gives path to extracted and decompressed release.

        :return: absolute file path
        """
        decompressed_file_name = f"{FundrefTelescope.DAG_ID}_{self.date}.rdf".replace('-', '_')
        extract_dir = telescope_path(FundrefTelescope.DAG_ID, SubFolder.extracted)
        path = os.path.join(extract_dir, decompressed_file_name)

        return path

    def get_filepath_transform(self) -> str:
        """
        Gives path to transformed release.

        :return: absolute file path
        """
        decompressed_file_name = f"{FundrefTelescope.DAG_ID}_{self.date}.rdf".replace('-', '_')
        transform_dir = telescope_path(FundrefTelescope.DAG_ID, SubFolder.transformed)
        path = os.path.join(transform_dir, decompressed_file_name)

        return path


def get_filepath_geonames():
    file_name = FundrefTelescope.GEONAMES_FILE_NAME
    transform_dir = telescope_path(FundrefTelescope.DAG_ID, SubFolder.transformed)
    path = os.path.join(transform_dir, file_name)

    return path


def download_geonames_dump():
    filename = get_filepath_geonames().strip('.txt')
    filedir = os.path.dirname(filename)
    logging.info(f"Downloading file: {filename}, url: {FundrefTelescope.GEONAMES_URL}")
    # extract zip file named 'filename' which contains 'allCountries.txt'
    file_path, updated = get_file(fname=filename, origin=FundrefTelescope.GEONAMES_URL, cache_subdir=filedir,
             extract=True, archive_format='zip')

    if file_path:
        logging.info(f'Success downloading release: {file_path}')
    else:
        logging.error(f"Error downloading release: {file_path}")
        exit(os.EX_DATAERR)


def geonames_dump():
    geonames_dict = {}

    geonames_dump_path = get_filepath_geonames()
    with open(geonames_dump_path, 'r') as file:
        for line in file:
            id = line.split('\t')[0]
            name = line.split('\t')[1]
            country_code = line.split('\t')[8]
            geonames_dict[id] = (name, country_code)
    return geonames_dict


def get_geoname_data(nested, geoname_dict):
    geonameid = nested.attrib['{http://www.w3.org/1999/02/22-rdf-syntax-ns#}resource'].split('sws.geonames.org')[
        -1].strip('/')
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


def parse_fundref_registry_rdf(file_name):
    """ Helper function to parse a fundref registry rdf file and to return a python list containing each funder.
    :param file_name: the filename of the registry.rdf file to be parsed.
    :return: A python list containing all the funders parsed from the input rdf
    """
    funders = []
    funders_by_key = {}

    tree = ET.parse(file_name)
    root = tree.getroot()

    geoname_dict = geonames_dump()

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


class FundrefTelescope:
    DAG_ID = 'fundref'
    DESCRIPTION = 'Fundref'
    DATASET_ID = DAG_ID
    XCOM_MESSAGES_NAME = "messages"
    XCOM_CONFIG_NAME = "config"
    TELESCOPE_URL = 'https://gitlab.com/api/v4/projects/crossref%2Fopen_funder_registry/releases'
    TELESCOPE_DEBUG_URL = 'debug_fundref_url'
    DEBUG_FILE_PATH = os.path.join(test_fixtures_path(), 'telescopes', 'fundref.tar.gz')

    GEONAMES_FILE_NAME = 'allCountries.txt'
    GEONAMES_URL = 'https://download.geonames.org/export/dump/allCountries.zip'
    DEBUG_GEONAMES_FILE_PATH = os.path.join(test_fixtures_path(), 'telescopes', 'allCountries.txt')

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
    def check_setup_requirements(**kwargs):
        """
        Depending on whether run from inside or outside a composer environment:
        If run from outside, checks if 'CONFIG_PATH' airflow variable is available and points to a valid config file.
        If run from inside, check if airflow variables for 'env', 'bucket' and 'project_id' are set.

        The corresponding values will be stored in a dict and pushed with xcom, so they can be accessed in consequent
        tasks.

        kwargs is required to access task instance
        :param kwargs: NA
        """
        invalid_list = []
        config_dict = {}

        if is_composer():
            environment = Variable.get('ENV', default_var=None)
            bucket = Variable.get('BUCKET', default_var=None)
            project_id = Variable.get('PROJECT_ID', default_var=None)

            for var in [(environment, 'ENV'), (bucket, 'BUCKET'), (project_id, 'PROJECT_ID')]:
                if var[0] is None:
                    invalid_list.append(f"Airflow variable '{var[1]}' not set with terraform.")

        else:
            default_config = ObservatoryConfig.CONTAINER_DEFAULT_PATH
            config_path = Variable.get('CONFIG_PATH', default_var=default_config)

            config = ObservatoryConfig.load(config_path)
            if config is not None:
                config_is_valid = config.is_valid
                if not config_is_valid:
                    invalid_list.append(f'Config file not valid: {config_is_valid}')
            if not config:
                invalid_list.append(f'Config file does not exist')

            environment = config.environment.value
            bucket = config.bucket_name
            project_id = config.project_id

        if invalid_list:
            for invalid_reason in invalid_list:
                logging.warning("-" + invalid_reason + "\n\n")
            raise AirflowException
        else:
            config_dict['environment'] = environment
            config_dict['bucket'] = bucket
            config_dict['project_id'] = project_id
            # Push messages
            ti: TaskInstance = kwargs['ti']
            ti.xcom_push(FundrefTelescope.XCOM_CONFIG_NAME, config_dict)

    @staticmethod
    def list_releases_last_month(**kwargs):
        """
        Based on a list of all releases, checks which ones were released between this and the next execution date of the
        DAG.
        If the release falls within the time period mentioned above, checks if a bigquery table doesn't exist yet for
        the release.
        A list of releases that passed both checks is passed to the next tasks. If the list is empty the workflow will
        stop.

        kwargs is required to access task instance
        """
        # Pull messages
        releases_list, environment, bucket, project_id = xcom_pull_info(kwargs['ti'])

        if environment == 'dev':
            releases_list_out = [{'url': FundrefTelescope.TELESCOPE_DEBUG_URL, 'date': '3000-01-01'}]
            # Push messages
            ti: TaskInstance = kwargs['ti']
            ti.xcom_push(FundrefTelescope.XCOM_MESSAGES_NAME, releases_list_out)
            return FundrefTelescope.TASK_ID_DOWNLOAD if releases_list_out else FundrefTelescope.TASK_ID_STOP

        execution_date = kwargs['execution_date']
        next_execution_date = kwargs['next_execution_date']
        releases_list = list_releases(FundrefTelescope.TELESCOPE_URL)
        logging.info(f'All releases:\n{releases_list}\n')

        bq_hook = BigQueryHook()
        # Select the releases that were published on or after the execution_date and before the next_execution_date
        releases_list_out = []
        logging.info('Releases between current and next execution date:')
        for release in releases_list:
            fundref_release = FundrefRelease(release['url'], release['date'])
            released_date: Pendulum = pendulum.parse(fundref_release.date)
            table_id = bigquery_partitioned_table_id(FundrefTelescope.DAG_ID, released_date)

            if execution_date <= released_date < next_execution_date:
                logging.info(fundref_release.url)
                table_exists = bq_hook.table_exists(
                    project_id=project_id,
                    dataset_id=FundrefTelescope.DATASET_ID,
                    table_id=table_id
                )
                logging.info('Checking if bigquery table already exists:')
                if table_exists:
                    logging.info(
                        f'- Table exists for {fundref_release.url}: {project_id}.{FundrefTelescope.DATASET_ID}.{table_id}')
                else:
                    logging.info(f"- Table doesn't exist yet, processing {fundref_release.url} in this workflow")
                    releases_list_out.append(release)

        # Push messages
        ti: TaskInstance = kwargs['ti']
        ti.xcom_push(FundrefTelescope.XCOM_MESSAGES_NAME, releases_list_out)
        return FundrefTelescope.TASK_ID_DOWNLOAD if releases_list_out else FundrefTelescope.TASK_ID_STOP

    @staticmethod
    def download(**kwargs):
        """
        Download release to file.
        If dev environment, copy debug file from this repository to the right location. Else download from url.

        kwargs is required to access task instance
        """
        # Pull messages
        releases_list, environment, bucket, project_id = xcom_pull_info(kwargs['ti'])

        for release in releases_list:
            fundref_release = FundrefRelease(release['url'], release['date'])
            if environment == 'dev':
                shutil.copy(FundrefTelescope.DEBUG_FILE_PATH, fundref_release.filepath_download)

            else:
                download_release(fundref_release)

    @staticmethod
    def decompress(**kwargs):
        """
        Extract release to new file.

        kwargs is required to access task instance
        """
        # Pull messages
        releases_list, environment, bucket, project_id = xcom_pull_info(kwargs['ti'])

        for release in releases_list:
            fundref_release = FundrefRelease(release['url'], release['date'])
            decompress_release(fundref_release)

    @staticmethod
    def geonames_dump(**kwargs):
        msgs_in, environment, bucket, project_id = xcom_pull_info(kwargs['ti'])

        # use blob_dir_name for fundref release later
        blob_dir_name = 'telescopes/fundref/transformed'
        blob_name = os.path.join(blob_dir_name, FundrefTelescope.GEONAMES_FILE_NAME)
        file_path = get_filepath_geonames()

        if environment == 'dev':
            shutil.copy(FundrefTelescope.DEBUG_GEONAMES_FILE_PATH, file_path)
        else:
            logging.info(f"Trying to download geonames_dump from bucket. bucket: {bucket}, blob_name: {blob_name}, "
                         f"file_path:{file_path}")
            try:
                success = download_blob_from_cloud_storage(bucket, blob_name, file_path)
            # except AirflowException:
            except NotFound:
                success = False

            if success:
                logging.info("Successfully downloaded geonames_dump file")
            else:
                logging.info(f"Could not download geonames_dump file from bucket. Downloading from "
                             f"{FundrefTelescope.GEONAMES_URL} instead")
                # download geonames
                download_geonames_dump()
                # upload to bucket for later usage
                upload_file_to_cloud_storage(bucket, blob_name, file_path=file_path)

        # Push messages
        ti: TaskInstance = kwargs['ti']
        ti.xcom_push('blob_dir_name', blob_dir_name)

    @staticmethod
    def transform(**kwargs):
        """
        Transform release by parsing the raw rdf file, transforming it into a json file and replacing geoname associated
        ids with their geoname name.

        kwargs is required to access task instance
        """
        # Pull messages
        releases_list, environment, bucket, project_id = xcom_pull_info(kwargs['ti'])

        for release in releases_list:
            fundref_release = FundrefRelease(release['url'], release['date'])
            transform_release(fundref_release)

    @staticmethod
    def upload_to_gcs(**kwargs):
        """
        Upload transformed release to a google cloud storage bucket.

        kwargs is required to access task instance
        """
        # Pull messages
        releases_list, environment, bucket, project_id = xcom_pull_info(kwargs['ti'])

        ti: TaskInstance = kwargs['ti']
        blob_dir_name = ti.xcom_pull(key='blob_dir_name', task_ids=FundrefTelescope.TASK_ID_GEONAMES,
                                     include_prior_dates=False)

        for release in releases_list:
            fundref_release = FundrefRelease(release['url'], release['date'])
            blob_name = os.path.join(blob_dir_name, os.path.basename(fundref_release.filepath_transform))
            upload_file_to_cloud_storage(bucket, blob_name, file_path=fundref_release.filepath_transform)

    @staticmethod
    def load_to_bq(**kwargs):
        """
        Upload transformed release to a bigquery table.

        kwargs is required to access task instance
        """
        releases_list, environment, bucket, project_id = xcom_pull_info(kwargs['ti'])

        # Get bucket location
        storage_client = storage.Client()
        bucket_object = storage_client.get_bucket(bucket)
        location = bucket_object.location

        # Create dataset
        dataset_id = FundrefTelescope.DAG_ID
        create_bigquery_dataset(project_id, dataset_id, location, FundrefTelescope.DESCRIPTION)

        # Pull messages
        ti: TaskInstance = kwargs['ti']
        releases_list = ti.xcom_pull(key=FundrefTelescope.XCOM_MESSAGES_NAME, task_ids=FundrefTelescope.TASK_ID_LIST,
                                     include_prior_dates=False)
        blob_dir_name = ti.xcom_pull(key='blob_dir_name', task_ids=FundrefTelescope.TASK_ID_GEONAMES,
                                     include_prior_dates=False)
        for release in releases_list:
            fundref_release = FundrefRelease(release['url'], release['date'])
            # get release_date
            released_date: Pendulum = pendulum.parse(fundref_release.date)
            table_id = bigquery_partitioned_table_id(FundrefTelescope.DAG_ID, released_date)

            # Select schema file based on release date
            analysis_schema_path = schema_path('telescopes')
            schema_file_path = find_schema(analysis_schema_path, FundrefTelescope.DAG_ID, released_date)
            if schema_file_path is None:
                logging.error(f'No schema found with search parameters: analysis_schema_path={analysis_schema_path}, '
                              f'table_name={FundrefTelescope.DAG_ID}, release_date={released_date}')
                exit(os.EX_CONFIG)

            # Load BigQuery table
            blob_name = os.path.join(blob_dir_name, os.path.basename(fundref_release.filepath_transform))
            uri = f"gs://{bucket}/{blob_name}"
            logging.info(f"URI: {uri}")
            load_bigquery_table(uri, dataset_id, location, table_id, schema_file_path,
                                SourceFormat.NEWLINE_DELIMITED_JSON)

    @staticmethod
    def cleanup_releases(**kwargs):
        """
        Delete files of downloaded, extracted and transformed release.

        kwargs is required to access task instance
        """
        # Pull messages
        releases_list, environment, bucket, project_id = xcom_pull_info(kwargs['ti'])

        for release in releases_list:
            fundref_release = FundrefRelease(release['url'], release['date'])
            try:
                pathlib.Path(fundref_release.filepath_download).unlink()
            except FileNotFoundError as e:
                logging.warning(f"No such file or directory {fundref_release.filepath_download}: {e}")

            try:
                pathlib.Path(fundref_release.filepath_extract).unlink()
            except FileNotFoundError as e:
                logging.warning(f"No such file or directory {fundref_release.filepath_extract}: {e}")

            try:
                pathlib.Path(fundref_release.filepath_transform).unlink()
            except FileNotFoundError as e:
                logging.warning(f"No such file or directory {fundref_release.filepath_transform}: {e}")
