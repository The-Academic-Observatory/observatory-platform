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

import json
import logging
import os
import pathlib
import shutil
import subprocess
import xml.etree.ElementTree as ET
from typing import Dict, List, Tuple, Union

import pendulum
import requests
from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.models.taskinstance import TaskInstance
from google.cloud import storage
from google.cloud.bigquery import SourceFormat
from pendulum import Pendulum

from academic_observatory.utils.config_utils import (
    find_schema,
    ObservatoryConfig,
    SubFolder,
    schema_path,
    telescope_path,
)
from academic_observatory.utils.gc_utils import (
    bigquery_partitioned_table_id,
    create_bigquery_dataset,
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


def list_releases(telescope_url: str) -> List:
    """
    List all available fundref releases

    :param telescope_url:
    :return: list of dictionaries that contain 'url' and 'release_date' in format 'YYYY-MM-DD'
    """
    releases_list = []
    response = retry_session().get(telescope_url+'?per_page=100')

    if response:
        no_pages = int(response.headers['X-Total-Pages'])
        current_page = int(response.headers['X-Page'])
        while no_pages >= current_page:
            response = retry_session().get(telescope_url+f'?per_page=100&page={current_page}')
            json_response = json.loads(response.text)
            for release in json_response:
                release_dict = {}
                version = float(release['tag_name'].strip('v'))
                for source in release['assets']['sources']:
                    if source['format'] == 'tar.gz':
                        if version == 0.1:
                            release_date = pendulum.parse('2014-03-01').to_date_string()
                        elif version < 1.0:
                            date_string = release['description'].split('\n')[0]
                            release_date = pendulum.parse('01' + date_string).to_date_string()
                        else:
                            release_date = pendulum.parse(release['released_at']).to_date_string()
                        release_dict['date'] = release_date
                        release_dict['url'] = source['url']
                        releases_list.append(release_dict)
            current_page += 1
    else:
        logging.error(f"Error retrieving response from url {telescope_url}")
        exit(os.EX_DATAERR)

    return releases_list


def download_release(fundref_release: 'FundrefRelease') -> str:
    """
    Downloads release from url.

    :param fundref_release: Instance of FundrefRelease class
    """
    file_path = fundref_release.filepath_download

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
    funders, funders_by_key = parse_fundref_registry_rdf(fundref_release.filepath_extract)
    funders = add_funders_relationships(funders, funders_by_key)
    with open(fundref_release.filepath_transform, 'w') as jsonl_out:
        jsonl_out.write('\n'.join(json.dumps(obj) for obj in funders))
    logging.info(f'Success transforming release: {fundref_release.url}')

    return fundref_release.filepath_transform


class FundrefRelease:
    """ Used to store info on a given fundref release """
    def __init__(self, url, date):
        self.url = url
        self.date = date
        self.filepath_download = self.get_filepath(SubFolder.downloaded)
        self.filepath_extract = self.get_filepath(SubFolder.extracted)
        self.filepath_transform = self.get_filepath(SubFolder.transformed)

    def get_filepath(self, sub_folder: SubFolder) -> str:
        """
        Gets complete path of file for download/extract/transform directory

        :param sub_folder: name of subfolder
        :return:
        """
        if sub_folder == SubFolder.downloaded:
            file_name = f"{FundrefTelescope.DAG_ID}_{self.date}.tar.gz".replace('-', '_')
        else:
            file_name = f"{FundrefTelescope.DAG_ID}_{self.date}.rdf".replace('-', '_')

        file_dir = telescope_path(FundrefTelescope.DAG_ID, sub_folder)
        path = os.path.join(file_dir, file_name)

        return path

    def get_blob_name(self, sub_folder: SubFolder) -> str:
        """
        Gives blob name that is used to determine path inside storage bucket

        :param sub_folder: name of subfolder
        :return: blob name
        """
        file_name = os.path.basename(self.get_filepath(sub_folder))
        blob_name = os.path.join(f'telescopes/{FundrefTelescope.DAG_ID}/{sub_folder.value}', file_name)

        return blob_name


def new_funder_template():
    """
    Helper Function for creating a new Funder.
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


def parse_fundref_registry_rdf(registry_file_path: str) -> Tuple[List, Dict]:
    """
    Helper function to parse a fundref registry rdf file and to return a python list containing each funder.

    :param registry_file_path: the filename of the registry.rdf file to be parsed.
    :return: funders list containing all the funders parsed from the input rdf and dictionary of funders with their
    id as key.
    """
    funders = []
    funders_by_key = {}

    tree = ET.parse(registry_file_path)
    root = tree.getroot()

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
                    funder['country'] = nested.text
                elif nested.tag == '{http://data.crossref.org/fundingdata/xml/schema/grant/grant-1.2/}state':
                    funder['state'] = nested.text
                elif nested.tag == '{http://schema.org/}address':
                    funder['country_code'] = nested[0][0].text
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
                    print(f"Unrecognized tag for element: {nested}")

            funders.append(funder)

    return funders, funders_by_key


def add_funders_relationships(funders: List, funders_by_key: Dict) -> List:
    """
    Adds any children/parent relationships to funder instances in the funders list.

    :param funders: List of funders
    :param funders_by_key: Dictionary of funders with their id as key.
    :return: funders with added relationships.
    """
    for funder in funders:
        children, returned_depth = recursive_funders(funders_by_key, funder, 0, 'narrower')
        funder["children"] = children
        funder['bottom'] = len(children) > 0

        parent, returned_depth = recursive_funders(funders_by_key, funder, 0, 'broader')
        funder["parents"] = parent
        funder['top'] = len(parent) > 0

    return funders


def recursive_funders(funders_by_key: Dict, funder: Dict, depth: int, direction: str) -> Tuple[List, int]:
    """
    Recursively goes through a funder/sub_funder dict. The funder properties can be looked up with the funders_by_key
    dictionary that stores the properties per funder id. Any children/parents for the funder are already given in the
    xml element with the 'narrower' and 'broader' tags. For each funder in the list, it will recursively add any
    children/parents for those funders in 'narrower'/'broader' and their funder properties.

    :param funders_by_key: dictionary with id as key and funders object as value
    :param funder: dictionary of a given funder containing 'narrower' and 'broader' info
    :param depth: keeping track of nested depth
    :param direction: either 'narrower' or 'broader' to get 'children' or 'parents'
    :return:
    """
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
    """ A container for holding the constants and static functions for the Fundref telescope. """
    DAG_ID = 'fundref'
    DESCRIPTION = 'Fundref'
    DATASET_ID = DAG_ID
    XCOM_MESSAGES_NAME = "messages"
    XCOM_CONFIG_NAME = "config"
    TELESCOPE_URL = 'https://gitlab.com/api/v4/projects/crossref%2Fopen_funder_registry/releases'
    TELESCOPE_DEBUG_URL = 'debug_fundref_url'
    DEBUG_FILE_PATH = os.path.join(test_fixtures_path(), 'telescopes', 'fundref.tar.gz')

    TASK_ID_SETUP = "check_setup_requirements"
    TASK_ID_LIST = f"list_releases"
    TASK_ID_STOP = f"stop_workflow"
    TASK_ID_DOWNLOAD = f"download_releases"
    TASK_ID_DECOMPRESS = f"decompress_releases"
    TASK_ID_TRANSFORM = f"transform_releases"
    TASK_ID_UPLOAD = f"upload_releases"
    TASK_ID_BQ_LOAD = f"bq_load_releases"
    TASK_ID_CLEANUP = f"cleanup_releases"

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
        environment = None
        bucket = None
        project_id = None

        default_config = ObservatoryConfig.CONTAINER_DEFAULT_PATH
        config_path = Variable.get('CONFIG_PATH', default_var=default_config)

        config = ObservatoryConfig.load(config_path)
        if config is not None:
            config_is_valid = config.is_valid
            if config_is_valid:
                environment = config.environment.value
                bucket = config.bucket_name
                project_id = config.project_id
            else:
                invalid_list.append(f'Config file not valid: {config_is_valid}')
        if not config:
            invalid_list.append(f'Config file does not exist')

        if invalid_list:
            for invalid_reason in invalid_list:
                logging.warning("-" + invalid_reason + "\n\n")
            raise AirflowException
        else:
            config_dict['environment'] = environment
            config_dict['bucket'] = bucket
            config_dict['project_id'] = project_id
            logging.info(f'environment: {environment}, bucket: {bucket}, project_id: {project_id}')
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
        execution_date = kwargs['execution_date']
        next_execution_date = kwargs['next_execution_date']

        if environment == 'dev':
            releases_list_out = [{'url': FundrefTelescope.TELESCOPE_DEBUG_URL, 'date': kwargs['ds']}]
            # Push messages
            ti: TaskInstance = kwargs['ti']
            ti.xcom_push(FundrefTelescope.XCOM_MESSAGES_NAME, releases_list_out)
            return FundrefTelescope.TASK_ID_DOWNLOAD if releases_list_out else FundrefTelescope.TASK_ID_STOP

        releases_list = list_releases(FundrefTelescope.TELESCOPE_URL)
        logging.info('All releases:\n')
        print(*releases_list, sep='\n')

        bq_hook = BigQueryHook()
        # Select the releases that were published on or after the execution_date and before the next_execution_date
        releases_list_out = []
        logging.info(f'Releases between current ({execution_date}) and next ({next_execution_date}) execution date:')
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
                    logging.info(f'- Table exists for {fundref_release.url}: '
                                 f'{project_id}.{FundrefTelescope.DATASET_ID}.{table_id}')
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

        for release in releases_list:
            fundref_release = FundrefRelease(release['url'], release['date'])

            upload_file_to_cloud_storage(bucket, fundref_release.get_blob_name(SubFolder.transformed),
                                         file_path=fundref_release.filepath_transform)

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
            uri = f"gs://{bucket}/{fundref_release.get_blob_name(SubFolder.transformed)}"
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
