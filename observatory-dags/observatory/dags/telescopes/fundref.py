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

# Author: Aniek Roelofs, Richard Hosking, James Diprose

from __future__ import annotations

import gzip
import io
import json
import logging
import os
import pathlib
import random
import shutil
import subprocess
import xml.etree.ElementTree as ET
from typing import Dict, List, Tuple

import jsonlines
import pendulum
import requests
from airflow.exceptions import AirflowException
from airflow.models.taskinstance import TaskInstance
from google.cloud.bigquery import SourceFormat
from pendulum import Pendulum

from observatory.dags.config import schema_path
from observatory.platform.utils.airflow_utils import AirflowVariable as Variable
from observatory.platform.utils.config_utils import AirflowVars, SubFolder, find_schema, telescope_path, check_variables
from observatory.platform.utils.gc_utils import (bigquery_partitioned_table_id,
                                                 bigquery_table_exists,
                                                 create_bigquery_dataset,
                                                 load_bigquery_table,
                                                 upload_file_to_cloud_storage)
from observatory.platform.utils.proc_utils import wait_for_process
from observatory.platform.utils.url_utils import retry_session


def list_releases(start_date: Pendulum, end_date: Pendulum) -> List[FundrefRelease]:
    """ List all available fundref releases

    :param start_date:
    :param end_date:
    :return: list of FundrefRelease instances.
    """

    # A selection of headers to prevent 403/forbidden error.

    headers_list = [{
        'authority': 'gitlab.com',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/84.0.4147.89 Safari/537.36',
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,'
                  '*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'sec-fetch-site': 'none',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-dest': 'document',
        'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8'
    },

        {
            'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:78.0) Gecko/20100101 Firefox/78.0',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        }]

    releases_list = []
    headers = random.choice(headers_list)
    current_page = 1

    while True:
        # Fetch page
        url = f'{FundrefTelescope.TELESCOPE_URL}?per_page=100&page={current_page}'
        response = retry_session().get(url, headers=headers)

        # Check if correct response code
        if response is not None and response.status_code == 200:
            # Parse json
            num_pages = int(response.headers['X-Total-Pages'])
            json_response = json.loads(response.text)

            # Parse release information
            for release in json_response:
                version = float(release['tag_name'].strip('v'))
                for source in release['assets']['sources']:
                    if source['format'] == 'tar.gz':
                        # Parse release date
                        if version == 0.1:
                            release_date = pendulum.datetime(year=2014, month=3, day=1)
                        elif version < 1.0:
                            date_string = release['description'].split('\n')[0]
                            release_date = pendulum.parse('01' + date_string)
                        else:
                            release_date = pendulum.parse(release['released_at'])

                        # Only include release if it is within start and end dates
                        if start_date <= release_date < end_date:
                            release = FundrefRelease(source['url'], release_date)
                            releases_list.append(release)

            # Check if we should exit or get the next page
            if num_pages <= current_page:
                break
            current_page += 1
        else:
            logging.error(f"Error retrieving response")
            exit(os.EX_DATAERR)

    return releases_list


def download_release(release: FundrefRelease) -> str:
    """ Downloads release from url.

    :param release: Instance of FundrefRelease class
    """

    file_path = release.filepath_download
    logging.info(f"Downloading file: {file_path}, url: {release.url}")

    # A selection of headers to prevent 403/forbidden error.
    headers_list = [{
        'authority': 'gitlab.com',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/83.0.4103.116 Safari/537.36',
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,'
                  '*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'sec-fetch-site': 'none',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-dest': 'document',
        'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8'
    },

        {
            'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:78.0) Gecko/20100101 Firefox/78.0',
            'Accept': '*/*',
            'Accept-Language': 'en-US,en;q=0.5',
            'Referer': 'https://gitlab.com/'
        }]

    # Download release
    with requests.get(release.url, headers=random.choice(headers_list), stream=True) as response:
        with open(file_path, 'wb') as file:
            shutil.copyfileobj(response.raw, file)

    return file_path


def extract_release(release: FundrefRelease) -> str:
    """ Extract release.

    :param release: Instance of FundrefRelease class
    """

    logging.info(f"Extracting file: {release.filepath_download}")
    # Tar file contains both README.md and registry.rdf, use tar -ztf to get path of 'registry.rdf'
    # Use this path to extract only registry.rdf to a new file.
    cmd = f"registry_path=$(tar -ztf {release.filepath_download} | grep -m1 '/registry.rdf'); " \
          f"tar -xOzf {release.filepath_download} $registry_path > {release.filepath_extract}"
    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, executable='/bin/bash')
    stdout, stderr = wait_for_process(p)

    if stdout:
        logging.info(stdout)

    if stderr:
        raise AirflowException(f"bash command failed for {release.url}: {stderr}")

    logging.info(f"File extracted to: {release.filepath_extract}")

    return release.filepath_extract


def strip_whitespace(file_path: str):
    """ Strip leading white space from the first line of the file.
    This is present in fundref release 2019-06-01. If not removed it will give a XML ParseError.

    :param file_path: Path to file from which to trim leading white space.
    :return: None.
    """
    with open(file_path, 'r') as f_in, open(file_path + '.tmp', 'w') as f_out:
        first_line = True
        for line in f_in:
            if first_line and not line.startswith(' '):
                os.remove(file_path + '.tmp')
                return
            elif first_line and line.startswith(' '):
                line = line.lstrip()
            f_out.write(line)
            first_line = False
    os.rename(file_path + '.tmp', file_path)


def transform_release(release: FundrefRelease) -> str:
    """ Transform release by parsing the raw rdf file, transforming it into a json file and replacing geoname associated
    ids with their geoname name.

    :param release: Instance of FundrefRelease class
    """

    # Strip leading whitespace from first line if present.
    strip_whitespace(release.filepath_extract)

    # Parse RDF funders data
    funders, funders_by_key = parse_fundref_registry_rdf(release.filepath_extract)
    funders = add_funders_relationships(funders, funders_by_key)

    # Transform FundRef release into JSON Lines format saving in memory buffer
    # Save in memory buffer to gzipped file

    with io.BytesIO() as bytes_io:
        with gzip.GzipFile(fileobj=bytes_io, mode='w') as gzip_file:
            with jsonlines.Writer(gzip_file) as writer:
                writer.write_all(funders)

        with open(release.filepath_transform, 'wb') as jsonl_gzip_file:
            jsonl_gzip_file.write(bytes_io.getvalue())

    logging.info(f'Success transforming release: {release.url}')

    return release.filepath_transform


class FundrefRelease:
    """ Used to store info on a given fundref release """

    def __init__(self, url: str, date: Pendulum):
        self.url = url
        self.date = date
        self.filepath_download = self.get_filepath(SubFolder.downloaded)
        self.filepath_extract = self.get_filepath(SubFolder.extracted)
        self.filepath_transform = self.get_filepath(SubFolder.transformed)

    def get_filepath(self, sub_folder: SubFolder) -> str:
        """ Gets complete path of file for download/extract/transform directory

        :param sub_folder: name of subfolder
        :return: path of file.
        """

        date_str = self.date.strftime("%Y_%m_%d")

        if sub_folder == SubFolder.downloaded:
            file_name = f"{FundrefTelescope.DAG_ID}_{date_str}.tar.gz"
        elif sub_folder == SubFolder.extracted:
            file_name = f"{FundrefTelescope.DAG_ID}_{date_str}.rdf"
        else:
            file_name = f"{FundrefTelescope.DAG_ID}_{date_str}.jsonl.gz"

        file_dir = telescope_path(sub_folder, FundrefTelescope.DAG_ID)
        path = os.path.join(file_dir, file_name)

        return path

    def get_blob_name(self, sub_folder: SubFolder) -> str:
        """ Gives blob name that is used to determine path inside storage bucket

        :param sub_folder: name of subfolder
        :return: blob name
        """

        file_name = os.path.basename(self.get_filepath(sub_folder))
        blob_name = f'telescopes/{FundrefTelescope.DAG_ID}/{file_name}'

        return blob_name


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


def parse_fundref_registry_rdf(registry_file_path: str) -> Tuple[List, Dict]:
    """ Helper function to parse a fundref registry rdf file and to return a python list containing each funder.

    :param registry_file_path: the filename of the registry.rdf file to be parsed.
    :return: funders list containing all the funders parsed from the input rdf and dictionary of funders with their
    id as key.
    """

    funders = []
    funders_by_key = {}

    tree = ET.parse(registry_file_path)
    root = tree.getroot()
    tag_prefix = root.tag.split('}')[0] + '}'
    for record in root:
        tag = record.tag.split('}')[-1]
        if tag == "ConceptScheme":
            for nested in record:
                tag = nested.tag.split('}')[-1]
                if tag == 'hasTopConcept':
                    funder_id = nested.attrib[tag_prefix + 'resource']
                    funders_by_key[funder_id] = new_funder_template()

        if tag == "Concept":
            funder_id = record.attrib[tag_prefix + 'about']
            funder = funders_by_key[funder_id]
            funder['funder'] = funder_id
            for nested in record:
                tag = nested.tag.split('}')[-1]
                if tag == 'inScheme':
                    continue
                elif tag == 'prefLabel':
                    funder['pre_label'] = nested[0][0].text
                elif tag == 'altLabel':
                    funder['alt_label'].append(nested[0][0].text)
                elif tag == 'narrower':
                    funder['narrower'].append(nested.attrib[tag_prefix + 'resource'])
                elif tag == 'broader':
                    funder['broader'].append(nested.attrib[tag_prefix + 'resource'])
                elif tag == 'modified':
                    funder['modified'] = nested.text
                elif tag == 'created':
                    funder['created'] = nested.text
                elif tag == 'fundingBodySubType':
                    funder['funding_body_type'] = nested.text
                elif tag == 'fundingBodyType':
                    funder['funding_body_sub_type'] = nested.text
                elif tag == 'region':
                    funder['region'] = nested.text
                elif tag == 'country':
                    funder['country'] = nested.text
                elif tag == 'state':
                    funder['state'] = nested.text
                elif tag == 'address':
                    funder['country_code'] = nested[0][0].text
                elif tag == 'taxId':
                    funder['tax_id'] = nested.text
                elif tag == 'continuationOf':
                    funder['continuation_of'].append(nested.attrib[tag_prefix + 'resource'])
                elif tag == 'renamedAs':
                    funder['renamed_as'].append(nested.attrib[tag_prefix + 'resource'])
                elif tag == 'replaces':
                    funder['replaces'].append(nested.attrib[tag_prefix + 'resource'])
                elif tag == 'affilWith':
                    funder['affil_with'].append(nested.attrib[tag_prefix + 'resource'])
                elif tag == 'mergedWith':
                    funder['merged_with'].append(nested.attrib[tag_prefix + 'resource'])
                elif tag == 'incorporatedInto':
                    funder['incorporated_into'].append(nested.attrib[tag_prefix + 'resource'])
                elif tag == 'isReplacedBy':
                    funder['is_replaced_by'].append(nested.attrib[tag_prefix + 'resource'])
                elif tag == 'incorporates':
                    funder['incorporates'].append(nested.attrib[tag_prefix + 'resource'])
                elif tag == 'splitInto':
                    funder['split_into'].append(nested.attrib[tag_prefix + 'resource'])
                elif tag == 'status':
                    funder['status'] = nested.attrib[tag_prefix + 'resource']
                elif tag == 'mergerOf':
                    funder['merger_of'].append(nested.attrib[tag_prefix + 'resource'])
                elif tag == 'splitFrom':
                    funder['split_from'] = nested.attrib[tag_prefix + 'resource']
                elif tag == 'formerlyKnownAs':
                    funder['formly_known_as'] = nested.attrib[tag_prefix + 'resource']
                elif tag == 'notation':
                    funder['notation'] = nested.text
                else:
                    print(f"Unrecognized tag for element: {nested}")

            funders.append(funder)

    return funders, funders_by_key


def add_funders_relationships(funders: List, funders_by_key: Dict) -> List:
    """ Adds any children/parent relationships to funder instances in the funders list.

    :param funders: List of funders
    :param funders_by_key: Dictionary of funders with their id as key.
    :return: funders with added relationships.
    """

    for funder in funders:
        children, returned_depth = recursive_funders(funders_by_key, funder, 0, 'narrower', [])
        funder["children"] = children
        funder['bottom'] = len(children) > 0

        parent, returned_depth = recursive_funders(funders_by_key, funder, 0, 'broader', [])
        funder["parents"] = parent
        funder['top'] = len(parent) > 0

    return funders


def recursive_funders(funders_by_key: Dict, funder: Dict, depth: int, direction: str, parents: List) -> Tuple[
    List, int]:
    """ Recursively goes through a funder/sub_funder dict. The funder properties can be looked up with the
    funders_by_key
    dictionary that stores the properties per funder id. Any children/parents for the funder are already given in the
    xml element with the 'narrower' and 'broader' tags. For each funder in the list, it will recursively add any
    children/parents for those funders in 'narrower'/'broader' and their funder properties.

    :param funders_by_key: dictionary with id as key and funders object as value
    :param funder: dictionary of a given funder containing 'narrower' and 'broader' info
    :param depth: keeping track of nested depth
    :param direction: either 'narrower' or 'broader' to get 'children' or 'parents'
    :param parents: list to keep track of which funder ids are parents
    :return: list of children and current depth
    """

    starting_depth = depth
    children = []
    for funder_id in funder[direction]:
        if funder_id in parents:
            print(f"funder {funder_id} is it's own parent/child, skipping..")
            name = 'NA'
            returned = []
            returned_depth = depth
        else:
            try:
                sub_funder = funders_by_key[funder_id]
                parents.append(sub_funder['funder'])
                name = sub_funder['pre_label']

                returned, returned_depth = recursive_funders(funders_by_key, sub_funder, starting_depth + 1, direction,
                                                             parents)
            except KeyError:
                print(f'Could not find funder by id: {funder_id}, skipping..')
                name = 'NA'
                returned = []
                returned_depth = depth

        if direction == "narrower":
            child = {
                'funder': funder_id,
                'name': name,
                'children': returned
            }
        else:
            child = {
                'funder': funder_id,
                'name': name,
                'parent': returned
            }
        children.append(child)
        parents = []
        if returned_depth > depth:
            depth = returned_depth
    return children, depth


def pull_releases(ti: TaskInstance):
    """ Pull a list of MagRelease instances with xcom.

    :param ti: the Apache Airflow task instance.
    :return: the list of MagRelease instances.
    """

    return ti.xcom_pull(key=FundrefTelescope.RELEASES_TOPIC_NAME, task_ids=FundrefTelescope.TASK_ID_LIST,
                        include_prior_dates=False)


class FundrefTelescope:
    """ A container for holding the constants and static functions for the Fundref telescope. """

    DAG_ID = 'fundref'
    DESCRIPTION = 'The Funder Registry dataset: https://www.crossref.org/services/funder-registry/'
    DATASET_ID = 'crossref'
    RELEASES_TOPIC_NAME = "releases"
    QUEUE = "remote_queue"
    TELESCOPE_URL = 'https://gitlab.com/api/v4/projects/crossref%2Fopen_funder_registry/releases'
    TELESCOPE_DEBUG_URL = 'debug_fundref_url'
    # DEBUG_FILE_PATH = os.path.join(test_data_path(), 'telescopes', 'fundref.tar.gz')
    RETRIES = 3

    TASK_ID_CHECK_DEPENDENCIES = "check_dependencies"
    TASK_ID_LIST = f"list_releases"
    TASK_ID_DOWNLOAD = f"download"
    TASK_ID_UPLOAD_DOWNLOADED = 'upload_downloaded'
    TASK_ID_EXTRACT = f"extract"
    TASK_ID_TRANSFORM = f"transform_releases"
    TASK_ID_UPLOAD_TRANSFORMED = 'upload_transformed'
    TASK_ID_BQ_LOAD = f"bq_load"
    TASK_ID_CLEANUP = f"cleanup"

    @staticmethod
    def check_dependencies(**kwargs):
        """ Check that all variables exist that are required to run the DAG.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        vars_valid = check_variables(AirflowVars.DATA_PATH, AirflowVars.PROJECT_ID,
                                     AirflowVars.DATA_LOCATION, AirflowVars.DOWNLOAD_BUCKET_NAME,
                                     AirflowVars.TRANSFORM_BUCKET_NAME)
        if not vars_valid:
            raise AirflowException('Required variables are missing')

    @staticmethod
    def list_releases(**kwargs):
        """ Based on a list of all releases, checks which ones were released between this and the next execution date
        of the DAG. If the release falls within the time period mentioned above, checks if a bigquery table doesn't
        exist yet for the release. A list of releases that passed both checks is passed to the next tasks. If the
        list is empty the workflow will stop.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        # Get variables
        project_id = Variable.get(AirflowVars.PROJECT_ID)

        # List releases between a start date and an end date
        execution_date = kwargs['execution_date']
        next_execution_date = kwargs['next_execution_date']
        releases_list = list_releases(execution_date, next_execution_date)
        logging.info(f'Releases between current ({execution_date}) and next ({next_execution_date}) execution date:')
        print(*releases_list, sep='\n')

        # Check if the BigQuery table for each release already exists and only process release if the table
        # doesn't exist
        releases_list_out = []
        for release in releases_list:
            table_id = bigquery_partitioned_table_id(FundrefTelescope.DAG_ID, release.date)
            logging.info('Checking if bigquery table already exists:')
            if bigquery_table_exists(project_id, FundrefTelescope.DATASET_ID, table_id):
                logging.info(f'Skipping as table exists for {release.url}: '
                             f'{project_id}.{FundrefTelescope.DATASET_ID}.{table_id}')
            else:
                logging.info(f"Table doesn't exist yet, processing {release.url} in this workflow")
                releases_list_out.append(release)

        # If releases_list_out contains items then the DAG will continue (return True) otherwise it will
        # stop (return False)
        continue_dag = len(releases_list_out)
        if continue_dag:
            ti: TaskInstance = kwargs['ti']
            ti.xcom_push(FundrefTelescope.RELEASES_TOPIC_NAME, releases_list_out, execution_date)
        return continue_dag

    @staticmethod
    def download(**kwargs):
        """ Download release to file. If develop environment, copy debug file from this repository to the right location.
        Else download from url.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        # Pull releases
        ti: TaskInstance = kwargs['ti']
        releases_list = pull_releases(ti)

        # Get variables
        environment = Variable.get(AirflowVars.ENVIRONMENT)

        # Download each release
        for release in releases_list:
            if environment == 'develop':
                shutil.copy(FundrefTelescope.DEBUG_FILE_PATH, release.filepath_download)
            else:
                download_release(release)

    @staticmethod
    def upload_downloaded(**kwargs):
        """ Upload the downloaded files to a Google Cloud Storage bucket.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        # Pull releases
        ti: TaskInstance = kwargs['ti']
        releases_list = pull_releases(ti)

        # Get variables
        bucket_name = Variable.get(AirflowVars.DOWNLOAD_BUCKET_NAME)

        # Upload each release
        for release in releases_list:
            upload_file_to_cloud_storage(bucket_name, release.get_blob_name(SubFolder.downloaded),
                                         file_path=release.filepath_download)

    @staticmethod
    def extract(**kwargs):
        """ Extract release to new file.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        # Pull releases
        ti: TaskInstance = kwargs['ti']
        releases_list = pull_releases(ti)

        # Extract each release
        for release in releases_list:
            extract_release(release)

    @staticmethod
    def transform(**kwargs):
        """ Transform release by parsing the raw rdf file, transforming it into a json file and replacing geoname
        associated
        ids with their geoname name.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        # Pull releases
        ti: TaskInstance = kwargs['ti']
        releases_list = pull_releases(ti)

        # Transform each release
        for release in releases_list:
            transform_release(release)

    @staticmethod
    def upload_transformed(**kwargs):
        """ Upload the transformed release to a Google Cloud Storage bucket.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        # Pull releases
        ti: TaskInstance = kwargs['ti']
        releases_list = pull_releases(ti)

        # Get variables
        bucket_name = Variable.get(AirflowVars.TRANSFORM_BUCKET_NAME)

        # Upload each release
        for release in releases_list:
            upload_file_to_cloud_storage(bucket_name, release.get_blob_name(SubFolder.transformed),
                                         file_path=release.filepath_transform)

    @staticmethod
    def bq_load(**kwargs):
        """ Upload transformed release to a bigquery table.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        # Pull releases
        ti: TaskInstance = kwargs['ti']
        releases_list = pull_releases(ti)

        # Get variables
        project_id = Variable.get(AirflowVars.PROJECT_ID)
        data_location = Variable.get(AirflowVars.DATA_LOCATION)
        bucket_name = Variable.get(AirflowVars.TRANSFORM_BUCKET_NAME)

        # Create dataset
        create_bigquery_dataset(project_id, FundrefTelescope.DAG_ID, data_location, FundrefTelescope.DESCRIPTION)

        # Load each release into BigQuery
        for release in releases_list:
            table_id = bigquery_partitioned_table_id(FundrefTelescope.DAG_ID, release.date)

            # Select schema file based on release date
            analysis_schema_path = schema_path()
            schema_file_path = find_schema(analysis_schema_path, FundrefTelescope.DAG_ID, release.date)
            if schema_file_path is None:
                logging.error(f'No schema found with search parameters: analysis_schema_path={analysis_schema_path}, '
                              f'table_name={FundrefTelescope.DAG_ID}, release_date={release.date}')
                exit(os.EX_CONFIG)

            # Load BigQuery table
            uri = f"gs://{bucket_name}/{release.get_blob_name(SubFolder.transformed)}"
            logging.info(f"URI: {uri}")
            load_bigquery_table(uri, FundrefTelescope.DAG_ID, data_location, table_id, schema_file_path,
                                SourceFormat.NEWLINE_DELIMITED_JSON)

    @staticmethod
    def cleanup(**kwargs):
        """ Delete files of downloaded, extracted and transformed releases.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html
        for a list of the keyword arguments that are passed to this argument.
        :return: None.
        """

        # Pull releases
        ti: TaskInstance = kwargs['ti']
        releases_list = pull_releases(ti)

        # Delete files for each release
        for release in releases_list:
            try:
                pathlib.Path(release.filepath_download).unlink()
            except FileNotFoundError as e:
                logging.warning(f"No such file or directory {release.filepath_download}: {e}")

            try:
                pathlib.Path(release.filepath_extract).unlink()
            except FileNotFoundError as e:
                logging.warning(f"No such file or directory {release.filepath_extract}: {e}")

            try:
                pathlib.Path(release.filepath_transform).unlink()
            except FileNotFoundError as e:
                logging.warning(f"No such file or directory {release.filepath_transform}: {e}")
