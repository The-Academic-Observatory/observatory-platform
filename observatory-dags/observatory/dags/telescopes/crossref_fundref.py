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
import random
import shutil
import subprocess
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import Dict, List, Tuple

import jsonlines
import pendulum
import requests
from airflow.api.common.experimental.pool import create_pool
from airflow.exceptions import AirflowException
from airflow.models.taskinstance import TaskInstance
from observatory.platform.telescopes.snapshot_telescope import SnapshotRelease, SnapshotTelescope
from observatory.platform.utils.airflow_utils import AirflowVariable as Variable, AirflowVars
from observatory.platform.utils.gc_utils import (bigquery_sharded_table_id,
                                                 bigquery_table_exists)
from observatory.platform.utils.proc_utils import wait_for_process
from observatory.platform.utils.template_utils import upload_files_from_list
from observatory.platform.utils.url_utils import retry_session
from pendulum import Pendulum


class CrossrefFundrefRelease(SnapshotRelease):
    def __init__(self, dag_id: str, release_date: pendulum.Pendulum, url: str):
        """ Construct a CrossrefFundrefRelease

        :param dag_id: The DAG id.
        :param release_date: The release date.
        :param url: The url corresponding with this release date.
        """
        self.url = url

        download_files_regex = 'crossref_fundref.tar.gz'
        extract_files_regex = 'crossref_fundref.rdf'
        transform_files_regex = 'crossref_fundref.jsonl.gz'
        super().__init__(dag_id, release_date, download_files_regex, extract_files_regex, transform_files_regex)

    @property
    def download_path(self) -> str:
        """ Get the path to the downloaded file.

        :return: the file path.
        """
        return os.path.join(self.download_folder, "crossref_fundref.tar.gz")

    @property
    def extract_path(self) -> str:
        """ Get the path to the extracted file.

        :return: the file path.
        """

        return os.path.join(self.extract_folder, "crossref_fundref.rdf")

    @property
    def transform_path(self) -> str:
        """ Get the path to the transformed file.

        :return: the file path.
        """

        return os.path.join(self.transform_folder, "crossref_fundref.jsonl.gz")

    def download(self):
        """ Downloads release tar.gz file from url.
        """

        logging.info(f"Downloading file: {self.download_path}, url: {self.url}")

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
        with requests.get(self.url, headers=random.choice(headers_list), stream=True) as response:
            with open(self.download_path, 'wb') as file:
                shutil.copyfileobj(response.raw, file)

    def extract(self):
        """ Extract release from gzipped tar file.

        """
        logging.info(f"Extracting file: {self.download_path}")
        # Tar file contains both README.md and registry.rdf, use tar -ztf to get path of 'registry.rdf'
        # Use this path to extract only registry.rdf to a new file.
        cmd = f"registry_path=$(tar -ztf {self.download_path} | grep -m1 '/registry.rdf'); " \
              f"tar -xOzf {self.download_path} $registry_path > {self.extract_path}"
        p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, executable='/bin/bash')
        stdout, stderr = wait_for_process(p)

        if stdout:
            logging.info(stdout)

        if stderr:
            raise AirflowException(f"bash command failed for {self.url}: {stderr}")

        logging.info(f"File extracted to: {self.extract_path}")

    def transform(self):
        """ Transforms release by storing file content in gzipped json format. Relationships between funders are added.

        :return: None
        """

        # Strip leading whitespace from first line if present.
        strip_whitespace(self.extract_path)

        # Parse RDF funders data
        funders, funders_by_key = parse_fundref_registry_rdf(self.extract_path)
        funders = add_funders_relationships(funders, funders_by_key)

        # Transform FundRef release into JSON Lines format saving in memory buffer
        # Save in memory buffer to gzipped file
        with io.BytesIO() as bytes_io:
            with gzip.GzipFile(fileobj=bytes_io, mode='w') as gzip_file:
                with jsonlines.Writer(gzip_file) as writer:
                    writer.write_all(funders)

            with open(self.transform_path, 'wb') as jsonl_gzip_file:
                jsonl_gzip_file.write(bytes_io.getvalue())

        logging.info(f'Success transforming release: {self.url}')


class CrossrefFundrefTelescope(SnapshotTelescope):
    """ Crossref Fundref Telescope."""
    DAG_ID = 'crossref_fundref'
    DATASET_ID = 'crossref'
    RELEASES_URL = 'https://gitlab.com/api/v4/projects/crossref%2Fopen_funder_registry/releases'

    def __init__(self, dag_id: str = DAG_ID, start_date: datetime = datetime(2014, 3, 1),
                 schedule_interval: str = '@weekly', dataset_id: str = DATASET_ID,
                 table_descriptions: Dict = None, airflow_vars: List = None):

        """ Construct a CrossrefFundrefTelescope instance.

        :param dag_id: the id of the DAG.
        :param start_date: the start date of the DAG.
        :param schedule_interval: the schedule interval of the DAG.
        :param dataset_id: the BigQuery dataset id.
        :param table_descriptions: a dictionary with table ids and corresponding table descriptions.
        :param catchup:  whether to catchup the DAG or not.
        :param airflow_vars: list of airflow variable keys, for each variable it is checked if it exists in airflow.
        """

        if table_descriptions is None:
            table_descriptions = {dag_id: 'The Funder Registry dataset: '
                                          'https://www.crossref.org/services/funder-registry/'}

        if airflow_vars is None:
            airflow_vars = [AirflowVars.DATA_PATH, AirflowVars.PROJECT_ID, AirflowVars.DATA_LOCATION,
                            AirflowVars.DOWNLOAD_BUCKET, AirflowVars.TRANSFORM_BUCKET]
        super().__init__(dag_id, start_date, schedule_interval, dataset_id,
                         table_descriptions=table_descriptions,
                         airflow_vars=airflow_vars)

        # Create Gitlab pool to limit the number of connections to Gitlab, which is very quick to block requests if
        # there are too many at once.
        pool_name = 'gitlab_pool'
        num_slots = 2
        description = 'A pool to limit the connections to Gitlab.'
        create_pool(pool_name, num_slots, description)

        self.add_setup_task(self.check_dependencies)
        self.add_setup_task(self.get_release_info, pool=pool_name)
        self.add_task(self.download, pool=pool_name)
        self.add_task(self.upload_downloaded)
        self.add_task(self.extract)
        self.add_task(self.transform)
        self.add_task(self.upload_transformed)
        self.add_task(self.bq_load)
        self.add_task(self.cleanup)

    def make_release(self, **kwargs) -> List[CrossrefFundrefRelease]:
        """ Make release instances. The release is passed as an argument to the function (TelescopeFunction) that is
        called in 'task_callable'.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are
        passed to this argument.
        :return: a list of GeonamesRelease instances.
        """

        ti: TaskInstance = kwargs['ti']
        release_info = ti.xcom_pull(key=CrossrefFundrefTelescope.RELEASE_INFO,
                                    task_ids=self.get_release_info.__name__,
                                    include_prior_dates=False)
        releases = []
        for release in release_info:
            releases.append(CrossrefFundrefRelease(self.dag_id, release['date'], release['url']))
        return releases

    def get_release_info(self, **kwargs) -> bool:
        """ Based on a list of all releases, checks which ones were released between the prev and this execution date
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
        prev_execution_date = kwargs['prev_execution_date']
        execution_date = kwargs['execution_date']
        releases_list = list_releases(prev_execution_date, execution_date)
        logging.info(f'Releases between prev ({prev_execution_date}) and current ({execution_date}) execution date:')
        logging.info(releases_list)

        # Check if the BigQuery table for each release already exists and only process release if the table
        # doesn't exist
        releases_list_out = []
        for release in releases_list:
            table_id = bigquery_sharded_table_id(CrossrefFundrefTelescope.DAG_ID, release['date'])
            logging.info('Checking if bigquery table already exists:')
            if bigquery_table_exists(project_id, self.dataset_id, table_id):
                logging.info(f"Skipping as table exists for {release['url']}: "
                             f'{project_id}.{self.dataset_id}.{table_id}')
            else:
                logging.info(f"Table does not exist yet, processing {release['url']} in this workflow")
                releases_list_out.append(release)

        # If releases_list_out contains items then the DAG will continue (return True) otherwise it will
        # stop (return False)
        continue_dag = len(releases_list_out) > 0
        if continue_dag:
            ti: TaskInstance = kwargs['ti']
            ti.xcom_push(CrossrefFundrefTelescope.RELEASE_INFO, releases_list_out, execution_date)
        return continue_dag

    def download(self, releases: List[CrossrefFundrefRelease], **kwargs):
        """ Task to download the releases.

        :param releases: a list with Crossref Fundref releases.
        :return: None.
        """
        for release in releases:
            release.download()

    def upload_downloaded(self, releases: List[CrossrefFundrefRelease], **kwargs):
        """ Task to upload the downloaded releases.

        :param releases: a list with Crossref Fundref releases.
        :return: None.
        """
        for release in releases:
            upload_files_from_list(release.download_files, release.download_bucket)

    def extract(self, releases: List[CrossrefFundrefRelease], **kwargs):
        """ Task to extract the releases.

        :param releases: a list with Crossref Fundref releases.
        :return: None.
        """
        for release in releases:
            release.extract()

    def transform(self, releases: List[CrossrefFundrefRelease], **kwargs):
        """ Task to transform the releases.

        :param releases: a list with Crossref Fundref releases.
        :return: None.
        """
        for release in releases:
            release.transform()


def list_releases(start_date: Pendulum, end_date: Pendulum) -> List[dict]:
    """ List all available CrossrefFundref releases between the start and end date

    :param start_date: The start date of the period to look for releases
    :param end_date: The end date of the period to look for releases
    :return: list with dictionaries of release info (url and release date)
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

    release_info = []
    headers = random.choice(headers_list)
    current_page = 1

    while True:
        # Fetch page
        url = f'{CrossrefFundrefTelescope.RELEASES_URL}?per_page=100&page={current_page}'
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
                            release_info.append({'url': source['url'], 'date': release_date})

            # Check if we should exit or get the next page
            if num_pages <= current_page:
                break
            current_page += 1
        else:
            raise AirflowException(f"Error retrieving response from: {url}")

    return release_info


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
                    alt_label = nested[0][0].text
                    if alt_label is not None:
                        funder['alt_label'].append(alt_label)
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
                    logging.info(f"Unrecognized tag for element: {nested}")

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


def recursive_funders(funders_by_key: Dict, funder: Dict, depth: int, direction: str, sub_funders: List) -> Tuple[
    List, int]:
    """ Recursively goes through a funder/sub_funder dict. The funder properties can be looked up with the
    funders_by_key dictionary that stores the properties per funder id. Any children/parents for the funder are
    already given in the xml element with the 'narrower' and 'broader' tags. For each funder in the list,
    it will recursively add any children/parents for those funders in 'narrower'/'broader' and their funder properties.

    :param funders_by_key: dictionary with id as key and funders object as value
    :param funder: dictionary of a given funder containing 'narrower' and 'broader' info
    :param depth: keeping track of nested depth
    :param direction: either 'narrower' or 'broader' to get 'children' or 'parents'
    :param sub_funders: list to keep track of which funder ids are parents
    :return: list of children and current depth
    """

    starting_depth = depth
    children = []
    # Loop through funder_ids in 'narrower' or 'broader' info
    for funder_id in funder[direction]:
        if funder_id in sub_funders:
            # Stop recursion if funder is it's own parent or child
            logging.info(f"Funder {funder_id} is it's own parent/child, skipping..")
            name = 'NA'
            returned = []
            returned_depth = depth
            sub_funders.append(funder_id)
        else:
            try:
                sub_funder = funders_by_key[funder_id]
                # Add funder id of sub_funder to list to keep track of 'higher' sub_funders in the recursion
                sub_funders.append(sub_funder['funder'])
                # Store name to pass on to child object
                name = sub_funder['pre_label']
                # Get children/parents of sub_funder
                returned, returned_depth = recursive_funders(funders_by_key, sub_funder, starting_depth + 1, direction,
                                                             sub_funders)
            except KeyError:
                logging.info(f'Could not find funder by id: {funder_id}, skipping..')
                name = 'NA'
                returned = []
                returned_depth = depth
                sub_funders.append(funder_id)

        # Add child/parent (containing nested children/parents) to list
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
        sub_funders.pop(-1)
        if returned_depth > depth:
            depth = returned_depth
    return children, depth
