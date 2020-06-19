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

# Author: James Diprose


import glob
import gzip
import io
import json
import logging
import os
from shutil import copyfile
from typing import List, Dict, Tuple
from zipfile import ZipFile, BadZipFile

import jsonlines
import pendulum
from airflow.models.taskinstance import TaskInstance
from google.cloud.bigquery import SourceFormat
from pendulum import Pendulum

from academic_observatory.utils.config_utils import telescope_path, SubFolder, schema_path, ObservatoryConfig, \
    find_schema
from academic_observatory.utils.data_utils import get_file
from academic_observatory.utils.gc_utils import upload_file_to_cloud_storage, load_bigquery_table, \
    create_bigquery_dataset, bigquery_partitioned_table_id
from academic_observatory.utils.url_utils import retry_session

GRID_DATASET_URL = "https://api.figshare.com/v2/collections/3812929/articles?page_size=1000"
GRID_FILE_URL = "https://api.figshare.com/v2/articles/{article_id}/files"


def list_grid_releases(timeout: float = 30.) -> List[Dict]:
    """ List all GRID releases available on Figshare.

    :param timeout: the number of seconds to wait until timing out.
    :return: the list of GRID releases.
    """

    response = retry_session().get(GRID_DATASET_URL, timeout=timeout, headers={'Accept-encoding': 'gzip'})
    return json.loads(response.text)


def download_grid_release(download_path: str, article_id: int, title: str, timeout: float = 30.) -> List[str]:
    """ Downloads an individual GRID release from Figshare.

    :param download_path: the directory where the downloaded GRID release should be saved.
    :param article_id: the Figshare article id of the GRID release. Called 'id' in the returned dictionary.
    :param title: the title of the Figshare article.
    :param timeout: the timeout in seconds when calling the Figshare API.
    :return: the paths on the system of the downloaded files.
    """

    response = retry_session().get(GRID_FILE_URL.format(article_id=article_id), timeout=timeout,
                                   headers={'Accept-encoding': 'gzip'})
    article_files = json.loads(response.text)

    downloads = []
    for i, article_file in enumerate(article_files):
        real_file_name = article_file['name']
        supplied_md5 = article_file['supplied_md5']
        download_url = article_file['download_url']
        file_type = os.path.splitext(real_file_name)[1]

        # Download
        logging.info(f"Downloading file: {real_file_name}, md5: {supplied_md5}, url: {download_url}")
        # The title is used for the filename because they seem to be labelled more reliably than the files.
        # Replace spaces with underscores and hyphens with underscores for consistency.
        # The index is added onto the end in case there are multiple files in one article
        dir_name = f'{title.replace(" ", "_").replace("-", "_")}-{i}'
        file_name = f"{dir_name}{file_type}"
        file_path, updated = get_file(file_name, download_url, md5_hash=supplied_md5, cache_subdir='',
                                      cache_dir=download_path)
        downloads.append(file_path)

    return downloads


def extract_grid_release(file_path: str, extraction_path: str) -> str:
    """ Extract a single GRID release to a given extraction path. The release will be extracted into the following
    directory structure: extraction_path/file_name (without extension).

    If the release is a .zip file, it will be extracted, otherwise it will be copied to a directory within the
    extraction path.

    :param file_path: the path to the downloaded GRID release.
    :param extraction_path: the folder to extract the files into.
    :return: the path to folder that the release was extracted or copied into.
    """

    # Make file and directory names
    file_name = os.path.basename(file_path)
    dir_name = os.path.basename(file_path).split(".")[0]

    # Extract zip files, leave other files such as .json and .csv
    unzip_folder_path = os.path.join(extraction_path, dir_name)
    if file_path.endswith(".zip"):
        logging.info(f"Extracting file: {file_path}")
        try:
            with ZipFile(file_path) as zip_file:
                zip_file.extractall(unzip_folder_path)
        except BadZipFile:
            logging.error("Not a zip file")
        extracted_folder_path = unzip_folder_path
        logging.info(f"File extracted to: {extracted_folder_path}")
    else:
        # File is already uncompressed, so make a directory and copy it into it
        extracted_folder_path = os.path.join(extraction_path, dir_name)
        extracted_file_path = os.path.join(extracted_folder_path, 'grid.json')
        if not os.path.exists(extracted_folder_path):
            os.makedirs(extracted_folder_path, exist_ok=True)
        copyfile(file_path, extracted_file_path)
        logging.info(f"File saved to: {extracted_file_path}")

    return extracted_folder_path


def transform_grid_release(release_json_path: str, transformed_path: str) -> Tuple[str, str, str]:
    """ Transform an extracted GRID release .json file into json lines format and gzip the result.

    :param release_json_path: the path to GRID release .json file.
    :param transformed_path: the path to save the results.
    :return: the GRID version, the file name and the file path.
    """

    with open(release_json_path) as json_file:
        # Load GRID release JSON file
        data = json.load(json_file)
        version = data['version']
        institutes = data['institutes']

        # Transform GRID release into JSON Lines format saving in memory buffer
        # Save in memory buffer to gzipped file
        with io.BytesIO() as bytes_io:
            with gzip.GzipFile(fileobj=bytes_io, mode='w') as gzip_file:
                with jsonlines.Writer(gzip_file) as writer:
                    writer.write_all(institutes)

            file_name = f"grid_{version}.jsonl.gz"
            file_path = os.path.join(transformed_path, file_name)
            with open(file_path, 'wb') as jsonl_gzip_file:
                jsonl_gzip_file.write(bytes_io.getvalue())

    return version, file_name, file_path


class GridTelescope:
    """ A container for holding the constants and static functions for the GRID telescope. """

    DAG_ID = 'grid'
    DESCRIPTION = 'The Global Research Identifier Database (GRID): https://grid.ac/'
    TASK_ID_LIST = f'{DAG_ID}_list_releases'
    TASK_ID_DOWNLOAD = f'{DAG_ID}_download'
    TASK_ID_EXTRACT = f'{DAG_ID}_extract'
    TASK_ID_TRANSFORM = f'{DAG_ID}_transform'
    TASK_ID_UPLOAD = f'{DAG_ID}_upload'
    TASK_ID_DB_LOAD = f'{DAG_ID}_db_load'
    TASK_ID_CLEANUP = f'{DAG_ID}_cleanup'
    TASK_ID_STOP = f'{DAG_ID}_stop'
    TOPIC_NAME = 'message'

    @staticmethod
    def list_releases(**kwargs):
        """ Task to lists all GRID releases for a given month """

        execution_date = kwargs['execution_date']
        next_execution_date = kwargs['next_execution_date']
        grid_releases = list_grid_releases()

        # Select the GRID releases that were published on or after the execution_date and before the next_execution_date
        msgs_out = []
        for release in grid_releases:
            published_date: Pendulum = pendulum.parse(release['published_date'])

            if execution_date <= published_date < next_execution_date:
                msg = dict()
                msg['article_id'] = release['id']
                msg['title'] = release['title']
                msg['published_date'] = published_date
                msgs_out.append(msg)

        # Push messages
        ti: TaskInstance = kwargs['ti']
        ti.xcom_push(GridTelescope.TOPIC_NAME, msgs_out, execution_date)

        return GridTelescope.TASK_ID_DOWNLOAD if msgs_out else GridTelescope.TASK_ID_STOP

    @staticmethod
    def download(**kwargs):
        """ Task to download the GRID releases for a given month """

        # Pull messages
        ti: TaskInstance = kwargs['ti']
        msgs_in = ti.xcom_pull(key=GridTelescope.TOPIC_NAME, task_ids=GridTelescope.TASK_ID_LIST,
                               include_prior_dates=False)

        # Prepare paths
        grid_download_path = telescope_path(GridTelescope.DAG_ID, SubFolder.downloaded)

        # Download and extract each release posted this month
        msgs_out = []
        for msg_in in msgs_in:
            # Download release
            article_id = msg_in['article_id']
            title = msg_in['title']
            files = download_grid_release(grid_download_path, article_id, title)

            # Prepare metadata
            for file_path in files:
                msg_out = dict()
                msg_out['download_path'] = file_path
                msgs_out.append(msg_out)

        # Push the selected GRID releases to an XCOM so that the next task knows which releases to download
        ti.xcom_push(GridTelescope.TOPIC_NAME, msgs_out, kwargs['execution_date'])

    @staticmethod
    def extract(**kwargs):
        """ Task to extract the GRID releases for a given month """

        # Pull messages
        ti: TaskInstance = kwargs['ti']
        msgs_in = ti.xcom_pull(key=GridTelescope.TOPIC_NAME, task_ids=GridTelescope.TASK_ID_DOWNLOAD,
                               include_prior_dates=False)

        # Prepare paths
        grid_extraction_path = telescope_path(GridTelescope.DAG_ID, SubFolder.extracted)

        # Download and extract each release posted this month
        msgs_out = []
        for msg_in in msgs_in:
            # Extract release
            download_path = msg_in['download_path']

            release_extracted_path = extract_grid_release(download_path, grid_extraction_path)

            msg_out = dict()
            msg_out['extracted_path'] = release_extracted_path
            msgs_out.append(msg_out)

        # Push the selected GRID releases to an XCOM so that the next task knows which releases to download
        ti.xcom_push(GridTelescope.TOPIC_NAME, msgs_out, kwargs['execution_date'])

    @staticmethod
    def transform(**kwargs):
        """ Task to transform the GRID releases for a given month """

        # Pull GRID releases to transform
        ti: TaskInstance = kwargs['ti']
        msgs_in = ti.xcom_pull(key=GridTelescope.TOPIC_NAME, task_ids=GridTelescope.TASK_ID_EXTRACT,
                               include_prior_dates=False)

        # Prepare paths
        grid_transformed_path = telescope_path(GridTelescope.DAG_ID, SubFolder.transformed)

        # Transform each release
        msgs_out = []
        for msg_in in msgs_in:
            extracted_path = msg_in['extracted_path']
            json_files = glob.glob(f"{extracted_path}/*.json")

            # Only process JSON files, skip the rest
            if len(json_files) > 0:
                release_json_file = json_files[0]
                logging.info(f'Transforming file: {release_json_file}')

                version, file_name, file_path = transform_grid_release(release_json_file, grid_transformed_path)

                # Prepare messages
                msg_out = dict()
                msg_out['version'] = version
                msg_out['json_gz_file_name'] = file_name
                msg_out['json_gz_file_path'] = file_path
                msgs_out.append(msg_out)
            else:
                logging.info(f"Skipping transforming as no JSON files in extracted path: {extracted_path}")

        # Push messages for next task to consume
        ti: TaskInstance = kwargs['ti']
        ti.xcom_push(GridTelescope.TOPIC_NAME, msgs_out, kwargs['execution_date'])

    @staticmethod
    def upload(**kwargs):
        """ Task to upload the transformed GRID releases for a given month to Google Cloud Storage """

        # Pull messages
        ti: TaskInstance = kwargs['ti']
        msgs_in = ti.xcom_pull(key=GridTelescope.TOPIC_NAME, task_ids=GridTelescope.TASK_ID_TRANSFORM,
                               include_prior_dates=False)

        # Upload each release
        config = ObservatoryConfig.load(os.environ['CONFIG_PATH'])
        bucket_name = config.bucket_name
        msgs_out = []
        for msg_in in msgs_in:
            file_name = msg_in['json_gz_file_name']
            file_path = msg_in['json_gz_file_path']

            # Upload to cloud storage
            release_date = file_name.replace("grid_release_", "").replace(".jsonl.gz", "")
            blob_name = f'telescopes/grid/transformed/{file_name}'
            upload_file_to_cloud_storage(bucket_name, blob_name, file_path=file_path)

            # Prepare metadata
            msg_out = dict()
            msg_out['release_date'] = release_date
            msg_out['blob_name'] = blob_name
            msgs_out.append(msg_out)

        # Push messages for next task to consume
        ti: TaskInstance = kwargs['ti']
        ti.xcom_push(GridTelescope.TOPIC_NAME, msgs_out, kwargs['execution_date'])

    @staticmethod
    def db_load(**kwargs):
        """ Task to load the transformed GRID releases for a given month to BigQuery """
        ti: TaskInstance = kwargs['ti']
        msgs_in = ti.xcom_pull(key=GridTelescope.TOPIC_NAME, task_ids=GridTelescope.TASK_ID_UPLOAD,
                               include_prior_dates=False)

        # Upload each release
        config = ObservatoryConfig.load(os.environ['CONFIG_PATH'])
        project_id = config.project_id
        location = config.location
        bucket_name = config.bucket_name

        # Create dataset
        dataset_id = GridTelescope.DAG_ID
        create_bigquery_dataset(project_id, dataset_id, location, GridTelescope.DESCRIPTION)

        analysis_schema_path = schema_path('telescopes')
        table_name = 'grid'

        # Load into BigQuery table
        for msg_in in msgs_in:
            release_date_str = msg_in['release_date']
            release_date = pendulum.parse(release_date_str.replace("_", "-"))
            blob_name = msg_in['blob_name']

            # Select schema file based on release date
            schema_file_path = find_schema(analysis_schema_path, table_name, release_date)
            if schema_file_path is None:
                logging.error(f'No schema found with search parameters: analysis_schema_path={analysis_schema_path}, '
                              f'table_name={table_name}, release_date={release_date}')
                exit(os.EX_CONFIG)

            # Create table id
            table_id = bigquery_partitioned_table_id(GridTelescope.DAG_ID, release_date)

            # Load BigQuery table
            uri = f"gs://{bucket_name}/{blob_name}"
            logging.info(f"URI: {uri}")
            load_bigquery_table(uri, dataset_id, location, table_id, schema_file_path,
                                SourceFormat.NEWLINE_DELIMITED_JSON)
