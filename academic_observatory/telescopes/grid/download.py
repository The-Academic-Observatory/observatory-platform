# Copyright 2019 Curtin University
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

import json
import logging
import os
from multiprocessing import cpu_count
from typing import List, Union
from typing import Tuple
from zipfile import ZipFile, BadZipFile

import ray

from academic_observatory.telescopes.grid.grid import grid_path
from academic_observatory.utils import retry_session, get_file, wait_for_tasks

GRID_DATASET_URL = "https://api.figshare.com/v2/collections/3812929/articles?page_size=1000"
GRID_FILE_URL = "https://api.figshare.com/v2/articles/{article_id}/files"


@ray.remote
def download_grid_release(output: Union[str, None], article_id: str, title: str, timeout: float) \
        -> List[Tuple[str, bool]]:
    """ Downloads an individual GRID release from Figshare.

    :param output: the output directory where the GRID dataset should be saved.
    :param article_id: the Figshare article id of the GRID release.
    :param title: the title of the Figshare article.
    :param timeout: the timeout in seconds when calling the Figshare API.
    :return: the paths on the system of the downloaded files.
    """

    logging.basicConfig(level=logging.INFO)

    response = retry_session().get(GRID_FILE_URL.format(article_id=article_id), timeout=timeout)
    article_files = json.loads(response.text)

    downloads = []
    for i, article_file in enumerate(article_files):
        real_file_name = article_file['name']
        supplied_md5 = article_file['supplied_md5']
        download_url = article_file['download_url']
        file_type = os.path.splitext(real_file_name)[1]

        # Download
        logging.info(f"Downloading file: {real_file_name}, md5: {supplied_md5}, url: {download_url}")
        dir_name = f"{title}-{i}"
        file_name = f"{dir_name}{file_type}"  # The title is used for the filename because they seem to be labelled
        # more reliably than the files
        file_path, updated = get_file(file_name, download_url, md5_hash=supplied_md5, cache_subdir='',
                                      cache_dir=output)

        # Extract zip files, leave other files such as .json and .csv
        unzip_path = os.path.join(os.path.dirname(file_path), dir_name)

        if file_type == ".zip" and (updated or not os.path.exists(unzip_path)):
            logging.info(f"Extracting file: {file_path}")
            try:
                with ZipFile(file_path) as zip_file:
                    zip_file.extractall(unzip_path)
                updated = True
            except BadZipFile:
                logging.error("Not a zip file")
        else:
            logging.info(f"File saved to: {file_path}")

        downloads.append((file_path, updated))

    return downloads


def download_grid_dataset(output: Union[str, None] = None, num_processes: int = cpu_count(), local_mode: bool = False,
                          timeout: float = 10.) \
        -> Tuple[str, bool]:
    """ Download all of the GRID releases from Figshare.

    :param output: the output directory where the results should be saved. If None then the default
    ~/.academic-observatory directory will be used.
    :param num_processes: the number of processes to use.
    :param local_mode: whether to run the processes serially or not.
    :param timeout: the timeout in seconds when calling the Figshare API.
    :return: None.
    """

    logging.basicConfig(level=logging.INFO)
    if not ray.is_initialized():
        ray.init(num_cpus=num_processes, local_mode=local_mode)

    logging.info("Fetching GRID data sources")
    response = retry_session().get(GRID_DATASET_URL, timeout=timeout)
    grid_articles = json.loads(response.text)

    if output is None:
        output = grid_path()

    # Spawn tasks
    logging.info("Spawning GRID release download tasks")
    task_ids = []
    for article in grid_articles:
        article_id = article['id']
        title = article['title']
        task_id = download_grid_release.remote(output, article_id, title, timeout)
        task_ids.append(task_id)

    # Wait for tasks to complete
    results = wait_for_tasks(task_ids)

    # Check if GRID dataset was updated
    grid_dataset_updated = False
    for result in results:
        for path, updated in result:
            if updated:
                grid_dataset_updated = True
                break

    # Get GRID dataset path.
    grid_dataset_path = os.path.dirname(results[0][0][0])
    logging.info(f"Downloading of GRID dataset complete, GRID dataset path: {grid_dataset_path}")

    return grid_dataset_path, grid_dataset_updated
