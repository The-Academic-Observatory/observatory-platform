import json
import logging
import os
import pathlib
from zipfile import ZipFile, BadZipFile

import ray

from academic_observatory.grid.grid import GRID_CACHE_SUBDIR
from academic_observatory.utils import retry_session, get_file, wait_for_tasks

GRID_DATASET_URL = "https://api.figshare.com/v2/collections/3812929/articles?page_size=1000"
GRID_FILE_URL = "https://api.figshare.com/v2/articles/{article_id}/files"


@ray.remote
def download_grid_release(article_id: str, title, timeout):
    logging.basicConfig(level=logging.INFO)

    response = retry_session().get(GRID_FILE_URL.format(article_id=article_id), timeout=timeout)
    article_files = json.loads(response.text)

    paths = []
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
        file_path = get_file(file_name, download_url, md5_hash=supplied_md5, cache_subdir=GRID_CACHE_SUBDIR)

        # Extract zip files, leave other files such as .json and .csv
        unzip_path = os.path.join(os.path.dirname(file_path), dir_name)
        if file_type == ".zip":
            logging.info(f"Extracting file: {file_path}")
            try:
                with ZipFile(file_path) as zip_file:
                    zip_file.extractall(unzip_path)
            except BadZipFile:
                logging.error("Not a zip file")
        else:
            logging.info(f"File saved to: {file_path}")

        paths.append(file_path)

    return paths


def download_grid_dataset(args):
    logging.basicConfig(level=logging.INFO)

    ray.init(num_cpus=args.num_processes, local_mode=args.local_mode)

    logging.info("Fetching GRID data sources")
    response = retry_session().get(GRID_DATASET_URL, timeout=args.timeout)
    grid_articles = json.loads(response.text)

    # Spawn tasks
    logging.info("Spawning GRID release download tasks")
    task_ids = []
    for article in grid_articles:
        article_id = article['id']
        title = article['title']
        task_id = download_grid_release.remote(article_id, title, args.timeout)
        task_ids.append(task_id)

    # Wait for tasks to complete
    results = wait_for_tasks(task_ids)

    # Get GRID dataset path.
    grid_dataset_path = pathlib.Path(results[0][0]).parent
    logging.info(f"Downloading of GRID dataset complete, GRID dataset path: {grid_dataset_path}")
