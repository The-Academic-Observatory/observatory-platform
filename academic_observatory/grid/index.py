import json
import logging
import os

from natsort import natsorted

from academic_observatory.grid.grid import GRID_CACHE_SUBDIR, parse_grid_release, save_grid
from academic_observatory.utils import get_user_dir


def index_grid_dataset(args):
    logging.basicConfig(level=logging.INFO)

    file_type = ".json"
    grid_index_filename = "grid_index.csv"
    unique_grids = dict()

    # Get default grid dataset path
    cache_dir, cache_subdir, datadir = get_user_dir(cache_subdir=GRID_CACHE_SUBDIR)

    # If user supplied no input path use default
    grid_dataset_path = args.input
    if args.input is None:
        grid_dataset_path = datadir

    # Get paths to all JSON files and sort them. We sort the files so that the oldest release is first and the newest
    # is last so that if there are duplicate entries we always save the latest information. If an older release
    # contains a GRID id that doesn't exist in the latest release then it won't get overwritten.
    paths = []
    for root, dirs, files in os.walk(grid_dataset_path):
        for name in files:
            if name.endswith(file_type):
                grid_json_path = os.path.join(root, name)
                paths.append(grid_json_path)
    paths = natsorted(paths)
    logging.debug(f"Paths: {paths}")

    # Iterate through all JSON files and process them
    for path in paths:
        logging.info(f'Processing file: {path}')

        with open(path) as file:
            # Get the JSON and convert to right format for Athena
            grid_release = json.load(file)
            version, results = parse_grid_release(grid_release)

            logging.debug(f"Num items {path}: {len(results)}")

            for result in results:
                unique_grids[result[0]] = result

            logging.debug(f"Num items after adding {path}: {len(unique_grids)}")

    # If user supplied no output path use default
    grid_index_save_path = args.output
    if args.output is None:
        grid_index_save_path = os.path.join(datadir, grid_index_filename)

    data = [val for key, val in unique_grids.items()]
    data = sorted(data, key=lambda item: item[1])
    save_grid(grid_index_save_path, data, header=True)

    logging.info(f"Saved GRID Index to: {grid_index_save_path}")
