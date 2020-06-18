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

import io
import json
import logging
import os
from typing import Union

import deprecation
from natsort import natsorted

from academic_observatory.telescopes.grid_old.grid import parse_grid_release, save_grid_index, grid_index_path, \
    grid_path


@deprecation.deprecated(deprecated_in="20.05.0", details="Replaced by academic_observatory.telescopes.grid")
def index_grid_dataset(input: Union[str, None] = None, output: Union[io.FileIO, None] = None):
    """ Create an index from the GRID dataset.

    :param input: the input path that contains the GRID dataset.
    :param output: the path where the GRID index CSV file should be saved.
    :return: None.
    """

    logging.basicConfig(level=logging.INFO)

    file_type = ".json"
    unique_grids = dict()

    # If user supplied no input path use default
    grid_dataset_path = input
    if input is None:
        grid_dataset_path = grid_path()

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
    grid_index_save_path = output
    if output is None:
        grid_index_save_path = grid_index_path()

    data = [val for key, val in unique_grids.items()]
    data = sorted(data, key=lambda item: item[1])
    save_grid_index(grid_index_save_path, data)

    logging.info(f"Saved GRID Index to: {grid_index_save_path}")
