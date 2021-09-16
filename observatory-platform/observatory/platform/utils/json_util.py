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

# Author: James Diprose, Tuan Chien

import csv
import json
from typing import List


def to_json_lines(items: List[dict], serialize_custom_types_func=None) -> str:
    """Coverts a list of dictionary objects into a JSON line formatted string.

    :param items: a list of dictionaries.
    :param serialize_custom_types_func: a function that serialized types not supported by JSON by default into strings.
    :return: a JSON line formatted string.
    """

    json_records = []

    for item in items:
        json_records.append(json.dumps(item, default=serialize_custom_types_func))
        json_records.append("\n")

    json_lines = "".join(json_records)

    return json_lines


def csv_to_jsonlines(*, csv_file: str, jsonl_file: str):
    """Convert CSV file to a json lines file.

    :param csv_file: File path to csv file.
    :param jsonl_file: File path to output jsonl file.
    """

    json_lines = []

    with open(csv_file, "r") as f:
        reader = csv.DictReader(f)

        for line in reader:
            json_line = json.dumps(line) + "\n"
            json_lines.append(json_line)

    with open(jsonl_file, "w") as f:
        f.writelines(json_lines)
