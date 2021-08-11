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
