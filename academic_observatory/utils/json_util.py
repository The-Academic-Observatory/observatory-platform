import json
from typing import List


def to_json_lines(items: List[dict], serialize_custom_types_func=None) -> str:
    """ Coverts a list of dictionary objects into a JSON line formatted string.

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
