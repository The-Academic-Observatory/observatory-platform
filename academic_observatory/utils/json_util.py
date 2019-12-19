import datetime
import json
import logging
from typing import List


def serialize_custom_types(obj) -> str:
    if isinstance(obj, datetime.datetime):
        result = obj.strftime("%Y-%m-%dT%H:%M:%S")
    else:
        result = str(obj)
        logging.error(f"serialize_custom_types: Object of type {type(obj)} is not JSON serializable: {result}")

    return result


def to_json_lines(items: List[dict]) -> str:
    """ Coverts a list of dictionary objects into a JSON line formatted string.

    :param items: a list of dictionaries.
    :return: a JSON line formatted string.
    """

    json_records = []

    for item in items:
        json_records.append(json.dumps(item, default=serialize_custom_types))
        json_records.append("\n")

    json_lines = "".join(json_records)

    return json_lines
