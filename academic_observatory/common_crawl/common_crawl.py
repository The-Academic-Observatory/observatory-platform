#
# Copyright 2019 Curtin University. All rights reserved.
#
# Author: James Diprose
#


import datetime
import gzip
import logging
import os
import pathlib
from typing import List

from academic_observatory.common_crawl.schema import PageInfo
from academic_observatory.utils import ao_home, to_json_lines


def common_crawl_path() -> str:
    """ Get the default path to the Common Crawl dataset.

    :return: the default path to the Common Crawl dataset.
    """

    return ao_home('datasets', 'common_crawl')


def common_crawl_serialize_custom_types(obj) -> str:
    """ Serialize custom objects for Common Crawl.

    :param obj: an object to serialize.
    :return: the serialized object.
    """

    if isinstance(obj, datetime.datetime):
        result = obj.isoformat()
    elif isinstance(obj, datetime.date):
        result = obj.strftime("%Y-%m-%d")
    else:
        msg = f"serialize_custom_types: Object of type {type(obj)} is not JSON serializable: {str(obj)}"
        logging.error(msg)
        raise TypeError(msg)

    return result


def save_page_infos(page_infos: List[PageInfo], output_path: str, table_name: str, start_time: datetime.datetime,
                    grid_id: str, fetch_month: datetime.date, batch_number: int) -> str:
    """ Save a list of PageInfo objects to newline delimited JSON to disk.

    :param page_infos: the list of PageInfo objects.
    :param output_path: the output directory to save the objects.
    :param table_name: the name of the BigQuery table.
    :param start_time: the time that harvesting started.
    :param grid_id: the GRID id that the data belongs to.
    :param fetch_month: the month that is being fetched.
    :param batch_number: the batch number.
    :return: the path of the saved file.
    """

    items = [page_info.to_dict() for page_info in page_infos]
    data = to_json_lines(items, serialize_custom_types_func=common_crawl_serialize_custom_types)
    time_str = start_time.isoformat()
    fetch_month_str = fetch_month.strftime("%Y-%m")
    base_path = os.path.join(output_path, f'grid-cc-index-content/{table_name}/{time_str}/{grid_id}/{fetch_month_str}')
    pathlib.Path(base_path).mkdir(parents=True, exist_ok=True)
    save_path = os.path.join(base_path, f"{batch_number}.json.gzip")

    with gzip.open(save_path, 'wb') as f:
        f.write(data.encode("utf-8"))  # gzip.compress() if to variable

    return save_path
