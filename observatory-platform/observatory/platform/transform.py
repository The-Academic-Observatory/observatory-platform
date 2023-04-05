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

# Author: Aniek Roelofs, James Diprose

import json
import os
import pathlib
import re
from datetime import datetime
from typing import Any, List
from typing import BinaryIO

import zlib
from google.cloud import bigquery

from observatory.platform.files import yield_jsonl


def merge_update_files(*, primary_key: str, input_files: List[str], output_file: str):
    """Merge the transformed jsonl into a single jsonl and delete the parts.

    This means that if the DOI appears in multiple update files, only the instance from the last change file will be
    written into the output file.

    :param primary_key: the primary key to merge on.
    :param input_files: a list of input file paths. The input file paths must be in the correct order.
    :param output_file: the output file to save the data to.
    :return: None.
    """

    # Create mapping
    update_data = {}
    for i, input_file in enumerate(input_files):
        for row in yield_jsonl(input_file):
            update_data[row[primary_key]] = i

    # Write out merged file
    with open(output_file, "w") as out_file:
        for i, input_file in enumerate(input_files):
            with open(input_file, "r") as in_file:
                for line in in_file:
                    row = json.loads(line)
                    if update_data[row[primary_key]] == i:
                        out_file.write(line)

    # Delete original parts
    for file in input_files:
        pathlib.Path(file).unlink()


def convert(k: str) -> str:
    """Convert a key name.
    BigQuery specification for field names: Fields must contain only letters, numbers, and underscores, start with a
    letter or underscore, and be at most 128 characters long.
    :param k: Key.
    :return: Converted key.
    """
    # Trim special characters at start:
    k = re.sub("^[^A-Za-z0-9]+", "", k)
    # Replace other special characters (except '_') in remaining string:
    k = re.sub(r"\W+", "_", k)
    return k


def change_keys(obj, convert):
    """Recursively goes through the dictionary obj and replaces keys with the convert function.
    :param obj: Dictionary object.
    :param convert: Convert function.
    :return: Updated dictionary object.
    """
    if isinstance(obj, (str, int, float)):
        return obj
    if isinstance(obj, dict):
        new = obj.__class__()
        for k, v in list(obj.items()):
            new[convert(k)] = change_keys(v, convert)
    elif isinstance(obj, (list, set, tuple)):
        new = obj.__class__(change_keys(v, convert) for v in obj)
    else:
        return obj
    return new


def add_partition_date(
    list_of_dicts: List[dict],
    partition_date: datetime,
    partition_type: bigquery.TimePartitioningType = bigquery.TimePartitioningType.DAY,
    partition_field: str = "snapshot_date",
):
    """Add a partition date key/value pair to each dictionary in the list of dicts.
    Used to load data into a BigQuery partition.

    :param list_of_dicts: List of dictionaries with original data
    :param partition_date: The partition date
    :param partition_type: The partition type
    :param partition_field: The name of the partition field in the BigQuery table
    :return: Updated list of dicts with partition dates
    """
    if partition_type == bigquery.TimePartitioningType.HOUR:
        partition_date = partition_date.isoformat()
    else:
        partition_date = partition_date.strftime("%Y-%m-%d")

    for entry in list_of_dicts:
        entry[partition_field] = partition_date
    return list_of_dicts


def find_replace_file(*, src: str, dst: str, pattern: str, replacement: str):
    """Find an expression (can be a regex) in lines of a text file and replace it with a replacement string.
    You can optionally save the file in a different location.

    :param src: Input file.
    :param dst: Destination file.
    :param pattern: Expression to search for.
    :param replacement: Replacement string.
    """

    with open(src, "r") as f_in:
        with open(dst, "w") as f_out:
            for line in f_in:
                output = re.sub(pattern=pattern, repl=replacement, string=line)
                f_out.write(output)


def get_as_list(base: dict, target):
    """Helper function that returns the target as a list.

    :param base: dictionary to query.
    :param target: target key.
    :return: base[target] as a list (if it isn't already).
    """

    if target not in base:
        return list()

    if not isinstance(base[target], list):
        return [base[target]]

    return base[target]


def get_as_list_or_none(base: dict, key, sub_key):
    """Helper function that returns a list or None if key is missing.

    :param base: dictionary to query.
    :param key: target key.
    :param sub_key: sub_key to target.
    :return: entry or None.
    """

    if key not in base or base[key]["@count"] == "0":
        return None

    return get_as_list(base[key], sub_key)


def get_entry_or_none(base: dict, target, var_type=None):
    """Helper function that returns an entry or None if key is missing.

    :param base: dictionary to query.
    :param target: target key.
    :param var_type: Type of variable this is supposed to be (for casting).
    :return: entry or None.
    """

    if target not in base:
        return None

    if var_type is not None:
        return var_type(base[target])

    return base[target]


def get_chunks(*, input_list: List[Any], chunk_size: int = 8) -> List[Any]:
    """Generator that splits a list into chunks of a fixed size.

    :param input_list: Input list.
    :param chunk_size: Size of chunks.
    :return: The next chunk from the input list.
    """

    n = len(input_list)
    for i in range(0, n, chunk_size):
        yield input_list[i : i + chunk_size]


def split_and_compress(
    input_file: pathlib.Path,
    output_path: pathlib.Path,
    max_output_size: int = int(3.9 * 1024**3),
    input_buffer_size: int = 64 * 1024,
) -> None:
    """Split an input_file into multiple parts and compress each part into a .gz file, making sure that the compressed
    output size is always close to max_output_size. The parts could be up to compressed size of (input_buffer_size + one line)
    over the max_output_size.

    :param input_file: the input file.
    :param output_path: the output path.
    :param max_output_size: the maximum output size. Approx 3.9 GB which is close to the BigQuery maximum gzip compressed file size.
    :param input_buffer_size: read 64KB at a time.
    :return: None.
    """

    with open(input_file, "rb") as in_file:
        part_num = 0
        compressor = zlib.compressobj(wbits=zlib.MAX_WBITS | 16)  # Set gzip format
        file_path = part_file_path(input_file, output_path, part_num)
        out_file = open(file_path, "wb")
        total_output_size = 0

        while True:
            chunk = in_file.read(input_buffer_size)
            if not chunk:
                break

            chunk += read_line_boundary(in_file)
            compressed_size = compress_chunk(chunk, compressor, out_file)
            total_output_size += compressed_size

            if total_output_size >= max_output_size:
                # Finish current output file and reset
                out_file.write(compressor.flush(zlib.Z_FINISH))
                out_file.close()
                compressor = zlib.compressobj(wbits=zlib.MAX_WBITS | 16)

                # Start a new output file
                part_num += 1
                file_path = part_file_path(input_file, output_path, part_num)
                out_file = open(file_path, "wb")
                total_output_size = 0

        out_file.write(compressor.flush(zlib.Z_FINISH))
        out_file.close()


def read_line_boundary(file: BinaryIO) -> bytes:
    """Read a file up until the new line. For split_and_compress.

    :param file: the file
    :return: the bytes.
    """

    partial_line = b""
    while True:
        byte = file.read(1)
        if not byte:
            break
        partial_line += byte
        if byte == b"\n":
            break
    return partial_line


def compress_chunk(chunk: bytes, compressor: Any, file: BinaryIO) -> int:
    """Compress a chunk. For split_and_compress.

    :param chunk: the byte chunk.
    :param compressor: the compressor.
    :param file: the file.
    :return: the total bytes compressed.
    """

    compressed_data = compressor.compress(chunk)
    flushed_data = compressor.flush(zlib.Z_SYNC_FLUSH)
    file.write(compressed_data + flushed_data)
    return len(compressed_data + flushed_data)


def part_file_path(input_file: pathlib.Path, output_path: pathlib.Path, part_num: int):
    """Make a file path. For split_and_compress.

    :param input_file: the input path.
    :param output_path: the output path.
    :param part_num: the part number.
    :return: the file path.
    """

    return os.path.join(output_path, f"{input_file.stem}{part_num:012}{input_file.suffix}.gz")
