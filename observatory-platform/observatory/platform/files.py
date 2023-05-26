# Copyright 2020, 2021 Curtin University
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

# Author: James Diprose, Tuan Chien, Aniek Roelofs


import codecs
import gzip
import hashlib
import io
import json
import logging
import os
import pathlib
import re
import shutil
import subprocess
from _hashlib import HASH
from datetime import datetime
from functools import partial
from pathlib import Path
from subprocess import Popen
from typing import Any, List
from typing import BinaryIO, Dict

import json_lines
import jsonlines
import numpy as np
import pandas as pd
import zlib
from google.cloud import bigquery
from google_crc32c import Checksum as Crc32cChecksum

from observatory.platform.utils.proc_utils import wait_for_process


def list_files(path: str, regex: str = None) -> List[str]:
    """Recursively list files on a path. Optionally only return files that match a regular expression.
    :param path: the path to recursively list files on.
    :param regex: a regular expression, if a file matches, then it is included.
    :return: a list of full paths to the files.
    """

    paths = []
    for root, dirs, files in os.walk(path):
        for file in files:
            if regex is None or re.match(regex, file):
                paths.append(os.path.join(root, file))
    return paths


def get_hasher_(algorithm: str) -> HASH:
    """Get the hasher for the specified algorithm.

    :param algorithm: Algorithm name.  See https://docs.python.org/3.8/library/hashlib.html for supported names.
    :return: Hasher object.
    """

    if algorithm == "md5":
        return hashlib.md5()
    elif algorithm == "sha256":
        return hashlib.sha256()
    elif algorithm == "sha512":
        return hashlib.sha512()

    raise Exception(f"get_hasher_ unsupported algorithm: {algorithm}")


def get_file_hash(*, file_path: str, algorithm: str = "md5") -> str:
    """Get the hash string of the file.

    :param file_path: File to hash.
    :param algorithm: Hashing algorithm to use.
    :return: Hash string.
    """

    hasher = get_hasher_(algorithm)

    BUFFER_SIZE = 2**16  # 64 KiB
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(BUFFER_SIZE), b""):
            hasher.update(chunk)

    hash_code = hasher.hexdigest()
    return hash_code


def validate_file_hash(*, file_path: str, expected_hash: str, algorithm="md5") -> bool:
    """Check whether a file has the correct hash string.

    :param file_path: File to check.
    :param expected_hash: Expected hash string.
    :param algorithm: Hashing algorithm to use.
    :return: Whether the hash is valid.
    """

    computed_hash = get_file_hash(file_path=file_path, algorithm=algorithm)
    return computed_hash == expected_hash


def gzip_file_crc(file_path: str) -> str:
    """Get the crc of a gzip file.

    :param file_path: the path to the file.
    :return: the crc.
    """

    proc: Popen = subprocess.Popen(["gzip", "-vl", file_path], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    output, error = wait_for_process(proc)
    return output.splitlines()[1].split(" ")[1].strip()


def hex_to_base64_str(hex_str: bytes) -> str:
    """Covert a hexadecimal string into a base64 encoded string. Removes trailing newline character.

    :param hex_str: the hexadecimal encoded string.
    :return: the base64 encoded string.
    """

    string = codecs.decode(hex_str, "hex")
    base64 = codecs.encode(string, "base64")
    return base64.decode("utf8").rstrip("\n")


def crc32c_base64_hash(file_path: str, chunk_size: int = 8 * 1024) -> str:
    """Create a base64 crc32c checksum of a file.

    :param file_path: the path to the file.
    :param chunk_size: the size of each chunk to check.
    :return: the checksum.
    """

    hash_alg = Crc32cChecksum()

    with open(file_path, "rb") as f:
        chunk = f.read(chunk_size)
        while chunk:
            hash_alg.update(chunk)
            chunk = f.read(chunk_size)
    return hex_to_base64_str(hash_alg.hexdigest())


def load_csv(file_path: str):
    """Return a CSV file as a list of dictionaries. It will check if the file is gzipped and unzip it if so.

    :param file_path: the path to the CSV file.
    :return: a list.
    """

    return list(yield_csv(file_path))


def is_gzip(file_path: str) -> bool:
    """Return whether a file is a gzip file or not

    :param file_path: the path to the file.
    :return: whether the file is a gzip file or not.
    """

    is_gzip_ = False
    with gzip.open(file_path, "rb") as f:
        try:
            f.read(1)
            is_gzip_ = True
        except OSError:
            pass

    return is_gzip_


def yield_csv(file_path: str):
    """Yield each row of a CSV file as a dictionary. It will check if the file is gzipped and unzip it if so.

    :param file_path: the path to the CSV file.
    :return: a generator.
    """

    if is_gzip(file_path):
        func = partial(gzip.open, file_path, mode="rb")
    else:
        func = partial(open, file_path, mode="r")

    with func() as f:
        df = pd.read_csv(f)
        df = df.replace({np.nan: None})
        for index, row in df.iterrows():
            yield row.to_dict()


def load_jsonl(file_path: str):
    """Return all rows of a JSON lines file as a list of dictionaries. If the file
    is gz compressed then it will be extracted.

    :param file_path: the path to the JSON lines file.
    :return: a list.
    """

    return list(yield_jsonl(file_path))


def yield_jsonl(file_path: str):
    """Return or yield row of a JSON lines file as a dictionary. If the file
    is gz compressed then it will be extracted.

    :param file_path: the path to the JSON lines file.
    :return: generator.
    """

    with json_lines.open(file_path) as file:
        for row in file:
            yield row


def save_jsonl_gz(file_path: str, data: List[Dict]) -> None:
    """Takes a list of dictionaries and writes this to a gzipped jsonl file.
    :param file_path: Path to the .jsonl.gz file
    :param data: a list of dictionaries that can be written out with jsonlines
    :return: None.
    """

    with io.BytesIO() as bytes_io:
        with gzip.GzipFile(fileobj=bytes_io, mode="w") as gzip_file:
            with jsonlines.Writer(gzip_file) as writer:
                writer.write_all(data)

        with open(file_path, "wb") as jsonl_gzip_file:
            jsonl_gzip_file.write(bytes_io.getvalue())


def save_jsonl(file_path: str, data: List[Dict]) -> None:
    """Takes a list of dictionaries and writes this to a jsonl file.
    :param file_path: Path to the .jsonl.gz file
    :param data: a list of dictionaries that can be written out with jsonlines
    :return: None.
    """

    with open(file_path, mode="w") as f:
        with jsonlines.Writer(f) as writer:
            writer.write_all(data)


def load_file(file_name: str, modes="r"):
    """Load a file.

    :param file_name: file to load.
    :param modes: File open modes. Defaults to 'r'
    :return: contents of file.
    """

    with open(file_name, modes) as f:
        return f.read()


def write_to_file(record, file_name: str):
    """Write a structure to file.

    :param record: Structure to write.
    :param file_name: File name to write to.
    """

    directory = os.path.dirname(file_name)
    Path(directory).mkdir(parents=True, exist_ok=True)

    with open(file_name, "w") as f:
        f.write(record)


def gunzip_files(*, file_list: List[str], output_dir: str = None):
    """Gunzip the list of files.

    :param file_list: List of files to unzip.
    :param output_dir: Optional output directory.
    """

    for file_path in file_list:
        if file_path[-3:] != ".gz":  # Skip files without .gz extension
            continue

        logging.info(f"Unzipping file {file_path}")

        if output_dir is None:
            output_dir = os.path.dirname(file_path)

        filename = os.path.basename(file_path)
        filename = filename[:-3]  # Strip .gz extension
        dst = os.path.join(output_dir, filename)

        with gzip.open(file_path, "rb") as f_in:
            with open(dst, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)


def clean_dir(directory: str):
    """Remove all files and folders within a directory, but not the directory itself.

    :param directory: the path to the directory.
    :return: None
    """

    # loop through all files and directories in the directory
    for item in os.listdir(directory):
        # construct the full path to the file or directory
        item_path = os.path.join(directory, item)

        # check if the item is a file (not a directory) and if so, delete it
        if os.path.isfile(item_path):
            os.remove(item_path)

        # if the item is a directory, use shutil.rmtree to remove it and its contents
        elif os.path.isdir(item_path):
            shutil.rmtree(item_path)


def split_file_and_compress(
    input_file: pathlib.Path,
    output_path: pathlib.Path,
    max_output_size: int = int(3.9 * 1024**3),
    input_buffer_size: int = 64 * 1024,
) -> None:
    """Split an input_file into multiple parts and compress each part into a .gz file, making sure that the compressed
    output size is always close to max_output_size. The parts could be up to compressed size of (input_buffer_size + one line)
    over the max_output_size. This function is useful for compressing and splitting data before uploading to Google
    Cloud Storage and loading into BigQuery. Uploading to Cloud Storage is faster and more reliable as the files are smaller
    and can be uploaded in parallel (e.g. 10 minutes vs 4 hours).

    :param input_file: the input file.
    :param output_path: the output path.
    :param max_output_size: the maximum output size. Approx 3.9 GB which is close to the BigQuery maximum gzip compressed file size.
    :param input_buffer_size: read 64KB at a time.
    :return: None.
    """

    part_num = 0
    compressor = zlib.compressobj(wbits=zlib.MAX_WBITS | 16)  # Set gzip format
    file_output_size = 0
    out_file = None
    with open(input_file, "rb") as in_file:
        while True:
            chunk = in_file.read(input_buffer_size)
            if not chunk:
                break
            chunk += read_line_boundary(in_file)

            # If out_file None then create
            if out_file is None:
                file_path = part_file_path(input_file, output_path, part_num, extra_suffix=".gz")
                out_file = open(file_path, "wb")

            compressed_size = compress_chunk(chunk, compressor, out_file)
            file_output_size += compressed_size

            if file_output_size >= max_output_size:
                # Finish current output file and reset
                out_file.write(compressor.flush(zlib.Z_FINISH))
                out_file.close()
                compressor = zlib.compressobj(wbits=zlib.MAX_WBITS | 16)

                # Start a new output file
                part_num += 1
                file_output_size = 0
                out_file = None

        # If out_file not None then we must have exited and not exceeded the max_output_size, so write and close file
        if out_file is not None:
            out_file.write(compressor.flush(zlib.Z_FINISH))
            out_file.close()


def split_file(input_file: pathlib.Path, output_path: pathlib.Path, max_output_size: int = int(30 * 1024**3)):
    """Split an input_file into multiple parts, making sure that the size of each part is always close to max_output_size.
    This function is useful for splitting data before uploading to Google Cloud Storage and loading into BigQuery.
    Uploading to Cloud Storage is faster and more reliable as they can be uploaded in parallel.

    :param input_file: the input file.
    :param output_path: the output path.
    :param max_output_size: the maximum output size. 32 GB.
    :return: None.
    """

    part_num = 0
    file_output_size = 0
    out_file = None
    with open(input_file, "r") as in_file:
        while True:
            line = in_file.readline()
            if not line:
                break

            # If out_file None then create
            if out_file is None:
                file_path = part_file_path(input_file, output_path, part_num)
                out_file = open(file_path, "w")

            # Write data to out_file
            file_output_size += out_file.write(line)

            # When over file_output_size close file and start again
            if file_output_size >= max_output_size:
                # Close this file
                out_file.close()

                # Start a new output file
                part_num += 1
                file_output_size = 0
                out_file = None

        # We must have exited and not exceeded the max_output_size, so close file
        if out_file is not None:
            out_file.close()


def read_line_boundary(file: BinaryIO) -> bytes:
    """Read a file up until the new line. For split_file_and_compress.

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
    """Compress a chunk. For split_file_and_compress.

    :param chunk: the byte chunk.
    :param compressor: the compressor.
    :param file: the file.
    :return: the total bytes compressed.
    """

    compressed_data = compressor.compress(chunk)
    flushed_data = compressor.flush(zlib.Z_SYNC_FLUSH)
    file.write(compressed_data + flushed_data)
    return len(compressed_data + flushed_data)


def part_file_path(input_file: pathlib.Path, output_path: pathlib.Path, part_num: int, extra_suffix: str = ""):
    """Make a file path. For split_file_and_compress.

    :param input_file: the input path.
    :param output_path: the output path.
    :param part_num: the part number.
    :param extra_suffix: an extra suffix e.g. .gz.
    :return: the file path.
    """

    return os.path.join(output_path, f"{input_file.stem}{part_num:012}{input_file.suffix}{extra_suffix}")


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
