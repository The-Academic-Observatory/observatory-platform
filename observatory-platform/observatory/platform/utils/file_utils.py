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

"""
File utility functions
"""

import codecs
import gzip
import hashlib
import io
import logging
import os
import re
import shutil
import subprocess
from functools import partial
from pathlib import Path
from subprocess import Popen
from typing import List

import json_lines
import jsonlines
import numpy as np
import pandas as pd
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


def _hash_file(fpath, algorithm="sha256", chunk_size=65535):
    """Calculates a file sha256 or md5 hash.

    # Example

    ```python
        >>> from keras.utils.data_utils import _hash_file
        >>> _hash_file('/path/to/file.zip')
        'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855'
    ```

    # Arguments
        fpath: path to the file being validated
        algorithm: hash algorithm, one of 'auto', 'sha256', or 'md5'.
            The default 'auto' detects the hash algorithm in use.
        chunk_size: Bytes to read at a time, important for large files.

    # Returns
        The file hash
    """
    if (algorithm == "sha256") or (algorithm == "auto" and len(hash) == 64):
        hasher = hashlib.sha256()
    else:
        hasher = hashlib.md5()

    with open(fpath, "rb") as fpath_file:
        for chunk in iter(lambda: fpath_file.read(chunk_size), b""):
            hasher.update(chunk)

    return hasher.hexdigest()


def validate_file(fpath, file_hash, algorithm="auto", chunk_size=65535):
    """Validates a file against a sha256 or md5 hash.
    # Arguments
        fpath: path to the file being validated
        file_hash:  The expected hash string of the file.
            The sha256 and md5 hash algorithms are both supported.
        algorithm: Hash algorithm, one of 'auto', 'sha256', or 'md5'.
            The default 'auto' detects the hash algorithm in use.
        chunk_size: Bytes to read at a time, important for large files.
    # Returns
        Whether the file is valid
    """
    if (algorithm == "sha256") or (algorithm == "auto" and len(file_hash) == 64):
        hasher = "sha256"
    else:
        hasher = "md5"

    if str(_hash_file(fpath, hasher, chunk_size)) == str(file_hash):
        return True
    else:
        return False


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


def list_to_jsonl_gz(file_path: str, list_of_dicts: List[dict]):
    """Takes a list of dictionaries and writes this to a gzipped jsonl file.
    :param file_path: Path to the .jsonl.gz file
    :param list_of_dicts: A list containing dictionaries that can be written out with jsonlines
    :return: None.
    """
    with io.BytesIO() as bytes_io:
        with gzip.GzipFile(fileobj=bytes_io, mode="w") as gzip_file:
            with jsonlines.Writer(gzip_file) as writer:
                writer.write_all(list_of_dicts)

        with open(file_path, "wb") as jsonl_gzip_file:
            jsonl_gzip_file.write(bytes_io.getvalue())


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


def zip_files(file_list: List[str]):
    """GZip up the list of files.

    :param file_list: List of files to zip up.
    :return: List of zipped up file names.
    """

    zip_list = list()
    for file_path in file_list:
        logging.info(f"Zipping file {file_path}")
        zip_file = f"{file_path}.gz"
        zip_list.append(zip_file)
        with open(file_path, "rb") as f_in:
            with gzip.open(zip_file, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)

    return zip_list
