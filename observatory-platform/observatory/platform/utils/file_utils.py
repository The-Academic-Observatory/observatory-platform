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

# Author: James Diprose

"""
File utility functions
"""

import codecs
import hashlib
import os
import re
import subprocess
from subprocess import Popen
from typing import List

from google_crc32c import Checksum as Crc32cChecksum
from observatory.platform.utils.proc_utils import wait_for_process


def list_files(path: str, regex: str = None) -> List[str]:
    """ Recursively list files on a path. Optionally only return files that match a regular expression.
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


def _hash_file(fpath, algorithm='sha256', chunk_size=65535):
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
    if (algorithm == 'sha256') or (algorithm == 'auto' and len(hash) == 64):
        hasher = hashlib.sha256()
    else:
        hasher = hashlib.md5()

    with open(fpath, 'rb') as fpath_file:
        for chunk in iter(lambda: fpath_file.read(chunk_size), b''):
            hasher.update(chunk)

    return hasher.hexdigest()


def validate_file(fpath, file_hash, algorithm='auto', chunk_size=65535):
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
    if ((algorithm == 'sha256') or (algorithm == 'auto' and len(file_hash) == 64)):
        hasher = 'sha256'
    else:
        hasher = 'md5'

    if str(_hash_file(fpath, hasher, chunk_size)) == str(file_hash):
        return True
    else:
        return False


def gzip_file_crc(file_path: str) -> str:
    """ Get the crc of a gzip file.

    :param file_path: the path to the file.
    :return: the crc.
    """

    proc: Popen = subprocess.Popen(['gzip', '-vl', file_path], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    output, error = wait_for_process(proc)
    return output.splitlines()[1].split(' ')[1].strip()


def hex_to_base64_str(hex_str: bytes) -> str:
    """ Covert a hexadecimal string into a base64 encoded string. Removes trailing newline character.

    :param hex_str: the hexadecimal encoded string.
    :return: the base64 encoded string.
    """

    string = codecs.decode(hex_str, 'hex')
    base64 = codecs.encode(string, 'base64')
    return base64.decode('utf8').rstrip('\n')


def crc32c_base64_hash(file_path: str, chunk_size: int = 8 * 1024) -> str:
    """ Create a base64 crc32c checksum of a file.

    :param file_path: the path to the file.
    :param chunk_size: the size of each chunk to check.
    :return: the checksum.
    """

    hash_alg = Crc32cChecksum()

    with open(file_path, 'rb') as f:
        chunk = f.read(chunk_size)
        while chunk:
            hash_alg.update(chunk)
            chunk = f.read(chunk_size)
    return hex_to_base64_str(hash_alg.hexdigest())
