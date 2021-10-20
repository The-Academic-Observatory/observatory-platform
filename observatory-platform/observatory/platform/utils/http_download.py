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

# Author: Tuan Chien

# Asynchronous HTTP GET file downloader. Use the download_file, and download_files interfaces to download.
# Creates a worker pool to asynchronously (single thread) download file(s).
# Valid hash algorithms: see observatory.platform.utils.file_utils.get_hasher_ for valid options.
#
# Usage examples:
#    custom_headers = {"User-Agent" : "Something" }
#  - download_file(url=url)
#  - download_file(url=url, filename="destination", headers=custom_headers)
#  - download_file(url=url, filename="destination", hash="d733dac9083babc757972353d75401ed", hash_algorithm="md5")
#
#  - download_files(download_list=["http://url1/file", "http://url2/file"])
#    download_list=[
#      {"url": "http://myurl/file", "filename": "dst_file.txt"},
#      {"url": "http://myurl2/file", "filename": "dst_file2.txt", "hash": "d733dac9083babc757972353d75401ed", "hash_algorithm" : "md5"},
#    ]
#  - download_files(download_list=download_list, num_connections=4, headers=custom_headers)
#
# Supports:
#  - Retrying downloads a few times in case of temporary failures.
#  - Retrying once if a supplied hash does not match the download.
#  - Skipping downloads if the file exists, and the hash matches the one supplied.

import asyncio
import logging
import os
from cgi import parse_header
from dataclasses import dataclass
from queue import Queue
from typing import Any, Dict, List, Union

import aiohttp
import backoff
import validators
from observatory.platform.utils.file_utils import validate_file_hash
from observatory.platform.utils.url_utils import get_filename_from_url


@dataclass
class DownloadInfo:
    """Metadata needed to download a file."""

    url: str  # URL to download
    filename: Union[str, None] = None  # Destination file
    hash: Union[str, None] = None  # Hash code for file integrity checking
    hash_algorithm: Union[str, None] = None  # Hash algorithm for the hash
    prefix_dir: str = ""  # Prefix the filename path with this
    retry: bool = False  # Whether to retry download


@backoff.on_exception(backoff.expo, aiohttp.ClientError, max_tries=11, max_time=60)
async def download_http_file_(*, download_info: DownloadInfo, headers=None):
    """Download a single file from a HTTP GET request.

    :param download_info: Download information.
    :param headers: Optional headers to use when making get request, e.g., if providing custom User Agent.
    """

    READ_BUFFER_SIZE = 2 ** 16  # 64 KiB

    url = download_info.url
    dst_file = download_info.filename

    async with aiohttp.ClientSession(raise_for_status=True, headers=headers) as session:
        async with session.get(url, timeout=None) as resp:
            if dst_file is None:
                _, params = parse_header(resp.headers.get("Content-Disposition", ""))
                if params != "" and "filename" in params:
                    dst_file = params["filename"]
                else:
                    dst_file = get_filename_from_url(url=url)

                download_info.filename = dst_file
            dst_file = os.path.join(download_info.prefix_dir, dst_file)

            logging.info(f"downloading {url} to {dst_file}")

            with open(dst_file, "wb") as f:
                while True:
                    chunk = await resp.content.read(READ_BUFFER_SIZE)
                    if not chunk:
                        break
                    f.write(chunk)


def skip_download(*, download_info: Dict) -> bool:
    """Check whether we should skip the download.  It will check if the file exists and its hash matches a supplied
    hash. If no hash is supplied, we will not skip.

    :param download_info: Download job information.
    :return: Whether we should skip the download for this file.
    """

    hash = download_info.hash
    hash_algorithm = download_info.hash_algorithm
    prefix_dir = download_info.prefix_dir
    filename = download_info.filename

    if filename is None:
        return False

    if prefix_dir != "":
        filename = os.path.join(prefix_dir, filename)

    if hash_algorithm is not None and os.path.exists(filename):
        valid = validate_file_hash(file_path=filename, expected_hash=hash, algorithm=hash_algorithm)
        if valid:
            logging.info(f"File {filename} exists, and has a valid hash file. Skipping download.")
            return True
        else:
            logging.info(f"File {filename} exists, but has a bad hash. Redownloading.")

    return False


def requeue_once_if_bad_hash(
    *, download_info: DownloadInfo, exception: Union[None, Exception], downloads: asyncio.Queue, errors: List[Exception]
):
    """If a hash was supplied, it checks if the file downloaded passes hash validation. If it doesn't, then on the
    first failure, it will requeue the download.  Otherwise, it will produce an error message and give up on that
    download.  If an exception was raised during download, it will not requeue.

    :param download_info: Download job information.
    :param exception: Whether an exception was raised during download.
    :param downloads: Queue of download job information.
    :param errors: Queue of encountered error objects for later processing.
    """

    hash = download_info.hash
    hash_algorithm = download_info.hash_algorithm
    prefix_dir = download_info.prefix_dir
    filename = download_info.filename

    if filename is None:
        return

    if prefix_dir != "":
        filename = os.path.join(prefix_dir, filename)

    if hash_algorithm is not None and exception is None and os.path.exists(filename):
        valid = validate_file_hash(file_path=filename, expected_hash=hash, algorithm=hash_algorithm)

        # Requeue download the first time there's a bad hash.
        if not valid and download_info.retry == False:
            warning_msg = f"File {filename} and hash {hash} do not match. Retrying download."
            logging.warning(warning_msg)
            download_info.retry = True
            downloads.put_nowait(download_info)

        # Error out if this is not the first attempt.
        elif not valid and download_info.retry:
            error_msg = f"File {filename} has a bad hash after retrying download. Giving up."
            errors.append(Exception(error_msg))


async def worker_(name: str, downloads: asyncio.Queue, errors: List[Exception], headers: Union[None, Dict]):
    """Worker that fetches download jobs and executes downloads.

    :param name: Name of the worker.
    :param downloads: Download queue of jobs.
    :param errors: List of encountered download errors.
    :param headers: Custom HTTP headers to use.
    """

    logging.info(f"Starting worker {name}")
    while True:
        # If there are errors, terminate the worker thread.
        if len(errors) > 0:
            break

        download_info = await downloads.get()

        # Skip if file exists and has good hash
        if skip_download(download_info=download_info):
            downloads.task_done()
            continue

        # Download the file
        url = download_info.url
        logging.info(f"{name}: downloading {url}")

        exception = None
        try:
            await download_http_file_(download_info=download_info, headers=headers)
        except Exception as e:
            errors.append(e)
            exception = e

        # Requeue on bad hash once.
        requeue_once_if_bad_hash(download_info=download_info, exception=exception, downloads=downloads, errors=errors)

        downloads.task_done()


async def download_http_files_(
    *,
    download_list: List[DownloadInfo],
    num_connections: str = 8,
    headers: Dict = None,
) -> bool:
    """Download a list of files via HTTP asynchronously.  Supports multiple connections
    and custom headers.

    :param download_list: List of files to download.
    :param num_connections: Maximum number of concurrent connections to use.
    :param headers: Custom headers to use in HTTP GET request.
    :return: True on success, False on failure.
    """

    downloads = asyncio.Queue()
    errors = list()

    # Load jobs in queue
    for task in download_list:
        downloads.put_nowait(task)

    # Create workers
    workers = list()
    num_workers = min(num_connections, len(download_list))
    for i in range(num_workers):
        name = f"{i}"
        worker = asyncio.create_task(worker_(name, downloads, errors, headers))
        workers.append(worker)

    # Block until all jobs done
    await downloads.join()

    # Stop all workers
    for worker in workers:
        worker.cancel()

    # Block until all workers are cancelled.
    await asyncio.gather(*workers, return_exceptions=True)

    for error in errors:
        logging.error(error)

    success = len(errors) == 0
    return success


def download_files(
    *,
    download_list: List[Union[str, DownloadInfo]],
    num_connections: int = 8,
    headers: Dict = None,
    prefix_dir: str = "",
) -> bool:
    """Download a list of files. Can support simultaneous connections and custom HTTP headers.

    :param download_list: List of download jobs. You can specify a list of url strings, or a dictionary of the form
    {"url": "urlstring", "filename": "savedfile", "hash" : "hashcode", "hash_algorithm": "hash algorithm"}
    :param num_connections: Maximum number of concurrent connections.
    :param headers: Custom HTTP header to use for downloading.
    :param prefix_dir: A directory to prefix the filename path with. If specified and the download_list is a list of DownloadInfo, it overrides the prefix_dir in each DownloadInfo.
    :return: True on sucess, False on failure.
    """

    if len(download_list) == 0:
        return

    # Convert list of urls to download info dict.
    for i, info in enumerate(download_list):
        if isinstance(info, str):
            info = DownloadInfo(url=info, prefix_dir=prefix_dir)
            download_list[i] = info
        elif isinstance(info, DownloadInfo):
            if prefix_dir != "":
                info.prefix_dir = prefix_dir
        else:
            raise Exception(f"Expecting a DownloadInfo object. Received a {type(info)}.")

    success = asyncio.run(
        download_http_files_(download_list=download_list, num_connections=num_connections, headers=headers),
        debug=True,
    )
    return success


def download_file(
    *,
    url: str,
    filename: str = None,
    headers: Dict = None,
    hash: str = None,
    hash_algorithm: str = None,
    prefix_dir: str = "",
) -> bool:
    """Download a single file from a url.

    :param url: URL to download file from.
    :param filename: Destination file.
    :param headers: Any custom header you want to use in HTTP session.
    :param prefix_dir: A directory to prefix the filename path with.
    :return: True on sucess, False on failure.
    """

    download_info = DownloadInfo(
        url=url,
        filename=filename,
        prefix_dir=prefix_dir,
    )

    if hash_algorithm is not None:
        download_info.hash = hash
        download_info.hash_algorithm = hash_algorithm

    success = download_files(download_list=[download_info], num_connections=1, headers=headers)
    return success
