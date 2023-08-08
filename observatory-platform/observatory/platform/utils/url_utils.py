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

# Author: James Diprose, Keegan Smith

import cgi
import json
import logging
import os
import urllib.error
import urllib.request
from datetime import datetime
from email.utils import parsedate_to_datetime
from typing import Dict, List, Tuple, Union, Optional

import pytz
import requests
import time
import xmltodict
from airflow import AirflowException
from importlib.metadata import metadata
from requests.adapters import HTTPAdapter
from tenacity import Retrying, stop_after_attempt, before_sleep_log, wait_exponential_jitter
from tenacity.wait import wait_base, wait_fixed
from urllib3.util.retry import Retry


def parse_retry_after(retry_after: Optional[str]) -> Optional[int]:
    """Parse the Retry-After header. Can be a delay in seconds or a datetime.

    :param retry_after: the Retry-After header.
    :return: returns the number of seconds to wait for.
    """

    delay = None
    if retry_after is not None:
        try:
            delay = int(retry_after)
        except ValueError:
            retry_date = parsedate_to_datetime(retry_after)
            delay = max(0.0, (retry_date - datetime.now(pytz.utc)).total_seconds())
    return delay


def retry_get_url(
    url: str,
    num_retries=3,
    wait: wait_base = wait_exponential_jitter(initial=5, max=300),
    squelch_url: bool = False,
    **kwargs,
):
    """Attempts an HTTP GET request inside a retry loop.
    The request will be retried the specified number of times if the request fails

    :param url: The URL to hit for the GET method
    :param num_retries: The number of times to retry the request before reraising the error
    :param wait: The Tenacity wait function. If unsupplied, use an exponential jitter wait
    :param squelch_url: Whether to hide the URL from logs
    :param kwargs: Keyword args for requests.get()
    :raises requests.exceptions.RequestException: Raised when the number of retries are exhausted
    """
    log_url = "***" if squelch_url else url
    logger = logging.getLogger(__name__)
    logging.basicConfig(level=logging.INFO)
    retrier = Retrying(
        stop=stop_after_attempt(num_retries),
        wait=wait,
        reraise=True,
        before_sleep=before_sleep_log(logger, logging.INFO),
    )
    prev_wait = None
    for attempt in retrier:
        # Set the function that Tenacity thinks it's retrying. This fixes the name in the logs
        attempt.retry_state.fn = retry_get_url

        # Set wait back to original wait function after 429 error handling
        if prev_wait is not None:
            retrier.wait = prev_wait
            prev_wait = None

        with attempt:
            try:
                response = None
                response = requests.get(url, **kwargs)
                response.raise_for_status()
            except (requests.exceptions.ReadTimeout, requests.exceptions.HTTPError) as e:
                log = f"Error getting url: {log_url} | "

                if isinstance(e, requests.exceptions.HTTPError):
                    # Timeout error doesn't result in response, but HTTPError does
                    log += f"Got response code: {response.status_code} | Reason: {response.reason} | "

                    # Handle HTTP 429 error
                    if response.status_code == 429:
                        delay = parse_retry_after(response.headers.get("Retry-After"))
                        if delay is not None:
                            prev_wait = retrier.wait
                            retrier.wait = wait_fixed(delay)
                            log += f"Retry-After header detected, sleeping for: {delay} seconds"

                log += f"Attempt: {attempt.retry_state.attempt_number} | Idle for: {attempt.retry_state.idle_for}"

                # Using e.__class__ logs the specific name of the exception
                raise e.__class__(log)
    return response


def retry_session(
    num_retries: int = 3, backoff_factor: float = 0.5, status_forcelist: Tuple = (500, 502, 503)
) -> requests.Session:
    """Create a session object that is able to retry a given number of times.

    :param num_retries: the number of times to retry.
    :param backoff_factor: the factor to use for determining how long to wait before retrying. See
    urllib3.util.retry.Retry for more details.
    :param status_forcelist: which integer status codes (that would normally not trigger a retry) should
    trigger a retry.
    :return: the session.
    """
    session = requests.Session()
    retry = Retry(
        total=num_retries,
        connect=num_retries,
        read=num_retries,
        redirect=num_retries,
        status_forcelist=status_forcelist,
        backoff_factor=backoff_factor,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def get_http_text_response(url: str) -> str:
    """Get the text response from an HTTP GET call.

    :param url: URL to query.
    :return: Text response.
    """

    HTTP_OK = 200
    response = retry_session().get(url)
    if response.status_code != HTTP_OK:
        raise ConnectionError(f"Error requesting {url}. Status: {response.status_code}")

    return response.text


def get_http_response_json(url: str) -> Union[List, Dict]:
    """Get the JSON response from an HTTP API call as a dict.

    :param url: URL to query.
    :return: Dictionary of the response.
    """

    response = get_http_text_response(url)
    response_obj = json.loads(response)
    return response_obj


def get_http_response_xml_to_dict(url: str) -> Dict:
    """Get the XML response from an HTTP API call as a dict.

    :param url: URL to query.
    :return: Dictionary of the response.
    """

    response = get_http_text_response(url)
    response_dict = xmltodict.parse(response)
    return response_dict


def wait_for_url(url: str, timeout: int = 60):
    """Wait for a URL to return a 200 status code.

    :param url: the url to wait for.
    :param timeout: the number of seconds to wait before timing out.
    :return: whether the URL returned a 200 status code or not.
    """

    start = time.time()
    started = False
    while True:
        duration = time.time() - start
        if duration >= timeout:
            break

        try:
            if urllib.request.urlopen(url).getcode() == 200:
                started = True
                break
            time.sleep(0.5)
        except ConnectionResetError:
            pass
        except ConnectionRefusedError:
            pass
        except urllib.error.URLError:
            pass

    return started


def get_user_agent(*, package_name: str) -> str:
    """Return a standardised user agent that can be used by custom web clients to indicate which Python
    package they came from.

    :return: User agent string.
    """

    pkg_info = metadata(package_name)
    version = pkg_info.get("Version")
    url = pkg_info.get("Home-page")
    mailto = pkg_info.get("Author-email")
    ua = f"{package_name} v{version} (+{url}; mailto: {mailto})"

    return ua


def get_observatory_http_header(*, package_name: str) -> Dict:
    """Construct a HTTP header with a custom user agent.

    :param package_name: Package name used to fetch metadata for the User Agent.
    """

    user_agent = get_user_agent(package_name=package_name)
    header = {"User-Agent": user_agent}
    return header


def get_filename_from_url(url: str) -> str:
    """Given a download url with the filename part of the url, extract the filename.

    :param url: URL to parse.
    :return: Filename.
    """

    dst_file = os.path.basename(url)
    tail = dst_file.find("?")  # Remove any parameters
    if tail < 0:
        tail = len(dst_file)
    dst_file = dst_file[:tail]

    return dst_file


def get_filename_from_http_header(url: str) -> str:
    """Get a download filename from the Content-Disposition header.

    :param url: Download url.
    :return: Filename.
    """

    response = requests.head(url)
    if response.status_code != 200:
        raise AirflowException(f"get_filename_from_http_header: url={response.url}, status_code={response.status_code}")
    header = response.headers["Content-Disposition"]
    value, params = cgi.parse_header(header)
    return params.get("filename")
