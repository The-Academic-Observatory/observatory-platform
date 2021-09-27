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

import hashlib
import json
import os
import time
import urllib.error
import urllib.request
from typing import Any, Dict, List, Tuple, Union
from urllib.parse import ParseResult, urljoin, urlparse

import requests
import tldextract
import xmltodict
from importlib_metadata import metadata
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


def get_url_domain_suffix(url: str) -> str:
    """Extract a URL composed of the domain name and the suffix of the URL. For example, library.curtin.edu would
    become curtin.edu

    :param url: a URL.
    :return: the domain + . + suffix of the URL.
    """

    result = tldextract.extract(url)
    return f"{result.domain}.{result.suffix}"


def unique_id(string: str) -> str:
    """Generate a unique identifier from a string.

    :param string: a string.
    :return: the unique identifier.
    """
    return hashlib.md5(string.encode("utf-8")).hexdigest()


def is_url_absolute(url: Union[str, ParseResult]) -> bool:
    """Return whether a URL is absolute or relative.

    :param url: the URL to test.
    :return: whether the URL is absolute or relative.
    """

    if not isinstance(url, ParseResult):
        url = urlparse(url)

    return bool(url.netloc)


def strip_query_params(url: str) -> str:
    """Remove the query parameters from an absolute URL.

    :param url: a URL.
    :return: the URL with the query parameters removed.
    """
    assert is_url_absolute(url), f"strip_query_params: url {url} is not absolute"
    return urljoin(url, urlparse(url).path)


def retry_session(
    num_retries: int = 3, backoff_factor: float = 0.5, status_forcelist: Tuple = (500, 502, 503, 50)
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
