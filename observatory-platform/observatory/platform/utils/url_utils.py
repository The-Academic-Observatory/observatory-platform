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
from typing import Tuple, Union
from urllib.parse import urlparse, urljoin, ParseResult

import requests
import tldextract
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


def get_url_domain_suffix(url: str) -> str:
    """ Extract a URL composed of the domain name and the suffix of the URL. For example, library.curtin.edu would
    become curtin.edu

    :param url: a URL.
    :return: the domain + . + suffix of the URL.
    """

    result = tldextract.extract(url)
    return f"{result.domain}.{result.suffix}"


def unique_id(string: str) -> str:
    """ Generate a unique identifier from a string.

    :param string: a string.
    :return: the unique identifier.
    """
    return hashlib.md5(string.encode('utf-8')).hexdigest()


def is_url_absolute(url: Union[str, ParseResult]) -> bool:
    """ Return whether a URL is absolute or relative.

    :param url: the URL to test.
    :return: whether the URL is absolute or relative.
    """

    if not isinstance(url, ParseResult):
        url = urlparse(url)

    return bool(url.netloc)


def strip_query_params(url: str) -> str:
    """ Remove the query parameters from an absolute URL.

    :param url: a URL.
    :return: the URL with the query parameters removed.
    """
    assert is_url_absolute(url), f"strip_query_params: url {url} is not absolute"
    return urljoin(url, urlparse(url).path)


def retry_session(num_retries: int = 3, backoff_factor: float = 0.5,
                  status_forcelist: Tuple = (500, 502, 503, 50)) -> requests.Session:
    """ Create a session object that is able to retry a given number of times.

    :param num_retries: the number of times to retry.
    :param backoff_factor: the factor to use for determining how long to wait before retrying. See
    urllib3.util.retry.Retry for more details.
    :param status_forcelist: which integer status codes (that would normally not trigger a retry) should
    trigger a retry.
    :return: the session.
    """
    session = requests.Session()
    retry = Retry(total=num_retries, connect=num_retries, read=num_retries, redirect=num_retries,
                  status_forcelist=status_forcelist, backoff_factor=backoff_factor)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session
