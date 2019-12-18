#
# Copyright 2019 Curtin University. All rights reserved.
#
# Author: James Diprose
#

import datetime
import logging
from typing import Tuple, List
from urllib.parse import urlparse, urljoin
from xml.etree import ElementTree

from sickle import Sickle
from sickle.models import Identify

from academic_observatory.utils import strip_query_params, is_url_absolute, retry_session, HtmlParser

############################
# OAI-PMH parsers
############################

__UTC_DATETIME_PATTERN_FULL = "%Y-%m-%dT%H:%M:%SZ"
__UTC_DATETIME_PATTERN_YEAR = "%Y-%m-%d"


def parse_value(obj: object, name: str):
    """ Get a sickle dictionary object. If no item is found then return None.

    :param obj: the object.
    :param name: the key.
    :return: the value.
    """

    try:
        return getattr(obj, name)
    except AttributeError:
        return None


def parse_utc_str_to_date(string: str) -> datetime.date:
    """ Parse an OAI-PMH date string into a datetime.datetime object.

    :param string: the UTC date string.
    :return: the datetime.datetime object.
    """

    result = None

    if string is not None:
        try:
            result = datetime.datetime.strptime(string, __UTC_DATETIME_PATTERN_FULL)
            result = result.replace(tzinfo=datetime.timezone.utc)
        except ValueError:
            try:
                result = datetime.datetime.strptime(string, __UTC_DATETIME_PATTERN_YEAR)
                result = result.replace(tzinfo=datetime.timezone.utc)
            except ValueError:
                pass

    return result


############################
# OAI-PMH exceptions
############################

class InvalidOaiPmhContextPageException(Exception):
    """Raise for invalid OAI-PMH context page."""


############################
# OAI-PMH fetching methods
############################

def fetch_identify(endpoint_url: str, timeout: float = 30.) -> Tuple[Identify, Exception]:
    """ Fetch an OAI-PMH identity object or any errors encountered when fetching it.

    :param endpoint_url: the URL of the OAI-PMH endpoint / identity.
    :param timeout: the timeout in seconds when fetching the identity.
    :return: the identity object and an exception.
    """

    logging.debug(f"fetch_identify: fetching identity of source: {endpoint_url}")

    # Create Sickle client
    conn = Sickle(endpoint_url, max_retries=1, timeout=timeout)
    identity = None
    ex = None

    # Get the Identity object: https://www.openarchives.org/OAI/openarchivesprotocol.html#Identify
    try:
        identity = conn.Identify()
    except Exception as e:
        ex = e
        logging.error(f"fetch_identify: exception getting identity: {ex}")

    return identity, ex


def fetch_context_urls(context_page_url: str) -> List[str]:
    """ Fetch the OAI-PMH context URLs within a a page specified by a URL.

    InvalidOaiPmhContextPageException will be thrown if the page isn't an OAI-PMH context page.

    :param context_page_url: the URL.
    :return: a list of OAI-PMH endpoint URLs that were found on the page.
    """

    urls = []

    # Get page and content type
    # Have to set a timeout otherwise requests may block indefinitely
    response = retry_session().get(context_page_url, timeout=10.)
    content_type = response.headers['Content-Type']

    # If an XML file with an OAI-PMH root tag then it is probably an endpoint
    if "text/xml" in content_type or "application/xml" in content_type:
        root = ElementTree.fromstring(response.text)
        if "OAI-PMH" in root.tag:
            raise InvalidOaiPmhContextPageException("Not an OAI-PMH context page: an OAI-PMH endpoint.")
        else:
            raise InvalidOaiPmhContextPageException("Not an OAI-PMH context page: unknown page type.")
    # If HTML page extract links that appear to be from an OAI-PMH context page
    elif "text/html" in content_type:
        parser = HtmlParser(response.text)
        links = parser.get_links()

        # Get identity URLs
        for url, title in links:
            # Replace \ with / for this case: http://dspace.nwu.ac.za/oai/
            sanitized_url = url.replace('\\', '/')
            parsed_url = urlparse(sanitized_url)
            if parsed_url.query == "verb=Identify":
                if is_url_absolute(parsed_url):
                    stripped_url = strip_query_params(sanitized_url)
                else:
                    stripped_url = strip_query_params(urljoin(context_page_url, sanitized_url))

                urls.append(stripped_url)

        if len(urls) == 0:
            raise InvalidOaiPmhContextPageException("No OAI-PMH endpoint URLs in HTML content.")

    return urls
