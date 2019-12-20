#
# Copyright 2019 Curtin University. All rights reserved.
#
# Author: James Diprose
#

import datetime
import logging
import os
from typing import List
from typing import Union
from urllib.parse import urlparse, urljoin
from xml.etree import ElementTree

from sickle import Sickle
from sickle.oaiexceptions import NoRecordsMatch

from academic_observatory.oai_pmh.schema import Endpoint, Record, RecordHeader
from academic_observatory.utils import get_home_dir
from academic_observatory.utils import strip_query_params, is_url_absolute, retry_session, HtmlParser

############################
# Globals
############################

OAI_PMH_CACHE_SUBDIR = "datasets/oai_pmh"
OAI_PMH_ENDPOINTS_FILENAME = "oai_pmh_endpoints.csv"

############################
# OAI-PMH parsers
############################

__UTC_DATETIME_PATTERN_FULL = "%Y-%m-%dT%H:%M:%SZ"
__UTC_DATETIME_PATTERN_YEAR = "%Y-%m-%d"


def parse_list(obj: dict, key: str) -> List[str]:
    """ Returns a list of objects, if key doesn't exist then returns an empty list

    :param obj:
    :param key:
    :return:
    """

    result = []
    if key in obj:
        # In BigQuery, items in a list cannot be None
        items = []
        for item in obj[key]:
            if item is not None:
                items.append(item)
        result = items
    return result


def parse_record_date(obj: dict, key: str):
    """ Return the first date that we can parse from an OAI-PMH record field.

    :param obj:
    :param key:
    :return:
    """
    result = None

    if key in obj:
        for item in obj[key]:
            if item is not None:
                result = parse_utc_str_to_date(item)
                if result is not None:
                    break

    return result


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


def serialize_date_to_utc_str(date: Union[datetime.datetime, datetime.date]) -> str:
    """ Serialize a date object to a UTC date string.

    :param date: the date to serialize.
    :return: utc date string.
    """

    result = ""

    try:
        result = date.strftime(__UTC_DATETIME_PATTERN_FULL)
    except ValueError:
        try:
            result = date.strftime(__UTC_DATETIME_PATTERN_YEAR)
        except ValueError:
            logging.warning(f"serialize_date_to_utc_str: date format not known: {date}")

    return result


def oai_pmh_serialize_custom_types(obj) -> str:
    if isinstance(obj, datetime.datetime):
        result = obj.strftime("%Y-%m-%dT%H:%M:%S")
    else:
        result = str(obj)
        logging.error(f"oai_pmh_serialize_custom_types: Object of type {type(obj)} is not JSON serializable: {result}")

    return result


############################
# OAI-PMH exceptions
############################

class InvalidOaiPmhContextPageException(Exception):
    """Raise for invalid OAI-PMH context page."""


class RecordDateOutOfRangeError(Exception):
    """Raise for when a record returns a date that is outside of the date queried."""
    pass


############################
# Main OAI-PMH functions
############################

def get_default_oai_pmh_path() -> str:
    """ Get the default path to the OAI-PMH dataset folder.
    :return: the default path to the OAI-PMH dataset folder.
    """

    cache_dir, cache_subdir, datadir = get_home_dir(cache_subdir=OAI_PMH_CACHE_SUBDIR)
    return datadir


def get_default_oai_pmh_endpoints_path() -> str:
    """ Get default OAI PMH endpoints file path.
    :return:
    """

    cache_dir, cache_subdir, datadir = get_home_dir(cache_subdir=OAI_PMH_CACHE_SUBDIR)
    return os.path.join(datadir, OAI_PMH_ENDPOINTS_FILENAME)


def fetch_endpoint(endpoint_url: str, timeout: float = 30.) -> Endpoint:
    """ Fetch an OAI-PMH endpoint object or any errors encountered when fetching it.

    :param endpoint_url: the URL of the OAI-PMH endpoint / identity.
    :param timeout: the timeout in seconds when fetching the identity.
    :return: the identity object and an exception.
    """

    logging.debug(f"fetch_endpoint: fetching identity of source: {endpoint_url}")

    # Create Sickle client
    conn = Sickle(endpoint_url, max_retries=1, timeout=timeout)

    # Get the Identify object: https://www.openarchives.org/OAI/openarchivesprotocol.html#Identify
    identify = conn.Identify()

    # Create Endpoint object from identify
    endpoint = Endpoint(None,
                        endpoint_url,
                        parse_value(identify, "adminEmail"),
                        parse_value(identify, "author"), parse_value(identify, "baseURL"),
                        parse_value(identify, "comment"), parse_value(identify, "compression"),
                        parse_value(identify, "content"), parse_value(identify, "creator"),
                        parse_value(identify, "dataPolicy"), parse_value(identify, "dc"),
                        parse_value(identify, "deletedRecord"), parse_value(identify, "delimiter"),
                        parse_value(identify, "description"),
                        parse_utc_str_to_date(parse_value(identify, "earliestDatestamp")),
                        parse_value(identify, "email"), parse_value(identify, "eprints"),
                        parse_value(identify, "friends"), parse_value(identify, "granularity"),
                        parse_value(identify, "identifier"), parse_value(identify, "institution"),
                        parse_value(identify, "metadataPolicy"), parse_value(identify, "name"),
                        parse_value(identify, "oai-identifier"), parse_value(identify, "protocolVersion"),
                        parse_value(identify, "purpose"), parse_value(identify, "repositoryIdentifier"),
                        parse_value(identify, "repositoryName"), parse_value(identify, "rights"),
                        parse_value(identify, "rightsDefinition"), parse_value(identify, "rightsManifest"),
                        parse_value(identify, "sampleIdentifier"), parse_value(identify, "scheme"),
                        parse_value(identify, "submissionPolicy"), parse_value(identify, "text"),
                        parse_value(identify, "title"), parse_value(identify, "toolkit"),
                        parse_value(identify, "toolkitIcon"), parse_value(identify, "URL"),
                        parse_value(identify, "version"), parse_value(identify, "XOAIDescription"),
                        None)

    return endpoint


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


def fetch_endpoint_records(endpoint: Endpoint, from_date: datetime.datetime, until_date: datetime.datetime,
                           metadata_prefix: str = 'oai_dc', ignore_deleted: bool = True, timeout: float = 10.,
                           max_retries: int = 3) -> List:
    """ Fetch a list of OAI-PMH records from an OAI-PMH endpoint.

    :param endpoint: the endpoint to fetch data from.
    :param from_date: the start date for the record search.
    :param until_date: the end date for the record search.
    :param metadata_prefix: ?
    :param ignore_deleted: ?
    :param timeout: the timeout when requesting data with the requests library.
    :param max_retries: how many times to retry on failures.
    :return:
    """

    logging.debug(f"fetch_records: fetching data for source: {endpoint.source_url}")
    records = []

    # Create Sickle client
    conn = Sickle(endpoint.source_url, max_retries=max_retries, timeout=timeout)

    # Create the query
    from_ = serialize_date_to_utc_str(from_date)
    until_ = serialize_date_to_utc_str(until_date)
    query = {
        'metadataPrefix': metadata_prefix,
        'ignore_deleted': ignore_deleted,
        'from': from_,
        'until': until_
    }
    logging.debug(f"fetch_records: query {query}")

    # Get the records
    try:
        iterator = conn.ListRecords(**query)
        for sickle_record in iterator:
            record_date = parse_utc_str_to_date(sickle_record.header.datestamp)
            if record_date is not None and not (from_date <= record_date <= until_date):
                msg = f"Endpoint is returning records outside of the requested date range."
                f" Not true {from_} <= {sickle_record.header.datestamp} <= {until_}"
                logging.error(msg)
                raise RecordDateOutOfRangeError(msg)

            header = RecordHeader(parse_utc_str_to_date(sickle_record.header.datestamp),
                                  sickle_record.header.deleted,
                                  sickle_record.header.identifier)
            metadata = sickle_record.metadata
            record = Record(None, endpoint.get_id(), endpoint.source_url, header,
                            parse_list(metadata, "title"),
                            parse_list(metadata, "creator"), parse_list(metadata, "description"),
                            parse_record_date(metadata, "date"),
                            parse_list(metadata, "type"), parse_list(metadata, "identifier"),
                            parse_list(metadata, "format"), parse_list(metadata, "subject"),
                            parse_list(metadata, "relation"), parse_list(metadata, "publisher"),
                            parse_list(metadata, "contributor"), parse_list(metadata, "language"))

            records.append(record)
    except NoRecordsMatch:
        # This exception is OK
        logging.warning(f"fetch_records: no records found for date range: {from_} to {until_}")

    return records
