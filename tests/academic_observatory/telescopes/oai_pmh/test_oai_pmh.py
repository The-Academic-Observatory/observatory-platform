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

import datetime
import os
import unittest

import httpretty

from academic_observatory.telescopes.oai_pmh import fetch_context_urls, oai_pmh_serialize_custom_types, \
    InvalidOaiPmhContextPageException, parse_utc_str_to_date, oai_pmh_endpoints_path, oai_pmh_path, \
    serialize_date_to_utc_str, parse_list, parse_record_date, parse_value
from academic_observatory.utils import test_data_dir


def load_page_body(file_name: str) -> str:
    path = os.path.join(test_data_dir(__file__), 'oai_pmh', file_name)
    data: str
    with open(path) as f:
        data = f.read()
    return data


class TestOaiPmh(unittest.TestCase):
    identify: dict

    record_metadata: dict

    def test_parse_list(self):
        # Test item that exists
        metadata = {'title': ['A title']}
        results = parse_list(metadata, "title")
        self.assertListEqual(results, metadata['title'])

        # Test empty list is returned for item that doesn't exist
        results = parse_list(metadata, "description")
        self.assertListEqual(results, [])

    def test_parse_record_date(self):
        # Test incorrectly formatted date
        metadata = {'date': ['2016']}
        result = parse_record_date(metadata, "date")
        self.assertEqual(result, None)

        # Test incorrectly formatted date
        metadata = {'date': ['']}
        result = parse_record_date(metadata, "date")
        self.assertEqual(result, None)

        # Test empty list
        metadata = {'date': []}
        result = parse_record_date(metadata, "date")
        self.assertEqual(result, None)

        # Test date that doesn't exist
        metadata = {}
        result = parse_record_date(metadata, "date")
        self.assertEqual(result, None)

        # Test correct date
        metadata = {"date": ['2019-12-01']}
        result = parse_record_date(metadata, "date")
        self.assertEqual(result, datetime.datetime(year=2019, month=12, day=1, tzinfo=datetime.timezone.utc))

        # Test correct date
        metadata = {"date": ['2019-10-22T00:49:14Z']}
        result = parse_record_date(metadata, "date")
        self.assertEqual(result,
                         datetime.datetime(year=2019, month=10, day=22, hour=0, minute=49, second=14, microsecond=0,
                                           tzinfo=datetime.timezone.utc))

    def test_parse_value(self):
        class Identify:
            pass

        obj = Identify()
        obj.adminEmail = 'person@xyz.com'
        obj.oai_identifier = None

        # Test object that exists
        value = parse_value(obj, "adminEmail")
        self.assertEqual(value, obj.adminEmail)

        # Test None type
        value = parse_value(obj, "oai_identifier")
        self.assertEqual(value, None)

        # Test attribute that doesn't exist
        value = parse_value(obj, "repositoryName")
        self.assertEqual(value, None)

    def test_parse_utc_str_to_date(self):
        input_str = '2019-10-22T00:49:14Z'
        expected_datetime = datetime.datetime(year=2019, month=10, day=22, hour=0, minute=49, second=14, microsecond=0,
                                              tzinfo=datetime.timezone.utc)
        actual_datetime = parse_utc_str_to_date(input_str)
        self.assertEqual(actual_datetime, expected_datetime)

    def test_serialize_date_to_utc_str(self):
        # Check datetime.datetime works
        datetime_instance = datetime.datetime(year=2019, month=10, day=1, hour=15, minute=23, second=14, microsecond=0,
                                              tzinfo=datetime.timezone.utc)
        expected_date = '2019-10-01T15:23:14Z'
        actual_date = serialize_date_to_utc_str(datetime_instance)
        self.assertEqual(actual_date, expected_date)

        # Check that datetime.date works (low granularity dates)
        date_instance = datetime.date(year=2019, month=10, day=1)
        expected_date = '2019-10-01T00:00:00Z'
        actual_date = serialize_date_to_utc_str(date_instance)
        self.assertEqual(actual_date, expected_date)

    def test_oai_pmh_serialize_custom_types(self):
        # datetime.datetime
        datetime_instance = datetime.datetime(year=2020, month=1, day=1, hour=1, minute=2, second=3, microsecond=4,
                                              tzinfo=datetime.timezone.utc)
        datetime_expected = '2020-01-01T01:02:03'
        datetime_actual = oai_pmh_serialize_custom_types(datetime_instance)
        self.assertEqual(datetime_actual, datetime_expected)

        # non supported type
        with self.assertRaises(TypeError):
            oai_pmh_serialize_custom_types(1)

    def test_oai_pmh_path(self):
        path = oai_pmh_path()
        self.assertTrue(os.path.exists(path))

    def test_oai_pmh_endpoints_path(self):
        path = oai_pmh_endpoints_path()
        self.assertEqual(path, os.path.join(oai_pmh_path(), "oai_pmh_endpoints.csv"))

    def test_fetch_context_urls(self):
        # Enable httpretty so that responses can be simulated
        httpretty.enable()

        # Register URIs for httpretty
        pages = [('https://espace.curtin.edu.au/', load_page_body('curtin_espace'), 'text/html;charset=utf-8', 200),
                 ('https://espace.curtin.edu.au/oai/request', load_page_body('curtin_oai_request'),
                  'application/xml;charset=UTF-8', 200),
                 ('https://espace.curtin.edu.au/oai/', load_page_body('curtin_oai_context'),
                  'text/html;charset=ISO-8859-1', 400),
                 ('http://dspace.nwu.ac.za/oai/', load_page_body('nwu_oai_context'), 'text/html;charset=ISO-8859-1',
                  400)]
        for uri, body, content_type, status in pages:
            httpretty.register_uri(httpretty.GET, uri, body=body, content_type=content_type, status=status)

        # An invalid OAI-PMH context page
        endpoint_url = 'https://espace.curtin.edu.au/'
        with self.assertRaises(InvalidOaiPmhContextPageException):
            fetch_context_urls(endpoint_url)

        # An OAI-PMH endpoint
        endpoint_url = 'https://espace.curtin.edu.au/oai/request'
        with self.assertRaises(InvalidOaiPmhContextPageException):
            fetch_context_urls(endpoint_url)

        # Valid OAI-PMH context page
        contexts_url = 'https://espace.curtin.edu.au/oai/'
        urls = fetch_context_urls(contexts_url)
        self.assertListEqual(urls, ['https://espace.curtin.edu.au/oai/request',
                                    'https://espace.curtin.edu.au/oai/driver',
                                    'https://espace.curtin.edu.au/oai/openaire',
                                    'https://espace.curtin.edu.au/oai/openaccess'])

        # Valid OAI-PMH context page with malformed URLs that have to be sanitized
        contexts_url = 'http://dspace.nwu.ac.za/oai/'
        urls = fetch_context_urls(contexts_url)
        self.assertListEqual(urls, ['http://dspace.nwu.ac.za/oai/request',
                                    'http://dspace.nwu.ac.za/oai/driver',
                                    'http://dspace.nwu.ac.za/oai/openaire'])

        # Disable httpretty so that it doesn't keep interfering with sockets
        httpretty.disable()
        httpretty.reset()
