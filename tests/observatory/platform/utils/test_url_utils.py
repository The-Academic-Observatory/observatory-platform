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

import time
import unittest
from datetime import datetime
from typing import List
from unittest.mock import patch

import httpretty
import requests
from airflow.exceptions import AirflowException
from click.testing import CliRunner
from tenacity import wait_fixed

from observatory.platform.observatory_environment import HttpServer, test_fixtures_path
from observatory.platform.utils.url_utils import (
    get_filename_from_url,
    get_http_response_json,
    get_http_response_xml_to_dict,
    get_http_text_response,
    get_observatory_http_header,
    get_user_agent,
    retry_get_url,
    retry_session,
    wait_for_url,
    get_filename_from_http_header,
)
from tests.observatory.platform.cli.test_platform_command import MockUrlOpen


class TestUrlUtils(unittest.TestCase):
    class MockMetadata:
        @classmethod
        def get(self, attribute):
            if attribute == "Version":
                return "1"
            if attribute == "Home-page":
                return "http://test.test"
            if attribute == "Author-email":
                return "test@test"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def __create_mock_request_sequence(self, url: str, status_codes: List[int], bodies: List[str], sleep: float = 0):
        self.sequence = 0

        def request_callback(request, uri, response_headers):
            status = status_codes[self.sequence]
            body = bodies[self.sequence]
            self.sequence = self.sequence + 1
            time.sleep(sleep)
            return [status, response_headers, body]

        httpretty.register_uri(httpretty.GET, url, body=request_callback)

    def test_retry_session(self):
        httpretty.enable()

        # Test that we receive the last response
        url = "http://test.com/"
        status_codes = [500, 500, 200]
        bodies = ["Internal server error", "Internal server error", "success"]
        self.__create_mock_request_sequence(url, status_codes, bodies)
        response = retry_session(num_retries=3).get(url)
        self.assertEqual(response.text, "success")

        # Test that a requests.exceptions.RetryError is triggered
        url = "http://fail.com/"
        status_codes = [500, 500, 500, 500, 200]  # It should fail before getting to status 200, because we only retry
        # 3 times
        bodies = ["Internal server error"] * 4 + ["success"]
        self.__create_mock_request_sequence(url, status_codes, bodies)

        with self.assertRaises(requests.exceptions.RetryError):
            retry_session(num_retries=3).get(url)

        # Cleanup
        httpretty.disable()
        httpretty.reset()

    def test_retry_get_url(self):
        httpretty.enable()

        # Test that we receive the last response
        url = "http://test.com/"
        status_codes = [500, 404, 200]
        bodies = ["Internal server error", "Page not found", "success"]
        self.__create_mock_request_sequence(url, status_codes, bodies)
        response = retry_get_url(url, num_retries=3, wait=wait_fixed(0))
        self.assertEqual(response.text, "success")

        # Test that an HTTPError is triggered
        url = "http://fail.com/"
        status_codes = [500, 500, 500, 500, 200]  # It should fail before getting to status 200, because we only retry
        # 3 times
        bodies = ["Internal server error"] * 4 + ["success"]
        self.__create_mock_request_sequence(url, status_codes, bodies)
        with self.assertRaises(requests.exceptions.HTTPError):
            response = retry_get_url(url, num_retries=3, wait=wait_fixed(0))

        # Test that a ReadTimeout is triggered
        url = "http://timeout.com/"
        status_codes = [500, 500, 500, 200]
        bodies = ["Internal server error"] * 4 + ["success"]
        self.__create_mock_request_sequence(url, status_codes, bodies, sleep=3)
        with self.assertRaises(requests.exceptions.ReadTimeout):
            response = retry_get_url(url, num_retries=3, wait=wait_fixed(0), timeout=2)

        # Cleanup
        httpretty.disable()
        httpretty.reset()

    @patch("observatory.platform.utils.url_utils.urllib.request.urlopen")
    def test_wait_for_url_success(self, mock_url_open):
        # Mock the status code return value: 200 should succeed
        mock_url_open.return_value = MockUrlOpen(200)

        start = datetime.now()
        state = wait_for_url("http://localhost:8080")
        end = datetime.now()
        duration = (end - start).total_seconds()

        self.assertTrue(state)
        self.assertAlmostEqual(0, duration, delta=0.5)

    @patch("observatory.platform.utils.url_utils.urllib.request.urlopen")
    def test_wait_for_url_failed(self, mock_url_open):
        # Mock the status code return value: 500 should fail
        mock_url_open.return_value = MockUrlOpen(500)

        expected_timeout = 10
        start = datetime.now()
        state = wait_for_url("http://localhost:8080", timeout=expected_timeout)
        end = datetime.now()
        duration = (end - start).total_seconds()

        self.assertFalse(state)
        self.assertAlmostEqual(expected_timeout, duration, delta=1)

    @patch("observatory.platform.utils.url_utils.metadata", return_value=MockMetadata)
    def test_user_agent(self, mock_cfg):
        """Test user agent generation"""

        gt = f"observatory-platform v1 (+http://test.test; mailto: test@test)"
        ua = get_user_agent(package_name="observatory-platform")
        self.assertEqual(ua, gt)

    @patch("observatory.platform.utils.url_utils.metadata", return_value=MockMetadata)
    def test_get_observatory_http_header(self, mock_cfg):
        expected_header = {"User-Agent": "observatory-platform v1 (+http://test.test; mailto: test@test)"}
        header = get_observatory_http_header(package_name="observatory-platform")
        self.assertEqual(expected_header, header)

    def test_get_filename_from_url(self):
        file1 = "myfile.gz"
        url1 = f"http://blah/{file1}"
        url2 = f"http://blah/{file1}?someparam=adfdf&somethingelse=akdf4"

        parsed1 = get_filename_from_url(url1)
        self.assertEqual(parsed1, file1)
        parsed2 = get_filename_from_url(url2)
        self.assertEqual(parsed2, file1)

    def test_get_http_text_response(self):
        with CliRunner().isolated_filesystem():
            httpserver = HttpServer(".")

            with httpserver.create():
                # 404
                url = f"http://{httpserver.host}:{httpserver.port}/missing.txt"
                self.assertRaises(ConnectionError, get_http_text_response, url)

                # OK
                url = f"http://{httpserver.host}:{httpserver.port}/"
                text = get_http_text_response(url)
                self.assertTrue(len(text) > 0)

    def test_get_http_response_json(self):
        with CliRunner().isolated_filesystem():
            httpserver = HttpServer(test_fixtures_path("utils"))

            with httpserver.create():
                url = f"http://{httpserver.host}:{httpserver.port}/get_http_response_json.json"

                response = get_http_response_json(url)
                self.assertTrue(isinstance(response, dict))
                self.assertEqual(response["test"], "value")

    def test_get_http_response_xml_to_dict(self):
        with CliRunner().isolated_filesystem():
            httpserver = HttpServer(test_fixtures_path("utils"))

            with httpserver.create():
                url = f"http://{httpserver.host}:{httpserver.port}/get_http_response_xml_to_dict.xml"

                response = get_http_response_xml_to_dict(url)
                self.assertTrue(isinstance(response, dict))
                self.assertEqual(response["note"]["to"], "Curtin")
                self.assertEqual(response["note"]["from"], "COKI")
                self.assertEqual(response["note"]["heading"], "Test heading")
                self.assertEqual(response["note"]["body"], "Test text")

    @patch("observatory.platform.utils.url_utils.requests.head")
    def test_get_filename_from_http_header(self, m_head):
        url = "http://someurl"

        class MockResponse:
            def __init__(self):
                self.url = url
                self.headers = {
                    "Content-Disposition": 'attachment; filename="unpaywall_snapshot_2023-03-23T083001.jsonl.gz"'
                }
                self.status_code = 200

        # Assert correct filename
        m_head.return_value = MockResponse()
        filename = get_filename_from_http_header(url)
        self.assertEqual("unpaywall_snapshot_2023-03-23T083001.jsonl.gz", filename)

        # Assert AirflowException when status code is invalid (not 200)
        with self.assertRaises(AirflowException):
            response = MockResponse()
            response.status_code = 403
            m_head.return_value = response
            get_filename_from_http_header(url)
