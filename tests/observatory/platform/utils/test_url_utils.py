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

import unittest
from datetime import datetime
from typing import List
from unittest.mock import patch

import httpretty
import requests

from observatory.platform.utils.url_utils import (
    get_url_domain_suffix,
    unique_id,
    is_url_absolute,
    strip_query_params,
    retry_session,
    get_user_agent,
    wait_for_url,
)
from tests.observatory.platform.cli.test_platform_command import MockUrlOpen


class TestUrlUtils(unittest.TestCase):
    relative_urls = [
        "#skip-to-content",
        "#",
        "/local/assets/css/tipso.css",
        "acknowledgements/rogers.html",
        "acknowledgements/staff.html#lwallace",
        "?residentType=INT",
        "hello/?p=2036",
    ]

    absolute_urls = [
        "https://www.curtin.edu.au/",
        "//global.curtin.edu.au/template/css/layoutv3.css",
        "https://www.curtin.edu.au/?p=1000",
        "https://www.curtin.edu.au/test#",
        "https://www.curtin.edu.au/test#lwallace",
        "//global.curtin.edu.au/template/css/layoutv3.css/?a=1",
    ]

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

    def test_get_url_domain_suffix(self):
        expected = "curtin.edu.au"

        level_one = get_url_domain_suffix("https://www.curtin.edu.au/")
        self.assertEqual(level_one, expected)

        level_two = get_url_domain_suffix("https://alumniandgive.curtin.edu.au/")
        self.assertEqual(level_two, expected)

        level_two_with_path = get_url_domain_suffix("https://alumniandgive.curtin.edu.au/giving-to-curtin/")
        self.assertEqual(level_two_with_path, expected)

        level_two_no_https = get_url_domain_suffix("alumniandgive.curtin.edu.au")
        self.assertEqual(level_two_no_https, expected)

    def test_unique_id(self):
        expected_ids = [
            "5d41402abc4b2a76b9719d911017c592",
            "7d793037a0760186574b0282f2f435e7",
            "5eb63bbbe01eeed093cb22bb8f5acdc3",
        ]
        ids = [unique_id("hello"), unique_id("world"), unique_id("hello world")]
        self.assertListEqual(ids, expected_ids)

    def test_is_url_absolute(self):
        urls = TestUrlUtils.relative_urls + TestUrlUtils.absolute_urls
        results_expected = [False] * len(TestUrlUtils.relative_urls) + [True] * len(TestUrlUtils.absolute_urls)
        results_actual = [is_url_absolute(url) for url in urls]
        self.assertListEqual(results_actual, results_expected)

    def test_strip_query_params(self):
        # Test absolute URLs
        results_expected = [
            "https://www.curtin.edu.au/",
            "//global.curtin.edu.au/template/css/layoutv3.css",
            "https://www.curtin.edu.au/",
            "https://www.curtin.edu.au/test",
            "https://www.curtin.edu.au/test",
            "//global.curtin.edu.au/template/css/layoutv3.css/",
        ]
        results_actual = [strip_query_params(url) for url in TestUrlUtils.absolute_urls]
        self.assertListEqual(results_actual, results_expected)

        # Test that passing a non-absolute URL raises an exception
        for url in TestUrlUtils.relative_urls:
            with self.assertRaises(Exception):
                strip_query_params(url)

    def __create_mock_request_sequence(self, url: str, status_codes: List[int], bodies: List[str]):
        self.sequence = 0

        def request_callback(request, uri, response_headers):
            status = status_codes[self.sequence]
            body = bodies[self.sequence]
            self.sequence = self.sequence + 1
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
        """ Test user agent generation """

        gt = f"observatory-platform v1 (+http://test.test; mailto: test@test)"
        ua = get_user_agent(package_name="observatory-platform")
        self.assertEqual(ua, gt)
