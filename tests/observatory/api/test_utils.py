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
#
#
# Author: Tuan Chien


import os

from observatory.api.client import ApiClient, Configuration
from observatory.api.client.api.observatory_api import ObservatoryApi  # noqa: E501
from observatory.api.testing import ObservatoryApiEnvironment
from observatory.api.utils import (
    get_api_client,
)
from observatory.platform.observatory_environment import ObservatoryTestCase
from observatory.platform.observatory_environment import find_free_port


class TestApiUtils(ObservatoryTestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # API environment
        self.host = "localhost"
        self.port = find_free_port()
        configuration = Configuration(host=f"http://{self.host}:{self.port}")
        api_client = ApiClient(configuration)
        self.api = ObservatoryApi(api_client=api_client)  # noqa: E501
        self.env = ObservatoryApiEnvironment(host=self.host, port=self.port)

    def test_get_api_client(self):
        # No env var set
        api = get_api_client()
        self.assertEqual(api.api_client.configuration.host, "http://localhost:5002")
        self.assertEqual(api.api_client.configuration.api_key, {})

        # Environment variable set
        os.environ["API_URI"] = "http://testhost:5002"
        api = get_api_client()
        self.assertEqual(api.api_client.configuration.host, "http://testhost:5002")
        self.assertEqual(api.api_client.configuration.api_key, {"api_key": None})
        del os.environ["API_URI"]

        # Pass in arguments
        api = get_api_client(host="host1")
        self.assertEqual(api.api_client.configuration.host, "http://host1:5002")
        self.assertEqual(api.api_client.configuration.api_key, {})
