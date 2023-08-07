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


from unittest.mock import patch

import pendulum
from airflow.models.connection import Connection

from observatory.api.client import ApiClient, Configuration
from observatory.api.client.api.observatory_api import ObservatoryApi  # noqa: E501
from observatory.api.testing import ObservatoryApiEnvironment
from observatory.platform.api import make_observatory_api, build_schedule
from observatory.platform.observatory_environment import ObservatoryTestCase, find_free_port


class TestObservatoryAPI(ObservatoryTestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def setUp(self):
        self.timezone = "Pacific/Auckland"
        self.host = "localhost"
        self.port = 5001
        configuration = Configuration(host=f"http://{self.host}:{self.port}")
        api_client = ApiClient(configuration)
        self.api = ObservatoryApi(api_client=api_client)  # noqa: E501
        self.env = ObservatoryApiEnvironment(host=self.host, port=self.port)

    @patch("airflow.hooks.base.BaseHook.get_connection")
    def test_make_observatory_api(self, mock_get_connection):
        """Test make_observatory_api"""

        conn_type = "http"
        host = "api.observatory.academy"
        api_key = "my_api_key"

        # No port
        mock_get_connection.return_value = Connection(uri=f"{conn_type}://:{api_key}@{host}")
        api = make_observatory_api()
        self.assertEqual(f"http://{host}", api.api_client.configuration.host)
        self.assertEqual(api_key, api.api_client.configuration.api_key["api_key"])

        # Port
        port = find_free_port()
        mock_get_connection.return_value = Connection(uri=f"{conn_type}://:{api_key}@{host}:{port}")
        api = make_observatory_api()
        self.assertEqual(f"http://{host}:{port}", api.api_client.configuration.host)
        self.assertEqual(api_key, api.api_client.configuration.api_key["api_key"])

        # Assertion error: missing conn_type empty string
        with self.assertRaises(AssertionError):
            mock_get_connection.return_value = Connection(uri=f"://:{api_key}@{host}")
            make_observatory_api()

        # Assertion error: missing host empty string
        with self.assertRaises(AssertionError):
            mock_get_connection.return_value = Connection(uri=f"{conn_type}://:{api_key}@")
            make_observatory_api()

        # Assertion error: missing password empty string
        with self.assertRaises(AssertionError):
            mock_get_connection.return_value = Connection(uri=f"://:{api_key}@{host}")
            make_observatory_api()

        # Assertion error: missing conn_type None
        with self.assertRaises(AssertionError):
            mock_get_connection.return_value = Connection(password=api_key, host=host)
            make_observatory_api()

        # Assertion error: missing host None
        with self.assertRaises(AssertionError):
            mock_get_connection.return_value = Connection(conn_type=conn_type, password=api_key)
            make_observatory_api()

        # Assertion error: missing password None
        with self.assertRaises(AssertionError):
            mock_get_connection.return_value = Connection(host=host, password=api_key)
            make_observatory_api()

    def test_build_schedule(self):
        start_date = pendulum.datetime(2021, 1, 1)
        end_date = pendulum.datetime(2021, 2, 1)
        schedule = build_schedule(start_date, end_date)
        self.assertEqual([pendulum.Period(pendulum.date(2021, 1, 1), pendulum.date(2021, 1, 31))], schedule)

        start_date = pendulum.datetime(2021, 1, 1)
        end_date = pendulum.datetime(2021, 3, 1)
        schedule = build_schedule(start_date, end_date)
        self.assertEqual(
            [
                pendulum.Period(pendulum.date(2021, 1, 1), pendulum.date(2021, 1, 31)),
                pendulum.Period(pendulum.date(2021, 2, 1), pendulum.date(2021, 2, 28)),
            ],
            schedule,
        )

        start_date = pendulum.datetime(2021, 1, 7)
        end_date = pendulum.datetime(2021, 2, 7)
        schedule = build_schedule(start_date, end_date)
        self.assertEqual([pendulum.Period(pendulum.date(2021, 1, 7), pendulum.date(2021, 2, 6))], schedule)
