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

# Author: Tuan Chien, Aniek Roelofs

import datetime
import json
import os
import unittest
from unittest.mock import patch
from urllib.parse import quote

import dateutil
import paramiko
import pendulum
import pysftp
from airflow.models.connection import Connection
from click.testing import CliRunner

from observatory.platform.utils.file_utils import load_jsonl, list_to_jsonl_gz, gzip_file_crc
from observatory.platform.utils.telescope_utils import (
    make_telescope_sensor,
    PeriodCount,
    ScheduleOptimiser,
    make_sftp_connection,
    make_dag_id,
    make_observatory_api,
    get_prev_execution_date,
    normalized_schedule_interval,
)


class TestTelescopeUtils(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(TestTelescopeUtils, self).__init__(*args, **kwargs)

    def test_normalized_schedule_interval(self):
        """ Test normalized_schedule_interval """
        schedule_intervals = [
            (None, None),
            ("@daily", "0 0 * * *"),
            ("@weekly", "0 0 * * 0"),
            ("@monthly", "0 0 1 * *"),
            ("@quarterly", "0 0 1 */3 *"),
            ("@yearly", "0 0 1 1 *"),
            ("@once", None),
            (datetime.timedelta(days=1), datetime.timedelta(days=1)),
        ]
        for test in schedule_intervals:
            schedule_interval = test[0]
            expected_n_schedule_interval = test[1]
            actual_n_schedule_interval = normalized_schedule_interval(schedule_interval)

            self.assertEqual(expected_n_schedule_interval, actual_n_schedule_interval)

    def test_get_prev_execution_date(self):
        """ Test get_prev_execution_date """
        execution_date = pendulum.Pendulum(2020, 2, 20)
        # test for both cron preset and cron expression
        for schedule_interval in ["@monthly", "0 0 1 * *"]:
            prev_execution_date = get_prev_execution_date(schedule_interval, execution_date)
            expected = datetime.datetime(2020, 2, 1, tzinfo=dateutil.tz.gettz(execution_date.timezone_name))
            self.assertEqual(expected, prev_execution_date)

    @patch.object(pysftp, "Connection")
    @patch("airflow.hooks.base_hook.BaseHook.get_connection")
    def test_make_sftp_connection(self, mock_airflow_conn, mock_pysftp_connection):
        """ Test that sftp connection is initialized correctly """

        # set up variables
        username = "username"
        password = "password"
        host = "host"
        host_key = quote(paramiko.RSAKey.generate(512).get_base64(), safe="")

        # mock airflow sftp service conn
        mock_airflow_conn.return_value = Connection(uri=f"ssh://{username}:{password}@{host}?host_key={host_key}")

        # run function
        sftp = make_sftp_connection()

        # confirm sftp server was initialised with correct username, password and cnopts
        call_args = mock_pysftp_connection.call_args

        self.assertEqual(1, len(call_args[0]))
        self.assertEqual(host, call_args[0][0])

        self.assertEqual(4, len(call_args[1]))
        self.assertEqual(username, call_args[1]["username"])
        self.assertEqual(password, call_args[1]["password"])
        self.assertIsInstance(call_args[1]["cnopts"], pysftp.CnOpts)
        self.assertIsNone(call_args[1]["port"])

    def test_make_dag_id(self):
        """ Test make_dag_id """

        expected_dag_id = "onix_curtin_press"
        dag_id = make_dag_id("onix", "Curtin Press")
        self.assertEqual(expected_dag_id, dag_id)

    @patch("airflow.hooks.base_hook.BaseHook.get_connection")
    def test_make_observatory_api(self, mock_get_connection):
        """ Test make_observatory_api """

        conn_type = "http"
        host = "api.observatory.academy"
        api_key = "my_api_key"

        # No port
        mock_get_connection.return_value = Connection(uri=f"{conn_type}://:{api_key}@{host}")
        api = make_observatory_api()
        self.assertEqual(f"http://{host}", api.api_client.configuration.host)
        self.assertEqual(api_key, api.api_client.configuration.api_key["api_key"])

        # Port
        port = 8080
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

    def test_list_to_jsonl_gz(self):
        """ Test writing list of dicts to jsonl.gz file """
        list_of_dicts = [{"k1a": "v1a", "k2a": "v2a"}, {"k1b": "v1b", "k2b": "v2b"}]
        file_path = "list.jsonl.gz"
        expected_file_hash = "e608cfeb"
        with CliRunner().isolated_filesystem():
            list_to_jsonl_gz(file_path, list_of_dicts)
            self.assertTrue(os.path.isfile(file_path))
            actual_file_hash = gzip_file_crc(file_path)
            self.assertEqual(expected_file_hash, actual_file_hash)

    def test_load_jsonl(self):
        """ Test loading json lines files """

        with CliRunner().isolated_filesystem() as t:
            expected_records = [
                {"name": "Elon Musk"},
                {"name": "Jeff Bezos"},
                {"name": "Peter Beck"},
                {"name": "Richard Branson"},
            ]
            file_path = os.path.join(t, "test.json")
            with open(file_path, mode="w") as f:
                for record in expected_records:
                    f.write(f"{json.dumps(record)}\n")

            actual_records = load_jsonl(file_path)
            self.assertListEqual(expected_records, actual_records)


class TestMakeTelescopeSensor(unittest.TestCase):
    """ Test the external task sensor creation. """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    class Organisation:
        def __init__(self):
            self.name = "test"

    class Response:
        def __init__(self):
            self.organisation = TestMakeTelescopeSensor.Organisation()

    def test_make_telescope_sensor(self):
        telescope = TestMakeTelescopeSensor.Response()
        sensor = make_telescope_sensor(telescope.organisation.name, "dag_prefix")
        self.assertEqual(sensor.task_id, "dag_prefix_test_sensor")
        self.assertEqual(sensor.mode, "reschedule")
        self.assertEqual(sensor.external_dag_id, "dag_prefix_test")


class TestScheduleOptimiser(unittest.TestCase):
    """ Test schedule optimiser that minimises API calls. """

    def __init__(self, *args, **kwargs):
        super(TestScheduleOptimiser, self).__init__(*args, **kwargs)

        self.max_per_call = 2
        self.max_per_query = 10

        self.historic_counts_trivial = [
            PeriodCount(pendulum.Period(start=pendulum.date(1000, 1, 1), end=pendulum.date(1000, 1, 1)), 0),
            PeriodCount(pendulum.Period(start=pendulum.date(1000, 2, 1), end=pendulum.date(1000, 2, 1)), 1),
            PeriodCount(pendulum.Period(start=pendulum.date(1000, 3, 1), end=pendulum.date(1000, 3, 1)), 2),
            PeriodCount(pendulum.Period(start=pendulum.date(1000, 4, 1), end=pendulum.date(1000, 4, 1)), 3),
        ]

    def test_get_num_calls(self):
        """ Test the num_call calculation. """

        num_results = 10
        max_per_call = 2
        calls = ScheduleOptimiser.get_num_calls(num_results, max_per_call)
        self.assertEqual(calls, 5)

        max_per_call = 3
        calls = ScheduleOptimiser.get_num_calls(num_results, max_per_call)
        self.assertEqual(calls, 4)

    def test_extract_schedule(self):
        """ Test schedule extraction from solution. """

        moves = [0, 1, 2, 3]
        schedule = ScheduleOptimiser.extract_schedule(self.historic_counts_trivial, moves)

        for i in range(len(schedule), 0, -1):
            self.assertEqual(schedule[i - 1].start.month, i)

        moves = [0, 1, 2, 1]
        schedule = ScheduleOptimiser.extract_schedule(self.historic_counts_trivial, moves)
        self.assertEqual(len(schedule), 2)
        self.assertEqual(schedule[0].start.month, 1)
        self.assertEqual(schedule[0].end.month, 1)
        self.assertEqual(schedule[1].start.month, 2)
        self.assertEqual(schedule[1].end.month, 4)

    def test_optimise_leading_zeros(self):
        historic_counts = [
            PeriodCount(pendulum.Period(start=pendulum.date(1000, 1, 1), end=pendulum.date(1000, 1, 1)), 0),
            PeriodCount(pendulum.Period(start=pendulum.date(1000, 1, 1), end=pendulum.date(1000, 1, 1)), 0),
            PeriodCount(pendulum.Period(start=pendulum.date(1000, 1, 1), end=pendulum.date(1000, 1, 1)), 0),
            PeriodCount(pendulum.Period(start=pendulum.date(1000, 4, 1), end=pendulum.date(1000, 4, 21)), 1),
        ]

        schedule, min_calls = ScheduleOptimiser.optimise(self.max_per_call, self.max_per_query, historic_counts)
        self.assertEqual(len(schedule), 1)
        self.assertEqual(schedule[0].start, pendulum.date(1000, 1, 1))
        self.assertEqual(schedule[0].end, pendulum.date(1000, 4, 21))
        self.assertEqual(min_calls, 1)
        self.assertEqual(len(schedule), 1)

    def test_optimise_leading_zeros2(self):
        historic_counts = [
            PeriodCount(pendulum.Period(start=pendulum.date(1000, 1, 1), end=pendulum.date(1000, 1, 1)), 0),
            PeriodCount(pendulum.Period(start=pendulum.date(1000, 1, 1), end=pendulum.date(1000, 1, 1)), 0),
        ]

        schedule, min_calls = ScheduleOptimiser.optimise(self.max_per_call, self.max_per_query, historic_counts)
        self.assertEqual(len(schedule), 1)
        self.assertEqual(min_calls, 0)
        self.assertEqual(schedule[0].start.month, 1)
        self.assertEqual(schedule[0].end.month, 1)

    def test_optimise_leading_trivial(self):
        schedule, min_calls = ScheduleOptimiser.optimise(
            self.max_per_call, self.max_per_query, self.historic_counts_trivial
        )
        self.assertEqual(len(schedule), 1)
        self.assertEqual(min_calls, 3)
        self.assertEqual(schedule[0].start.month, 1)
        self.assertEqual(schedule[0].end.month, 4)

    def test_optimise_historic_counts_case1(self):
        historic_counts = [
            PeriodCount(pendulum.Period(start=pendulum.date(1000, 1, 1), end=pendulum.date(1000, 1, 1)), 10),
            PeriodCount(pendulum.Period(start=pendulum.date(1000, 2, 1), end=pendulum.date(1000, 2, 1)), 1),
            PeriodCount(pendulum.Period(start=pendulum.date(1000, 3, 1), end=pendulum.date(1000, 3, 1)), 2),
            PeriodCount(pendulum.Period(start=pendulum.date(1000, 4, 1), end=pendulum.date(1000, 4, 1)), 3),
        ]

        schedule, min_calls = ScheduleOptimiser.optimise(self.max_per_call, self.max_per_query, historic_counts)
        self.assertEqual(len(schedule), 2)
        self.assertEqual(min_calls, 8)
        self.assertEqual(schedule[0].start.month, 1)
        self.assertEqual(schedule[0].end.month, 1)
        self.assertEqual(schedule[1].start.month, 2)
        self.assertEqual(schedule[1].end.month, 4)

    def test_optimise_historic_counts_case2(self):
        historic_counts = [
            PeriodCount(pendulum.Period(start=pendulum.date(1000, 1, 1), end=pendulum.date(1000, 1, 1)), 5),
            PeriodCount(pendulum.Period(start=pendulum.date(1000, 2, 1), end=pendulum.date(1000, 2, 1)), 6),
            PeriodCount(pendulum.Period(start=pendulum.date(1000, 3, 1), end=pendulum.date(1000, 3, 1)), 0),
            PeriodCount(pendulum.Period(start=pendulum.date(1000, 4, 1), end=pendulum.date(1000, 4, 1)), 10),
            PeriodCount(pendulum.Period(start=pendulum.date(1000, 5, 1), end=pendulum.date(1000, 5, 1)), 2),
        ]

        schedule, min_calls = ScheduleOptimiser.optimise(self.max_per_call, self.max_per_query, historic_counts)
        self.assertEqual(len(schedule), 4)  # Naive is 5
        self.assertEqual(min_calls, 12)  # Naive is 12
        self.assertEqual(schedule[0].start, pendulum.datetime(1000, 1, 1))
        self.assertEqual(schedule[0].end, pendulum.datetime(1000, 1, 1))
        self.assertEqual(schedule[1].start, pendulum.datetime(1000, 2, 1))
        self.assertEqual(schedule[1].end, pendulum.datetime(1000, 2, 1))
        self.assertEqual(schedule[2].start, pendulum.datetime(1000, 3, 1))
        self.assertEqual(schedule[2].end, pendulum.datetime(1000, 4, 1))
        self.assertEqual(schedule[3].start, pendulum.datetime(1000, 5, 1))
        self.assertEqual(schedule[3].end, pendulum.datetime(1000, 5, 1))

    def test_optimise_historic_counts_case3(self):
        historic_counts = [
            PeriodCount(pendulum.Period(start=pendulum.date(1000, 1, 1), end=pendulum.date(1000, 1, 1)), 1),
            PeriodCount(pendulum.Period(start=pendulum.date(1000, 2, 1), end=pendulum.date(1000, 2, 1)), 1),
            PeriodCount(pendulum.Period(start=pendulum.date(1000, 3, 1), end=pendulum.date(1000, 3, 1)), 0),
            PeriodCount(pendulum.Period(start=pendulum.date(1000, 4, 1), end=pendulum.date(1000, 4, 1)), 1),
            PeriodCount(pendulum.Period(start=pendulum.date(1000, 5, 1), end=pendulum.date(1000, 5, 1)), 2),
        ]

        schedule, min_calls = ScheduleOptimiser.optimise(self.max_per_call, self.max_per_query, historic_counts)
        self.assertEqual(len(schedule), 1)  # Naive is 5
        self.assertEqual(min_calls, 3)  # Naive is 5
        self.assertEqual(schedule[0].start, pendulum.datetime(1000, 1, 1))
        self.assertEqual(schedule[0].end, pendulum.datetime(1000, 5, 1))

    def test_optimise_historic_counts_case4(self):
        historic_counts = [
            PeriodCount(pendulum.Period(start=pendulum.date(1000, 1, 1), end=pendulum.date(1000, 1, 1)), 3),
            PeriodCount(pendulum.Period(start=pendulum.date(1000, 2, 1), end=pendulum.date(1000, 2, 1)), 3),
            PeriodCount(pendulum.Period(start=pendulum.date(1000, 3, 1), end=pendulum.date(1000, 3, 1)), 3),
            PeriodCount(pendulum.Period(start=pendulum.date(1000, 4, 1), end=pendulum.date(1000, 4, 1)), 1),
            PeriodCount(pendulum.Period(start=pendulum.date(1000, 5, 1), end=pendulum.date(1000, 5, 1)), 3),
        ]

        schedule, min_calls = ScheduleOptimiser.optimise(self.max_per_call, self.max_per_query, historic_counts)
        self.assertEqual(len(schedule), 2)  # Naive is 5
        self.assertEqual(min_calls, 7)  # Naive is 13
        self.assertEqual(schedule[0].start, pendulum.datetime(1000, 1, 1))
        self.assertEqual(schedule[0].end, pendulum.datetime(1000, 1, 1))
        self.assertEqual(schedule[1].start, pendulum.datetime(1000, 2, 1))
        self.assertEqual(schedule[1].end, pendulum.datetime(1000, 5, 1))
