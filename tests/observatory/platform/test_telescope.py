# Copyright 2021 Curtin University
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

# Author: Tuan Chien

from datetime import datetime, timezone, timedelta
from airflow.models.connection import Connection
from airflow.sensors.time_delta_sensor import TimeDeltaSensor
from observatory.platform.telescopes.telescope import Telescope
from observatory.platform.utils.airflow_utils import AirflowConns
from observatory.platform.utils.gc_utils import bigquery_partitioned_table_id
from observatory.platform.utils.template_utils import telescope_path, SubFolder, blob_name
from observatory.platform.utils.test_utils import (ObservatoryEnvironment, ObservatoryTestCase,
                                                   test_fixtures_path, module_file_path)


class MockTelescope(Telescope):
    """
    Generic Workflow telescope for running tasks.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def make_release(self) -> None:
        """ Not needed. """
        return None


class TestAddSensorsTelescope(ObservatoryTestCase):
    """ Tests the sensor interface. """

    def __init__(self, *args, **kwargs):
        """ Constructor which sets up variables used by tests.

        :param args: arguments.
        :param kwargs: keyword arguments.
        """

        super().__init__(*args, **kwargs)

    def dummy_func(self):
        pass

    def test_add_sensor(self):
        mt = MockTelescope(dag_id='1', start_date=datetime(
            1970, 1, 1, 0, 0, tzinfo=timezone.utc), schedule_interval='daily')
        mt.add_task(self.dummy_func)
        tds = TimeDeltaSensor(delta=timedelta(seconds=5), task_id='test', start_date=datetime(
            1970, 1, 1, 0, 0, tzinfo=timezone.utc))
        tds2 = TimeDeltaSensor(delta=timedelta(seconds=5), task_id='test2', start_date=datetime(
            1970, 1, 1, 0, 0, tzinfo=timezone.utc))
        mt.add_sensor(tds)
        mt.add_sensor(tds2)
        dag = mt.make_dag()

        self.assert_dag_structure(
            {
                'dummy_func': [],
                'test': ['dummy_func'],
                'test2': ['dummy_func']
            }, dag)

    def test_add_sensors(self):
        mt = MockTelescope(dag_id='1', start_date=datetime(
            1970, 1, 1, 0, 0, tzinfo=timezone.utc), schedule_interval='daily')
        mt.add_task(self.dummy_func)
        tds = TimeDeltaSensor(delta=timedelta(seconds=5), task_id='test', start_date=datetime(
            1970, 1, 1, 0, 0, tzinfo=timezone.utc))
        tds2 = TimeDeltaSensor(delta=timedelta(seconds=5), task_id='test2', start_date=datetime(
            1970, 1, 1, 0, 0, tzinfo=timezone.utc))
        mt.add_sensors([tds, tds2])
        dag = mt.make_dag()

        self.assert_dag_structure(
            {
                'dummy_func': [],
                'test': ['dummy_func'],
                'test2': ['dummy_func']
            }, dag)

    def test_add_sensors_empty(self):
        mt = MockTelescope(dag_id='1', start_date=datetime(
            1970, 1, 1, 0, 0, tzinfo=timezone.utc), schedule_interval='daily')
        mt.add_task(self.dummy_func)
        mt.add_sensors([])
        dag = mt.make_dag()

        self.assert_dag_structure(
            {
                'dummy_func': [],
            }, dag)
