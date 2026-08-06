# Copyright 2020, 2021 Curtin University
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
# Author: Tuan Chien, Keegan Smith, Jamie Diprose

from __future__ import annotations

import pendulum
from airflow.models import DagRun
from airflow.utils.session import create_session
from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunTriggeredByType, DagRunType

from observatory_platform.airflow.sensors import DagCompleteSensor, get_logical_dates
from observatory_platform.sandbox.sandbox_environment import SandboxEnvironment
from observatory_platform.sandbox.test_utils import SandboxTestCase


def add_dag_run(
    *,
    dag_id: str,
    logical_date: pendulum.DateTime,
    data_interval_end: pendulum.DateTime,
    state: str = DagRunState.SUCCESS,
) -> DagRun:
    """Insert a bare DagRun row for `dag_id` directly, without running anything.

    This is a plain ORM insert -- it doesn't touch scheduler/execution internals, so it isn't affected by the
    Airflow 3 changes to create_dagrun/verify_integrity/etc. It's enough to satisfy get_logical_dates() and
    ExternalTaskSensor.poke(), since both just query the DagRun table.
    """
    with create_session() as session:
        dagrun = DagRun(
            dag_id=dag_id,
            run_id=f"test__{logical_date.isoformat()}",
            logical_date=logical_date,
            data_interval=(logical_date, data_interval_end),
            run_type=DagRunType.MANUAL,
            state=state,
            triggered_by=DagRunTriggeredByType.TEST,
        )
        session.add(dagrun)
        session.commit()
        session.refresh(dagrun)
    return dagrun


class TestGetLogicalDates(SandboxTestCase):
    """Test the get_logical_dates function directly -- this is the core date-matching logic, and needs nothing
    more than real DagRun rows in the DB."""

    def test_returns_most_recent_run_before_data_interval_end(self):
        env = SandboxEnvironment()
        with env.create():
            add_dag_run(
                dag_id="crossref_metadata",
                logical_date=pendulum.datetime(2024, 1, 7),
                data_interval_end=pendulum.datetime(2024, 1, 7),
            )
            add_dag_run(
                dag_id="crossref_metadata",
                logical_date=pendulum.datetime(2024, 2, 7),
                data_interval_end=pendulum.datetime(2024, 2, 7),
            )

            dates = get_logical_dates(
                external_dag_id="crossref_metadata",
                logical_date=pendulum.datetime(2024, 2, 4),
                data_interval_end=pendulum.datetime(2024, 2, 11),
            )

            self.assertEqual([pendulum.datetime(2024, 2, 7)], dates)

    def test_returns_empty_when_no_matching_runs(self):
        env = SandboxEnvironment()
        with env.create():
            dates = get_logical_dates(
                external_dag_id="crossref_metadata",
                logical_date=pendulum.datetime(2024, 2, 4),
                data_interval_end=pendulum.datetime(2024, 2, 11),
            )

            self.assertEqual([], dates)


class TestDagCompleteSensor(SandboxTestCase):
    """Test the sensor's poke() behaviour directly, without running it as part of a real DAG/scheduler."""

    def _make_sensor(self, external_dag_id: str) -> DagCompleteSensor:
        return DagCompleteSensor(
            task_id=f"{external_dag_id}_sensor",
            external_dag_id=external_dag_id,
            mode="reschedule",
            check_existence=False,  # avoid needing a DagModel row for this
        )

    def test_poke_true_when_external_run_succeeded(self):
        env = SandboxEnvironment()
        with env.create():
            add_dag_run(
                dag_id="crossref_metadata",
                logical_date=pendulum.datetime(2024, 2, 7),
                data_interval_end=pendulum.datetime(2024, 2, 7),
                state=DagRunState.SUCCESS,
            )

            sensor = self._make_sensor("crossref_metadata")
            context = {
                "logical_date": pendulum.datetime(2024, 2, 4),
                "data_interval_end": pendulum.datetime(2024, 2, 11),
            }

            self.assertTrue(sensor.poke(context))

    def test_poke_false_when_external_run_failed(self):
        env = SandboxEnvironment()
        with env.create():
            add_dag_run(
                dag_id="crossref_metadata",
                logical_date=pendulum.datetime(2024, 2, 7),
                data_interval_end=pendulum.datetime(2024, 2, 7),
                state=DagRunState.FAILED,
            )

            sensor = self._make_sensor("crossref_metadata")
            context = {
                "logical_date": pendulum.datetime(2024, 2, 4),
                "data_interval_end": pendulum.datetime(2024, 2, 11),
            }

            self.assertFalse(sensor.poke(context))

    def test_poke_false_when_no_external_run_yet(self):
        env = SandboxEnvironment()
        with env.create():
            sensor = self._make_sensor("crossref_metadata")
            context = {
                "logical_date": pendulum.datetime(2024, 2, 4),
                "data_interval_end": pendulum.datetime(2024, 2, 11),
            }

            self.assertFalse(sensor.poke(context))
