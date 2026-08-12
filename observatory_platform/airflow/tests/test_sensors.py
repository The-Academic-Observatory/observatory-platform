from unittest.mock import MagicMock

import pendulum
from airflow.models import DagRun
from airflow.utils.session import create_session
from airflow.utils.state import DagRunState, TaskInstanceState
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
    """Insert a bare DagRun row for `dag_id` directly. This is only used to give get_logical_dates()
    something real to query -- poke()'s own state-matching now goes through ti.get_dr_count(), which is
    mocked separately, not through this row's `state`."""
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
    """get_logical_dates queries the DagRun table directly -- unaffected by the Task SDK changes to poke()."""

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
    """Test poke()'s decision logic. In Airflow 3, poke() gets state counts from ti.get_dr_count(...) rather
    than a direct DB query, so ti is mocked here to control that count directly."""

    def _make_sensor(self, external_dag_id: str) -> DagCompleteSensor:
        return DagCompleteSensor(
            task_id=f"{external_dag_id}_sensor",
            external_dag_id=external_dag_id,
            mode="reschedule",
            check_existence=False,
        )

    def test_poke_true_when_matching_run_succeeded(self):
        env = SandboxEnvironment()
        with env.create():
            # Real row so get_logical_dates finds a date to check
            add_dag_run(
                dag_id="crossref_metadata",
                logical_date=pendulum.datetime(2024, 2, 7),
                data_interval_end=pendulum.datetime(2024, 2, 7),
            )

            sensor = self._make_sensor("crossref_metadata")
            ti = MagicMock()
            ti.get_dr_count.return_value = 1  # 1 of 1 matching date is in allowed_states (SUCCESS)

            context = {
                "ti": ti,
                "logical_date": pendulum.datetime(2024, 2, 4),
                "data_interval_end": pendulum.datetime(2024, 2, 11),
            }

            self.assertTrue(sensor.poke(context))
            ti.get_dr_count.assert_called_once_with(
                dag_id="crossref_metadata",
                logical_dates=[pendulum.datetime(2024, 2, 7)],
                states=[TaskInstanceState.SUCCESS.value],
            )

    def test_poke_false_when_matching_run_not_successful(self):
        env = SandboxEnvironment()
        with env.create():
            add_dag_run(
                dag_id="crossref_metadata",
                logical_date=pendulum.datetime(2024, 2, 7),
                data_interval_end=pendulum.datetime(2024, 2, 7),
            )

            sensor = self._make_sensor("crossref_metadata")
            ti = MagicMock()
            ti.get_dr_count.return_value = 0  # matching date exists but isn't in allowed_states yet

            context = {
                "ti": ti,
                "logical_date": pendulum.datetime(2024, 2, 4),
                "data_interval_end": pendulum.datetime(2024, 2, 11),
            }

            self.assertFalse(sensor.poke(context))

    def test_poke_true_when_no_matching_run_exists(self):
        """No prior external DAG run to wait on at all -- nothing blocks progress."""
        env = SandboxEnvironment()
        with env.create():
            sensor = self._make_sensor("crossref_metadata")
            ti = MagicMock()
            ti.get_dr_count.return_value = 0

            context = {
                "ti": ti,
                "logical_date": pendulum.datetime(2024, 2, 4),
                "data_interval_end": pendulum.datetime(2024, 2, 11),
            }

            self.assertTrue(sensor.poke(context))
            ti.get_dr_count.assert_called_once_with(
                dag_id="crossref_metadata", logical_dates=[], states=[TaskInstanceState.SUCCESS.value]
            )
