import unittest
from unittest.mock import MagicMock, patch

from airflow.exceptions import AirflowException

from observatory_platform.airflow.sensors import DagCompleteSensor

EXTERNAL_DAG_ID = "some_external_dag"
API_BASE_URL = "https://example-airflow.astronomer.run"


def make_sensor(**overrides):
    """Helper to build a sensor instance with sensible test defaults."""
    kwargs = {
        "task_id": "wait_for_external_dag",
        "external_dag_id": EXTERNAL_DAG_ID,
        "conn_id": "airflow_api",
    }
    kwargs.update(overrides)
    return DagCompleteSensor(**kwargs)


def make_connection(host=API_BASE_URL, password="fake-token"):  # noqa: S106 - test fixture, not a real secret
    conn = MagicMock()
    conn.host = host
    conn.password = password
    return conn


def make_response(dag_runs, status_code=200):
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = {"dag_runs": dag_runs}
    resp.raise_for_status.side_effect = None
    return resp


class TestDagCompleteSensor(unittest.TestCase):
    @patch("observatory_platform.airflow.sensors.requests.get")
    @patch("observatory_platform.airflow.sensors.BaseHook.get_connection")
    def test_poke_returns_false_when_no_dag_runs_exist(self, mock_get_connection, mock_get):
        mock_get_connection.return_value = make_connection()
        mock_get.return_value = make_response(dag_runs=[])

        sensor = make_sensor()
        result = sensor.poke(context={})

        self.assertFalse(result)

    @patch("observatory_platform.airflow.sensors.requests.get")
    @patch("observatory_platform.airflow.sensors.BaseHook.get_connection")
    def test_poke_returns_true_when_latest_run_succeeded(self, mock_get_connection, mock_get):
        mock_get_connection.return_value = make_connection()
        mock_get.return_value = make_response(
            dag_runs=[{"logical_date": "2026-09-10T00:00:00+00:00", "state": "success"}]
        )

        sensor = make_sensor()
        result = sensor.poke(context={})

        self.assertTrue(result)

    @patch("observatory_platform.airflow.sensors.requests.get")
    @patch("observatory_platform.airflow.sensors.BaseHook.get_connection")
    def test_poke_returns_false_when_latest_run_still_running(self, mock_get_connection, mock_get):
        mock_get_connection.return_value = make_connection()
        mock_get.return_value = make_response(
            dag_runs=[{"logical_date": "2026-09-10T00:00:00+00:00", "state": "running"}]
        )

        sensor = make_sensor()
        result = sensor.poke(context={})

        self.assertFalse(result)

    @patch("observatory_platform.airflow.sensors.requests.get")
    @patch("observatory_platform.airflow.sensors.BaseHook.get_connection")
    def test_poke_raises_when_latest_run_failed(self, mock_get_connection, mock_get):
        mock_get_connection.return_value = make_connection()
        mock_get.return_value = make_response(
            dag_runs=[{"logical_date": "2026-09-10T00:00:00+00:00", "state": "failed"}]
        )

        sensor = make_sensor()

        with self.assertRaises(AirflowException) as ctx:
            sensor.poke(context={})

        self.assertIn(EXTERNAL_DAG_ID, str(ctx.exception))

    @patch("observatory_platform.airflow.sensors.requests.get")
    @patch("observatory_platform.airflow.sensors.BaseHook.get_connection")
    def test_poke_respects_custom_allowed_and_failed_states(self, mock_get_connection, mock_get):
        mock_get_connection.return_value = make_connection()
        # "up_for_retry" is not a normally-terminal state, but let's say this DAG
        # treats it as an allowed completion state for this particular check.
        mock_get.return_value = make_response(
            dag_runs=[{"logical_date": "2026-09-10T00:00:00+00:00", "state": "up_for_retry"}]
        )

        sensor = make_sensor(allowed_states=["success", "up_for_retry"], failed_states=["failed"])
        result = sensor.poke(context={})

        self.assertTrue(result)

    @patch("observatory_platform.airflow.sensors.requests.get")
    @patch("observatory_platform.airflow.sensors.BaseHook.get_connection")
    def test_poke_calls_api_with_expected_url_and_params(self, mock_get_connection, mock_get):
        mock_get_connection.return_value = make_connection(host=API_BASE_URL + "/")  # trailing slash
        mock_get.return_value = make_response(
            dag_runs=[{"logical_date": "2026-09-10T00:00:00+00:00", "state": "success"}]
        )

        sensor = make_sensor()
        sensor.poke(context={})

        mock_get.assert_called_once()
        called_args, called_kwargs = mock_get.call_args

        expected_url = f"{API_BASE_URL}/api/v2/dags/{EXTERNAL_DAG_ID}/dagRuns"
        self.assertEqual(called_args[0], expected_url)
        self.assertEqual(called_kwargs["params"], {"order_by": "-logical_date", "limit": 1})
        self.assertEqual(called_kwargs["headers"]["Authorization"], "Bearer fake-token")

    @patch("observatory_platform.airflow.sensors.requests.get")
    @patch("observatory_platform.airflow.sensors.BaseHook.get_connection")
    def test_poke_propagates_http_errors(self, mock_get_connection, mock_get):
        mock_get_connection.return_value = make_connection()
        error_response = MagicMock()
        error_response.raise_for_status.side_effect = Exception("500 Server Error")
        mock_get.return_value = error_response

        sensor = make_sensor()

        with self.assertRaises(Exception) as ctx:
            sensor.poke(context={})

        self.assertIn("500 Server Error", str(ctx.exception))

    @patch("observatory_platform.airflow.sensors.requests.get")
    @patch("observatory_platform.airflow.sensors.BaseHook.get_connection")
    def test_poke_picks_only_the_first_returned_run(self, mock_get_connection, mock_get):
        """
        The API is queried with limit=1 and order_by=-logical_date, so the sensor should
        always act on dag_runs[0] without needing to sort or filter further itself.
        """
        mock_get_connection.return_value = make_connection()
        mock_get.return_value = make_response(
            dag_runs=[{"logical_date": "2026-09-10T00:00:00+00:00", "state": "success"}]
        )

        sensor = make_sensor()
        result = sensor.poke(context={})

        self.assertTrue(result)
        _, called_kwargs = mock_get.call_args
        self.assertEqual(called_kwargs["params"]["limit"], 1)
