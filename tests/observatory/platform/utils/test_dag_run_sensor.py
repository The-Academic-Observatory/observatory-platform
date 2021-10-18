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
#
# Author: Tuan Chien

import datetime
from unittest.mock import patch

import pendulum
from airflow import DAG
from airflow.exceptions import AirflowException, AirflowSensorTimeout
from airflow.models import DagRun
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.session import provide_session
from airflow.utils.state import DagRunState, State

from observatory.platform.utils.dag_run_sensor import DagRunSensor
from observatory.platform.utils.test_utils import (
    ObservatoryEnvironment,
    ObservatoryTestCase,
)
from observatory.platform.workflows.workflow import Workflow


class MonitoringWorkflow(Workflow):
    DAG_ID = "test_workflow"

    def __init__(
        self,
        *,
        start_date: pendulum.DateTime,
        ext_dag_id: str,
        schedule_interval: str = "@monthly",
        mode: str = "reschedule",
        check_exists: bool = True,
    ):
        super().__init__(
            dag_id=MonitoringWorkflow.DAG_ID, start_date=start_date, schedule_interval=schedule_interval, catchup=False
        )

        sensor = DagRunSensor(
            task_id="sensor_task",
            external_dag_id=ext_dag_id,
            duration=datetime.timedelta(days=7),
            poke_interval=1,
            timeout=2,
            mode=mode,
            check_exists=check_exists,
            grace_period=datetime.timedelta(seconds=1),
        )

        self.add_sensor(sensor)
        self.add_task(self.dummy_task)

    def make_release(self, **kwargs):
        return None

    def dummy_task(self, release, **kwargs):
        if not self.succeed:
            raise ValueError("Problem")


class TestDagRunSensor(ObservatoryTestCase):
    """Test the Task Window Sensor.  We use one of the stock example dags"""

    def __init__(self, *args, **kwargs):
        self.start_date = pendulum.datetime(2021, 9, 1)

        super().__init__(*args, **kwargs)

    @provide_session
    def find_runs(self, session):
        return session.query(DagRun).all()

    @provide_session
    def update_db(self, *, session, object):
        session.merge(object)
        session.commit()

    def triggering_dag(self, *, dag_id="dag", execution_date):
        with DAG(
            dag_id=dag_id,
            schedule_interval="@monthly",
            default_args={
                "start_date": self.start_date,
            },
            catchup=False,
        ) as dag:
            task = TriggerDagRunOperator(
                trigger_dag_id="example_bash_operator", task_id="trigger_dag", execution_date=execution_date
            )

        return dag

    def test_no_dag_exists(self):
        env = ObservatoryEnvironment()
        with env.create():
            execution_date = pendulum.datetime(2021, 9, 1)
            wf = MonitoringWorkflow(start_date=self.start_date, ext_dag_id="nodag", check_exists=True)
            dag = wf.make_dag()
            with env.create_dag_run(dag=dag, execution_date=execution_date):
                self.assertRaises(AirflowException, env.run_task, "sensor_task")

    def test_no_dag_exists_no_check(self):
        env = ObservatoryEnvironment()
        with env.create():
            execution_date = pendulum.datetime(2021, 9, 1)
            wf = MonitoringWorkflow(start_date=self.start_date, ext_dag_id="nodag", check_exists=False)
            dag = wf.make_dag()
            with env.create_dag_run(dag=dag, execution_date=execution_date):
                ti = env.run_task("sensor_task")
                self.assertEqual(ti.state, State.SUCCESS)

    def test_no_execution_date_in_range(self):
        env = ObservatoryEnvironment()
        with env.create():
            execution_date = pendulum.datetime(2021, 9, 1)
            wf = MonitoringWorkflow(start_date=self.start_date, ext_dag_id="example_bash_operator")
            dag = wf.make_dag()
            with env.create_dag_run(dag=dag, execution_date=execution_date):
                ti = env.run_task("sensor_task")
                self.assertEqual(ti.state, State.SUCCESS)

    @patch("observatory.platform.utils.dag_run_sensor.DagRunSensor.get_latest_execution_date")
    def test_grace_period(self, m_get_execdate):
        m_get_execdate.return_value = None
        env = ObservatoryEnvironment()
        with env.create():
            execution_date = pendulum.datetime(2021, 9, 1)
            wf = MonitoringWorkflow(start_date=self.start_date, ext_dag_id="example_bash_operator")
            dag = wf.make_dag()
            with env.create_dag_run(dag=dag, execution_date=execution_date):
                ti = env.run_task("sensor_task")
                self.assertEqual(ti.state, State.SUCCESS)

            self.assertEqual(m_get_execdate.call_count, 2)

    def test_execution_on_oldest_boundary(self):
        env = ObservatoryEnvironment()
        with env.create():
            execution_date = pendulum.datetime(2021, 8, 25)
            dag = self.triggering_dag(execution_date=execution_date)
            with env.create_dag_run(dag=dag, execution_date=execution_date):
                ti = env.run_task("trigger_dag")
                self.assertEqual(ti.state, State.SUCCESS)

                dagruns = self.find_runs()
                self.assertEqual(dagruns[1].execution_date, execution_date)
                dagruns[1].set_state(DagRunState.SUCCESS)
                self.update_db(object=dagruns[1])

            execution_date = pendulum.datetime(2021, 9, 1)

            wf = MonitoringWorkflow(start_date=self.start_date, ext_dag_id="example_bash_operator")
            dag = wf.make_dag()
            with env.create_dag_run(dag=dag, execution_date=execution_date):
                ti = env.run_task("sensor_task")
                self.assertEqual(ti.state, State.SUCCESS)

    def test_execution_on_newest_boundary(self):
        env = ObservatoryEnvironment()
        with env.create():
            execution_date = pendulum.datetime(2021, 9, 1)
            dag = self.triggering_dag(execution_date=execution_date)
            with env.create_dag_run(dag=dag, execution_date=execution_date):
                ti = env.run_task("trigger_dag")
                self.assertEqual(ti.state, State.SUCCESS)

                dagruns = self.find_runs()
                self.assertEqual(dagruns[1].execution_date, execution_date)
                dagruns[1].set_state(DagRunState.SUCCESS)
                self.update_db(object=dagruns[1])

            execution_date = pendulum.datetime(2021, 9, 1)

            wf = MonitoringWorkflow(start_date=self.start_date, ext_dag_id="example_bash_operator")
            dag = wf.make_dag()
            with env.create_dag_run(dag=dag, execution_date=execution_date):
                ti = env.run_task("sensor_task")
                self.assertEqual(ti.state, State.SUCCESS)

    def test_execution_multiple_dagruns_last_success(self):
        env = ObservatoryEnvironment()
        with env.create():
            execution_date = pendulum.datetime(2021, 8, 25)
            dag = self.triggering_dag(execution_date=execution_date)
            with env.create_dag_run(dag=dag, execution_date=execution_date):
                ti = env.run_task("trigger_dag")
                self.assertEqual(ti.state, State.SUCCESS)

                dagruns = self.find_runs()
                self.assertEqual(dagruns[1].execution_date, execution_date)
                dagruns[1].set_state(DagRunState.SUCCESS)
                self.update_db(object=dagruns[1])

            execution_date = pendulum.datetime(2021, 9, 1)
            dag = self.triggering_dag(execution_date=execution_date)
            with env.create_dag_run(dag=dag, execution_date=execution_date):
                ti = env.run_task("trigger_dag")
                self.assertEqual(ti.state, State.SUCCESS)

                dagruns = self.find_runs()
                self.assertEqual(dagruns[3].execution_date, execution_date)
                dagruns[3].set_state(DagRunState.SUCCESS)
                self.update_db(object=dagruns[3])

            execution_date = pendulum.datetime(2021, 9, 1)

            wf = MonitoringWorkflow(start_date=self.start_date, ext_dag_id="example_bash_operator")
            dag = wf.make_dag()
            with env.create_dag_run(dag=dag, execution_date=execution_date):
                ti = env.run_task("sensor_task")
                self.assertEqual(ti.state, State.SUCCESS)

    def test_execution_multiple_dagruns_last_fail_reschedule_mode(self):
        env = ObservatoryEnvironment()
        with env.create():
            execution_date = pendulum.datetime(2021, 8, 25)
            dag = self.triggering_dag(execution_date=execution_date)
            with env.create_dag_run(dag=dag, execution_date=execution_date):
                ti = env.run_task("trigger_dag")
                self.assertEqual(ti.state, State.SUCCESS)

                dagruns = self.find_runs()
                self.assertEqual(dagruns[1].execution_date, execution_date)
                dagruns[1].set_state(DagRunState.SUCCESS)
                self.update_db(object=dagruns[1])

            execution_date = pendulum.datetime(2021, 9, 1)
            dag = self.triggering_dag(execution_date=execution_date)
            with env.create_dag_run(dag=dag, execution_date=execution_date):
                ti = env.run_task("trigger_dag")
                self.assertEqual(ti.state, State.SUCCESS)

                dagruns = self.find_runs()
                self.assertEqual(dagruns[3].execution_date, execution_date)
                dagruns[3].set_state(DagRunState.FAILED)
                self.update_db(object=dagruns[3])

            execution_date = pendulum.datetime(2021, 9, 1)
            wf = MonitoringWorkflow(start_date=self.start_date, ext_dag_id="example_bash_operator")
            dag = wf.make_dag()
            with env.create_dag_run(dag=dag, execution_date=execution_date):
                ti = env.run_task("sensor_task")
                self.assertEqual(ti.state, "up_for_reschedule")

    def test_execution_multiple_dagruns_last_fail_poke_mode(self):
        env = ObservatoryEnvironment()
        with env.create():
            execution_date = pendulum.datetime(2021, 8, 25)
            dag = self.triggering_dag(execution_date=execution_date)
            with env.create_dag_run(dag=dag, execution_date=execution_date):
                ti = env.run_task("trigger_dag")
                self.assertEqual(ti.state, State.SUCCESS)

                dagruns = self.find_runs()
                self.assertEqual(dagruns[1].execution_date, execution_date)
                dagruns[1].set_state(DagRunState.SUCCESS)
                self.update_db(object=dagruns[1])

            execution_date = pendulum.datetime(2021, 9, 1)
            dag = self.triggering_dag(execution_date=execution_date)
            with env.create_dag_run(dag=dag, execution_date=execution_date):
                ti = env.run_task("trigger_dag")
                self.assertEqual(ti.state, State.SUCCESS)

                dagruns = self.find_runs()
                self.assertEqual(dagruns[3].execution_date, execution_date)
                dagruns[3].set_state(DagRunState.FAILED)
                self.update_db(object=dagruns[3])

            execution_date = pendulum.datetime(2021, 9, 1)
            wf = MonitoringWorkflow(start_date=self.start_date, ext_dag_id="example_bash_operator", mode="poke")
            dag = wf.make_dag()
            with env.create_dag_run(dag=dag, execution_date=execution_date):
                # ti = env.run_task("sensor_task")
                self.assertRaises(AirflowSensorTimeout, env.run_task, "sensor_task")
