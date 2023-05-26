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
import os.path
from unittest.mock import patch

import pendulum
from airflow.exceptions import AirflowException, AirflowSensorTimeout
from airflow.models import DagRun, DagModel
from airflow.utils.session import provide_session
from airflow.utils.state import DagRunState, State

from observatory.platform.observatory_environment import ObservatoryEnvironment, ObservatoryTestCase, make_dummy_dag
from observatory.platform.utils.dag_run_sensor import DagRunSensor
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

        self.add_operator(sensor)
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
        self.ext_dag_id = "dummy_dag"
        self.sensor_task_id = "sensor_task"

        super().__init__(*args, **kwargs)

    @provide_session
    def find_runs(self, session):
        return session.query(DagRun).all()

    @provide_session
    def update_db(self, *, session, object):
        session.merge(object)
        session.commit()

    def test_no_dag_exists(self):
        env = ObservatoryEnvironment()
        with env.create():
            execution_date = pendulum.datetime(2021, 9, 1)
            wf = MonitoringWorkflow(start_date=self.start_date, ext_dag_id="nodag", check_exists=True)
            dag = wf.make_dag()
            with env.create_dag_run(dag=dag, execution_date=execution_date):
                self.assertRaises(AirflowException, env.run_task, self.sensor_task_id)

    def test_no_dag_exists_no_check(self):
        env = ObservatoryEnvironment()
        with env.create():
            execution_date = pendulum.datetime(2021, 9, 1)
            wf = MonitoringWorkflow(start_date=self.start_date, ext_dag_id="nodag", check_exists=False)
            dag = wf.make_dag()
            with env.create_dag_run(dag=dag, execution_date=execution_date):
                ti = env.run_task(self.sensor_task_id)
                self.assertEqual(ti.state, State.SUCCESS)

    def test_no_execution_date_in_range(self):
        env = ObservatoryEnvironment()
        with env.create() as t:
            self.add_dummy_dag_model(t, self.ext_dag_id, "@weekly")

            execution_date = pendulum.datetime(2021, 9, 1)
            wf = MonitoringWorkflow(start_date=self.start_date, ext_dag_id=self.ext_dag_id)
            dag = wf.make_dag()
            with env.create_dag_run(dag=dag, execution_date=execution_date):
                ti = env.run_task(self.sensor_task_id)
                self.assertEqual(ti.state, State.SUCCESS)

    @patch("observatory.platform.utils.dag_run_sensor.DagRunSensor.get_latest_execution_date")
    def test_grace_period(self, m_get_execdate):
        m_get_execdate.return_value = None
        env = ObservatoryEnvironment()
        with env.create() as t:
            self.add_dummy_dag_model(t, self.ext_dag_id, "@weekly")

            execution_date = pendulum.datetime(2021, 9, 1)
            wf = MonitoringWorkflow(start_date=self.start_date, ext_dag_id=self.ext_dag_id)
            dag = wf.make_dag()
            with env.create_dag_run(dag=dag, execution_date=execution_date):
                ti = env.run_task(self.sensor_task_id)
                self.assertEqual(ti.state, State.SUCCESS)

            self.assertEqual(m_get_execdate.call_count, 2)

    def add_dummy_dag_model(self, t: str, dag_id: str, schedule_interval: str):
        model = DagModel()
        model.dag_id = dag_id
        model.schedule_interval = schedule_interval
        model.fileloc = os.path.join(t, "dummy_dag.py")
        open(model.fileloc, mode="a").close()
        self.update_db(object=model)

    def run_dummy_dag(
        self, env: ObservatoryEnvironment, execution_date: pendulum.DateTime, task_id: str = "dummy_task"
    ):
        dag = make_dummy_dag(self.ext_dag_id, execution_date)

        # Add DagModel to db
        self.add_dummy_dag_model(env.temp_dir, dag.dag_id, dag.schedule_interval)

        # Run DAG
        with env.create_dag_run(dag, execution_date):
            ti = env.run_task(task_id)
            self.assertEqual(State.SUCCESS, ti.state)

            dagruns = self.find_runs()
            self.assertEqual(dagruns[-1].execution_date, execution_date)

    def test_execution_on_oldest_boundary(self):
        env = ObservatoryEnvironment()
        with env.create():
            execution_date = pendulum.datetime(2021, 8, 25)
            self.run_dummy_dag(env, execution_date)

            execution_date = pendulum.datetime(2021, 9, 1)
            wf = MonitoringWorkflow(start_date=self.start_date, ext_dag_id=self.ext_dag_id)
            dag = wf.make_dag()
            with env.create_dag_run(dag=dag, execution_date=execution_date):
                ti = env.run_task(self.sensor_task_id)
                self.assertEqual(ti.state, State.SUCCESS)

    def test_execution_on_newest_boundary(self):
        env = ObservatoryEnvironment()
        with env.create():
            execution_date = pendulum.datetime(2021, 9, 1)
            self.run_dummy_dag(env, execution_date)

            execution_date = pendulum.datetime(2021, 9, 1)
            wf = MonitoringWorkflow(start_date=self.start_date, ext_dag_id=self.ext_dag_id)
            dag = wf.make_dag()
            with env.create_dag_run(dag=dag, execution_date=execution_date):
                ti = env.run_task(self.sensor_task_id)
                self.assertEqual(ti.state, State.SUCCESS)

    def test_execution_multiple_dagruns_last_success(self):
        env = ObservatoryEnvironment()
        with env.create():
            execution_date = pendulum.datetime(2021, 8, 25)
            self.run_dummy_dag(env, execution_date)

            execution_date = pendulum.datetime(2021, 9, 1)
            self.run_dummy_dag(env, execution_date)

            execution_date = pendulum.datetime(2021, 9, 1)
            wf = MonitoringWorkflow(start_date=self.start_date, ext_dag_id=self.ext_dag_id)
            dag = wf.make_dag()
            with env.create_dag_run(dag=dag, execution_date=execution_date):
                ti = env.run_task(self.sensor_task_id)
                self.assertEqual(ti.state, State.SUCCESS)

    def fail_last_dag_run(self):
        dagruns = self.find_runs()
        last_dag_run = dagruns[-1]
        last_dag_run.set_state(DagRunState.FAILED)
        self.update_db(object=last_dag_run)

    def test_execution_multiple_dagruns_last_fail_reschedule_mode(self):
        env = ObservatoryEnvironment()
        with env.create():
            execution_date = pendulum.datetime(2021, 8, 25)
            self.run_dummy_dag(env, execution_date)

            execution_date = pendulum.datetime(2021, 9, 1)
            self.run_dummy_dag(env, execution_date)
            self.fail_last_dag_run()

            execution_date = pendulum.datetime(2021, 9, 1)
            wf = MonitoringWorkflow(start_date=self.start_date, ext_dag_id=self.ext_dag_id)
            dag = wf.make_dag()
            with env.create_dag_run(dag=dag, execution_date=execution_date):
                ti = env.run_task(self.sensor_task_id)
                self.assertEqual(ti.state, "up_for_reschedule")

    def test_execution_multiple_dagruns_last_fail_poke_mode(self):
        env = ObservatoryEnvironment()
        with env.create():
            execution_date = pendulum.datetime(2021, 8, 25)
            self.run_dummy_dag(env, execution_date)

            execution_date = pendulum.datetime(2021, 9, 1)
            self.run_dummy_dag(env, execution_date)
            self.fail_last_dag_run()

            execution_date = pendulum.datetime(2021, 9, 1)
            wf = MonitoringWorkflow(start_date=self.start_date, ext_dag_id=self.ext_dag_id, mode="poke")
            dag = wf.make_dag()
            with env.create_dag_run(dag=dag, execution_date=execution_date):
                # ti = env.run_task(self.sensor_task_id)
                self.assertRaises(AirflowSensorTimeout, env.run_task, self.sensor_task_id)
