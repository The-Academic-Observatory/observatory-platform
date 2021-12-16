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

import datetime
import os
from unittest.mock import patch

import pendulum
from airflow.models import XCom
from airflow.models.connection import Connection
from airflow.models.variable import Variable
from airflow.utils.session import provide_session
from airflow.utils.state import DagRunState, State
from observatory.platform.observatory_config import VirtualMachine
from observatory.platform.terraform_api import TerraformVariable
from observatory.platform.utils.airflow_utils import AirflowConns, AirflowVars
from observatory.platform.utils.test_utils import (
    ObservatoryEnvironment,
    ObservatoryTestCase,
    module_file_path,
)
from observatory.platform.workflows.vm_workflow import (
    TerraformRelease,
    VmCreateWorkflow,
    VmDestroyWorkflow,
    parse_datetime,
)


@provide_session
def xcom_count(*, execution_date, dag_ids, session=None):
    return XCom.get_many(
        execution_date=execution_date,
        dag_ids=dag_ids,
        include_prior_dates=False,
        session=session,
    ).count()


@provide_session
def xcom_push(*, key, value, dag_id, task_id, execution_date, session=None):
    XCom.set(
        key=key,
        value=value,
        task_id=task_id,
        dag_id=dag_id,
        execution_date=execution_date,
        session=session,
    )


class TestVmDestroyWorkflow(ObservatoryTestCase):
    """Test the vm_destroy dag."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def setup_env(self, env, watch_list):
        var = Variable(key=AirflowVars.PROJECT_ID, val="project")
        env.add_variable(var)

        var = Variable(key=AirflowVars.TERRAFORM_ORGANIZATION, val="terraform_org")
        env.add_variable(var)

        var = Variable(key=AirflowVars.ENVIRONMENT, val="environment")
        env.add_variable(var)

        var = Variable(key=AirflowVars.VM_DAGS_WATCH_LIST, val=watch_list)
        env.add_variable(var)

        conn = Connection(conn_id=AirflowConns.TERRAFORM, uri="http://localhost")
        env.add_connection(conn)

    def test_dag_structure(self):
        """Test that vm_create has the correct structure.
        :return: None
        """

        dag = VmDestroyWorkflow().make_dag()
        self.assert_dag_structure(
            {
                "check_dependencies": ["check_vm_state"],
                "check_vm_state": ["check_dags_status"],
                "check_dags_status": ["update_terraform_variable"],
                "update_terraform_variable": ["run_terraform"],
                "run_terraform": ["check_run_status"],
                "check_run_status": ["cleanup"],
                "cleanup": [],
            },
            dag,
        )

    def test_dag_load(self):
        """Test that vm_create can be loaded from a DAG bag.
        :return: None
        """

        with ObservatoryEnvironment().create():
            dag_file = os.path.join(module_file_path("observatory.platform.dags"), "vm_destroy.py")
            self.assert_dag_load("vm_destroy", dag_file)

    def test_parse_datetime(self):
        expected = pendulum.datetime(2021, 1, 1)
        actual = parse_datetime("2021-01-01")
        self.assertEqual(expected, actual)

    @patch("observatory.platform.workflows.vm_workflow.DagRun.find")
    def test_get_last_execution_prev(self, m_drfind):
        workflow = VmDestroyWorkflow()

        class MockDagRun:
            def __init__(self, *, start_date, execution_date, state):
                self.start_date = start_date
                self.execution_date = execution_date
                self.state = state

        class MockDag:
            def __init__(self):
                self.default_args = {"start_date": datetime.datetime(2000, 1, 1)}

        # No dag runs, no prev start
        m_drfind.return_value = []
        ts = workflow._get_last_execution_prev(dag=MockDag(), dag_id="dagid", prev_start_time_vm=None)
        self.assertEqual(ts, datetime.datetime(2000, 1, 1))

        # No dag runs, prev start
        m_drfind.return_value = []
        ts = workflow._get_last_execution_prev(
            dag=MockDag(), dag_id="dagid", prev_start_time_vm=pendulum.datetime(2001, 1, 1)
        )
        self.assertEqual(ts, datetime.datetime(2000, 1, 1))

        # Dag run running
        m_drfind.return_value = [
            MockDagRun(
                start_date=datetime.datetime(2000, 2, 1),
                execution_date=datetime.datetime(2000, 2, 1),
                state=DagRunState.RUNNING,
            )
        ]
        ts = workflow._get_last_execution_prev(
            dag=MockDag(), dag_id="dagid", prev_start_time_vm=pendulum.datetime(2001, 1, 1)
        )
        self.assertIsNone(ts)

        # Dag run success, no prev start time vm
        m_drfind.return_value = [
            MockDagRun(
                start_date=datetime.datetime(2000, 2, 1),
                execution_date=datetime.datetime(2000, 3, 1),
                state=DagRunState.SUCCESS,
            )
        ]
        ts = workflow._get_last_execution_prev(
            dag=MockDag(),
            dag_id="dagid",
            prev_start_time_vm=None,
        )
        self.assertEqual(ts, datetime.datetime(2000, 1, 1))

        # Dag run success, dag run start date before prev start time vm
        m_drfind.return_value = [
            MockDagRun(
                start_date=datetime.datetime(2000, 2, 1),
                execution_date=datetime.datetime(2000, 3, 1),
                state=DagRunState.SUCCESS,
            )
        ]
        ts = workflow._get_last_execution_prev(
            dag=MockDag(), dag_id="dagid", prev_start_time_vm=pendulum.datetime(2001, 1, 1)
        )
        self.assertEqual(ts, datetime.datetime(2000, 3, 1))

        # Dag run success, dag run start date after prev start time vm
        m_drfind.return_value = [
            MockDagRun(
                start_date=datetime.datetime(2002, 2, 1),
                execution_date=datetime.datetime(2000, 3, 1),
                state=DagRunState.SUCCESS,
            )
        ]
        ts = workflow._get_last_execution_prev(
            dag=MockDag(), dag_id="dagid", prev_start_time_vm=pendulum.datetime(2001, 1, 1)
        )
        self.assertEqual(ts, datetime.datetime(2000, 1, 1))

    @patch("observatory.platform.workflows.vm_workflow.DagRun.find")
    def test_check_success_run(self, m_drfind):
        workflow = VmDestroyWorkflow()

        class MockDagRun:
            def __init__(self, *, start_date, execution_date, state):
                self.start_date = start_date
                self.execution_date = execution_date
                self.state = state
                self.dag_id = "dagid"

        # No dates
        m_drfind.return_value = []
        execution_dates = []
        status = workflow._check_success_runs(dag_id="dagid", execution_dates=execution_dates)
        self.assertTrue(status)

        # Date, no runs
        m_drfind.return_value = None
        execution_dates = [datetime.datetime(2000, 1, 1)]
        status = workflow._check_success_runs(dag_id="dagid", execution_dates=execution_dates)
        self.assertFalse(status)

        # Date, run not success
        m_drfind.return_value = [
            MockDagRun(
                start_date=datetime.datetime(2000, 2, 1),
                execution_date=datetime.datetime(2000, 3, 1),
                state=DagRunState.FAILED,
            )
        ]
        execution_dates = [datetime.datetime(2000, 1, 1)]
        status = workflow._check_success_runs(dag_id="dagid", execution_dates=execution_dates)
        self.assertFalse(status)

        # Date, run success
        m_drfind.return_value = [
            MockDagRun(
                start_date=datetime.datetime(2000, 2, 1),
                execution_date=datetime.datetime(2000, 3, 1),
                state=DagRunState.SUCCESS,
            )
        ]
        execution_dates = [datetime.datetime(2000, 1, 1)]
        status = workflow._check_success_runs(dag_id="dagid", execution_dates=execution_dates)
        self.assertTrue(status)

    @patch("observatory.platform.workflows.vm_workflow.TerraformApi.list_workspace_variables")
    @patch("observatory.platform.workflows.vm_workflow.TerraformApi.workspace_id")
    def test_vm_destroy_vm_already_off(self, m_tapi, m_list_workspace_vars):
        """Test the vm_destroy workflow"""

        m_tapi.return_value = "workspace"

        vm = VirtualMachine(machine_type="vm_type", disk_size=10, disk_type="ssd", create=False)
        m_list_workspace_vars.return_value = [
            TerraformVariable(
                key="airflow_worker_vm",
                value=vm.to_hcl(),
                hcl=True,
            )
        ]

        env = ObservatoryEnvironment()
        with env.create():
            workflow = VmDestroyWorkflow()
            dag = workflow.make_dag()
            execution_date = pendulum.datetime(2021, 1, 1)
            self.setup_env(env, "[]")

            with env.create_dag_run(dag, execution_date):
                # check dependencies
                ti = env.run_task(workflow.check_dependencies.__name__)
                self.assertEqual(ti.state, State.SUCCESS)

                # check vm state
                ti = env.run_task(workflow.check_vm_state.__name__)
                self.assertEqual(ti.state, State.SUCCESS)

                # check dags status
                ti = env.run_task(workflow.check_dags_status.__name__)
                self.assertEqual(ti.state, State.SKIPPED)

    @patch("observatory.platform.workflows.vm_workflow.TerraformApi.list_workspace_variables")
    @patch("observatory.platform.workflows.vm_workflow.TerraformApi.workspace_id")
    def test_vm_destroy_empty_watchlist(self, m_tapi, m_list_workspace_vars):
        """Test the vm_destroy workflow"""

        m_tapi.return_value = "workspace"

        vm = VirtualMachine(machine_type="vm_type", disk_size=10, disk_type="ssd", create=True)
        m_list_workspace_vars.return_value = [
            TerraformVariable(
                key="airflow_worker_vm",
                value=vm.to_hcl(),
                hcl=True,
            )
        ]

        env = ObservatoryEnvironment()
        with env.create():
            workflow = VmDestroyWorkflow()
            dag = workflow.make_dag()
            execution_date = pendulum.datetime(2021, 1, 1)
            self.setup_env(env, "[]")

            with env.create_dag_run(dag, execution_date):
                # check dependencies
                ti = env.run_task(workflow.check_dependencies.__name__)
                self.assertEqual(ti.state, State.SUCCESS)

                # check vm state
                ti = env.run_task(workflow.check_vm_state.__name__)
                self.assertEqual(ti.state, State.SUCCESS)

                # check dags status
                ti = env.run_task(workflow.check_dags_status.__name__)
                self.assertEqual(ti.state, State.SUCCESS)

                # update terraform variable
                with patch(
                    "observatory.platform.workflows.vm_workflow.TerraformRelease.update_terraform_vm_create_variable"
                ) as m_update:
                    ti = env.run_task(workflow.update_terraform_variable.__name__)
                    m_update.assert_called_once_with(False)
                self.assertEqual(ti.state, State.SUCCESS)

                #  run terraform
                with patch("observatory.platform.workflows.vm_workflow.TerraformApi.create_run") as m_create_run:
                    m_create_run.return_value = "run_id"
                    ti = env.run_task(workflow.run_terraform.__name__)
                    call_args, _ = m_create_run.call_args
                    self.assertEqual(call_args[0], "workspace")
                    self.assertEqual(call_args[1], "module.airflow_worker_vm")
                    self.assertEqual(ti.state, State.SUCCESS)

                # check run status
                with patch("observatory.platform.workflows.vm_workflow.TerraformApi.get_run_details") as m_run_details:
                    with patch("observatory.platform.workflows.vm_workflow.send_slack_msg") as m_slack:
                        m_run_details.return_value = {"data": {"attributes": {"status": "planned_and_finished"}}}

                        ti = env.run_task(workflow.check_run_status.__name__)
                        self.assertEqual(ti.state, State.SUCCESS)
                        _, kwargs = m_slack.call_args
                        self.assertEqual(kwargs["comments"], "Terraform run status: planned_and_finished")

                # cleanup
                ti = env.run_task(workflow.cleanup.__name__)
                self.assertEqual(ti.state, State.SUCCESS)
                self.assertEqual(
                    xcom_count(
                        execution_date=execution_date,
                        dag_ids=workflow.dag_id,
                    ),
                    2,
                )

    @patch("observatory.platform.workflows.vm_workflow.TerraformApi.list_workspace_variables")
    @patch("observatory.platform.workflows.vm_workflow.TerraformApi.workspace_id")
    def test_vm_destroy_manual_create(self, m_tapi, m_list_workspace_vars):
        """Test the vm_destroy workflow"""

        m_tapi.return_value = "workspace"

        vm = VirtualMachine(machine_type="vm_type", disk_size=10, disk_type="ssd", create=True)
        m_list_workspace_vars.return_value = [
            TerraformVariable(
                key="airflow_worker_vm",
                value=vm.to_hcl(),
                hcl=True,
            )
        ]

        env = ObservatoryEnvironment()
        with env.create():
            workflow = VmDestroyWorkflow()
            dag = workflow.make_dag()
            execution_date = pendulum.datetime(2021, 1, 1)
            self.setup_env(env, '["vm_destroy"]')

            with env.create_dag_run(dag, execution_date):
                # check dependencies
                ti = env.run_task(workflow.check_dependencies.__name__)
                self.assertEqual(ti.state, State.SUCCESS)

                # check vm state
                ti = env.run_task(workflow.check_vm_state.__name__)
                self.assertEqual(ti.state, State.SUCCESS)

                # check dags status
                ti = env.run_task(workflow.check_dags_status.__name__)
                self.assertEqual(ti.state, State.SUCCESS)

                # update terraform variable
                ti = env.run_task(workflow.update_terraform_variable.__name__)
                self.assertEqual(ti.state, State.SKIPPED)

    @patch("observatory.platform.workflows.vm_workflow.TerraformApi.list_workspace_variables")
    @patch("observatory.platform.workflows.vm_workflow.TerraformApi.workspace_id")
    def test_vm_destroy_prev_execution_and_start_time(self, m_tapi, m_list_workspace_vars):
        """Test the vm_destroy workflow"""

        m_tapi.return_value = "workspace"

        vm = VirtualMachine(machine_type="vm_type", disk_size=10, disk_type="ssd", create=True)
        m_list_workspace_vars.return_value = [
            TerraformVariable(
                key="airflow_worker_vm",
                value=vm.to_hcl(),
                hcl=True,
            )
        ]

        env = ObservatoryEnvironment()
        with env.create():
            workflow = VmDestroyWorkflow()
            dag = workflow.make_dag()
            execution_date = pendulum.datetime(2021, 1, 1)
            self.setup_env(env, '["vm_destroy"]')

            with env.create_dag_run(dag, execution_date):
                # check dependencies
                ti = env.run_task(workflow.check_dependencies.__name__)
                self.assertEqual(ti.state, State.SUCCESS)

                # check vm state
                ti = env.run_task(workflow.check_vm_state.__name__)
                self.assertEqual(ti.state, State.SUCCESS)

                # check dags status
                xcom_push(
                    dag_id=VmCreateWorkflow.DAG_ID,
                    task_id=VmCreateWorkflow.run_terraform.__name__,
                    execution_date=execution_date,
                    key=TerraformRelease.XCOM_START_TIME_VM,
                    value="2021-01-01",
                )

                xcom_push(
                    dag_id=VmCreateWorkflow.DAG_ID,
                    task_id=VmCreateWorkflow.run_terraform.__name__,
                    execution_date=execution_date,
                    key=TerraformRelease.XCOM_PREV_START_TIME_VM,
                    value="2021-01-01",
                )

                class MockDR:
                    def __init__(self):
                        self.start_date = datetime.datetime(2000, 1, 1)
                        self.execution_date = datetime.datetime(2020, 1, 1)
                        self.state = DagRunState.SUCCESS
                        self.dag_id = "dagid"

                class MockDag:
                    def __init__(self):
                        self.normalized_schedule_interval = "@weekly"
                        self.catchup = False

                    def previous_schedule(self, *args):
                        return datetime.datetime(2000, 1, 1)

                    def get_run_dates(self, *args):
                        return [datetime.datetime(2000, 1, 1)]

                with patch("observatory.platform.workflows.vm_workflow.DagRun.find") as m_drfind:
                    with patch("observatory.platform.workflows.vm_workflow.DagBag.get_dag") as m_getdag:
                        m_drfind.return_value = [MockDR()]
                        m_getdag.return_value = MockDag()

                        ti = env.run_task(workflow.check_dags_status.__name__)
                        self.assertEqual(ti.state, State.SUCCESS)

                # update terraform variable
                with patch(
                    "observatory.platform.workflows.vm_workflow.TerraformRelease.update_terraform_vm_create_variable"
                ) as m_update:
                    ti = env.run_task(workflow.update_terraform_variable.__name__)
                    self.assertEqual(ti.state, State.SUCCESS)
                    m_update.assert_called_once_with(False)

                # update terraform variable
                with patch(
                    "observatory.platform.workflows.vm_workflow.TerraformRelease.create_terraform_run"
                ) as m_runterraform:
                    m_runterraform.return_value = "run_id"
                    ti = env.run_task(workflow.run_terraform.__name__)
                    self.assertEqual(ti.state, State.SUCCESS)
                    m_runterraform.assert_called_once()

                # check run status
                with patch(
                    "observatory.platform.workflows.vm_workflow.TerraformRelease.check_terraform_run_status"
                ) as m_checkrun:
                    ti = env.run_task(workflow.check_run_status.__name__)
                    self.assertEqual(ti.state, State.SUCCESS)
                    m_checkrun.assert_called_once()

                # cleanup
                ti = env.run_task(workflow.cleanup.__name__)
                self.assertEqual(ti.state, State.SUCCESS)
                self.assertEqual(
                    xcom_count(
                        execution_date=execution_date,
                        dag_ids=workflow.dag_id,
                    ),
                    2,
                )

    @patch("observatory.platform.workflows.vm_workflow.TerraformApi.list_workspace_variables")
    @patch("observatory.platform.workflows.vm_workflow.TerraformApi.workspace_id")
    def test_vm_destroy_prev_execution_and_start_time_ge_destroy_time(self, m_tapi, m_list_workspace_vars):
        """Test the vm_destroy workflow"""

        m_tapi.return_value = "workspace"

        vm = VirtualMachine(machine_type="vm_type", disk_size=10, disk_type="ssd", create=True)
        m_list_workspace_vars.return_value = [
            TerraformVariable(
                key="airflow_worker_vm",
                value=vm.to_hcl(),
                hcl=True,
            )
        ]

        env = ObservatoryEnvironment()
        with env.create():
            workflow = VmDestroyWorkflow()
            dag = workflow.make_dag()
            execution_date = pendulum.datetime(2021, 1, 1)
            self.setup_env(env, '["vm_destroy"]')

            with env.create_dag_run(dag, execution_date):
                # check dependencies
                ti = env.run_task(workflow.check_dependencies.__name__)
                self.assertEqual(ti.state, State.SUCCESS)

                # check vm state
                ti = env.run_task(workflow.check_vm_state.__name__)
                self.assertEqual(ti.state, State.SUCCESS)

                # check dags status
                xcom_push(
                    dag_id=VmCreateWorkflow.DAG_ID,
                    task_id=VmCreateWorkflow.run_terraform.__name__,
                    execution_date=execution_date,
                    key=TerraformRelease.XCOM_START_TIME_VM,
                    value="2021-01-01",
                )

                xcom_push(
                    dag_id=VmCreateWorkflow.DAG_ID,
                    task_id=VmCreateWorkflow.run_terraform.__name__,
                    execution_date=execution_date,
                    key=TerraformRelease.XCOM_PREV_START_TIME_VM,
                    value="2021-01-01",
                )

                xcom_push(
                    dag_id=VmDestroyWorkflow.DAG_ID,
                    task_id=VmCreateWorkflow.run_terraform.__name__,
                    execution_date=execution_date,
                    key=VmDestroyWorkflow.XCOM_DESTROY_TIME_VM,
                    value="2021-01-01",
                )

                class MockDR:
                    def __init__(self):
                        self.start_date = datetime.datetime(2000, 1, 1)
                        self.execution_date = datetime.datetime(2020, 1, 1)
                        self.state = DagRunState.SUCCESS
                        self.dag_id = "dagid"

                class MockDag:
                    def __init__(self):
                        self.normalized_schedule_interval = "@weekly"
                        self.catchup = False

                    def previous_schedule(self, *args):
                        return datetime.datetime(2000, 1, 1)

                    def get_run_dates(self, *args):
                        return [datetime.datetime(2000, 1, 1)]

                with patch("observatory.platform.workflows.vm_workflow.DagRun.find") as m_drfind:
                    with patch("observatory.platform.workflows.vm_workflow.DagBag.get_dag") as m_getdag:
                        m_drfind.return_value = [MockDR()]
                        m_getdag.return_value = MockDag()

                        ti = env.run_task(workflow.check_dags_status.__name__)
                        self.assertEqual(ti.state, State.SUCCESS)

                # update terraform variable
                with patch(
                    "observatory.platform.workflows.vm_workflow.TerraformRelease.update_terraform_vm_create_variable"
                ) as m_update:
                    ti = env.run_task(workflow.update_terraform_variable.__name__)
                    self.assertEqual(ti.state, State.SUCCESS)
                    m_update.assert_called_once_with(False)

                # update terraform variable
                with patch(
                    "observatory.platform.workflows.vm_workflow.TerraformRelease.create_terraform_run"
                ) as m_runterraform:
                    m_runterraform.return_value = "run_id"
                    ti = env.run_task(workflow.run_terraform.__name__)
                    self.assertEqual(ti.state, State.SUCCESS)
                    m_runterraform.assert_called_once()

                # check run status
                with patch(
                    "observatory.platform.workflows.vm_workflow.TerraformRelease.check_terraform_run_status"
                ) as m_checkrun:
                    ti = env.run_task(workflow.check_run_status.__name__)
                    self.assertEqual(ti.state, State.SUCCESS)
                    m_checkrun.assert_called_once()

                # cleanup
                ti = env.run_task(workflow.cleanup.__name__)
                self.assertEqual(ti.state, State.SUCCESS)
                self.assertEqual(
                    xcom_count(
                        execution_date=execution_date,
                        dag_ids=workflow.dag_id,
                    ),
                    2,
                )

    @patch("observatory.platform.workflows.vm_workflow.TerraformApi.list_workspace_variables")
    @patch("observatory.platform.workflows.vm_workflow.TerraformApi.workspace_id")
    def test_vm_destroy_prev_execution_and_start_time_lt_destroy_time(self, m_tapi, m_list_workspace_vars):
        """Test the vm_destroy workflow"""

        m_tapi.return_value = "workspace"

        vm = VirtualMachine(machine_type="vm_type", disk_size=10, disk_type="ssd", create=True)
        m_list_workspace_vars.return_value = [
            TerraformVariable(
                key="airflow_worker_vm",
                value=vm.to_hcl(),
                hcl=True,
            )
        ]

        env = ObservatoryEnvironment()
        with env.create():
            workflow = VmDestroyWorkflow()
            dag = workflow.make_dag()
            execution_date = pendulum.datetime(2021, 1, 1)
            self.setup_env(env, '["vm_destroy"]')

            with env.create_dag_run(dag, execution_date):
                # check dependencies
                ti = env.run_task(workflow.check_dependencies.__name__)
                self.assertEqual(ti.state, State.SUCCESS)

                # check vm state
                ti = env.run_task(workflow.check_vm_state.__name__)
                self.assertEqual(ti.state, State.SUCCESS)

                # check dags status
                xcom_push(
                    dag_id=VmCreateWorkflow.DAG_ID,
                    task_id=VmCreateWorkflow.run_terraform.__name__,
                    execution_date=execution_date,
                    key=TerraformRelease.XCOM_START_TIME_VM,
                    value="2020-01-01",
                )

                xcom_push(
                    dag_id=VmCreateWorkflow.DAG_ID,
                    task_id=VmCreateWorkflow.run_terraform.__name__,
                    execution_date=execution_date,
                    key=TerraformRelease.XCOM_PREV_START_TIME_VM,
                    value="2021-01-01",
                )

                xcom_push(
                    dag_id=VmDestroyWorkflow.DAG_ID,
                    task_id=VmCreateWorkflow.run_terraform.__name__,
                    execution_date=execution_date,
                    key=VmDestroyWorkflow.XCOM_DESTROY_TIME_VM,
                    value="2021-01-01",
                )

                class MockDR:
                    def __init__(self):
                        self.start_date = datetime.datetime(2000, 1, 1)
                        self.execution_date = datetime.datetime(2020, 1, 1)
                        self.state = DagRunState.SUCCESS
                        self.dag_id = "dagid"

                class MockDag:
                    def __init__(self):
                        self.normalized_schedule_interval = "@weekly"
                        self.catchup = False

                    def previous_schedule(self, *args):
                        return datetime.datetime(2000, 1, 1)

                    def get_run_dates(self, *args):
                        return [datetime.datetime(2000, 1, 1)]

                with patch("observatory.platform.workflows.vm_workflow.DagRun.find") as m_drfind:
                    with patch("observatory.platform.workflows.vm_workflow.DagBag.get_dag") as m_getdag:
                        m_drfind.return_value = [MockDR()]
                        m_getdag.return_value = MockDag()

                        ti = env.run_task(workflow.check_dags_status.__name__)
                        self.assertEqual(ti.state, State.SUCCESS)

                # update terraform variable
                with patch(
                    "observatory.platform.workflows.vm_workflow.TerraformRelease.update_terraform_vm_create_variable"
                ) as m_update:
                    ti = env.run_task(workflow.update_terraform_variable.__name__)
                    self.assertEqual(ti.state, State.SUCCESS)
                    m_update.assert_called_once_with(False)

                # update terraform variable
                with patch(
                    "observatory.platform.workflows.vm_workflow.TerraformRelease.create_terraform_run"
                ) as m_runterraform:
                    m_runterraform.return_value = "run_id"
                    ti = env.run_task(workflow.run_terraform.__name__)
                    self.assertEqual(ti.state, State.SUCCESS)
                    m_runterraform.assert_called_once()

                # check run status
                with patch(
                    "observatory.platform.workflows.vm_workflow.TerraformRelease.check_terraform_run_status"
                ) as m_checkrun:
                    ti = env.run_task(workflow.check_run_status.__name__)
                    self.assertEqual(ti.state, State.SUCCESS)
                    m_checkrun.assert_called_once()

                # cleanup
                ti = env.run_task(workflow.cleanup.__name__)
                self.assertEqual(ti.state, State.SUCCESS)
                self.assertEqual(
                    xcom_count(
                        execution_date=execution_date,
                        dag_ids=workflow.dag_id,
                    ),
                    2,
                )

    @patch("observatory.platform.workflows.vm_workflow.TerraformApi.list_workspace_variables")
    @patch("observatory.platform.workflows.vm_workflow.TerraformApi.workspace_id")
    def test_vm_destroy_prev_execution_and_start_time_lt_destroy_time_catchup(self, m_tapi, m_list_workspace_vars):
        """Test the vm_destroy workflow"""

        m_tapi.return_value = "workspace"

        vm = VirtualMachine(machine_type="vm_type", disk_size=10, disk_type="ssd", create=True)
        m_list_workspace_vars.return_value = [
            TerraformVariable(
                key="airflow_worker_vm",
                value=vm.to_hcl(),
                hcl=True,
            )
        ]

        env = ObservatoryEnvironment()
        with env.create():
            workflow = VmDestroyWorkflow()
            dag = workflow.make_dag()
            execution_date = pendulum.datetime(2021, 1, 1)
            self.setup_env(env, '["vm_destroy"]')

            with env.create_dag_run(dag, execution_date):
                # check dependencies
                ti = env.run_task(workflow.check_dependencies.__name__)
                self.assertEqual(ti.state, State.SUCCESS)

                # check vm state
                ti = env.run_task(workflow.check_vm_state.__name__)
                self.assertEqual(ti.state, State.SUCCESS)

                # check dags status
                xcom_push(
                    dag_id=VmCreateWorkflow.DAG_ID,
                    task_id=VmCreateWorkflow.run_terraform.__name__,
                    execution_date=execution_date,
                    key=TerraformRelease.XCOM_START_TIME_VM,
                    value="2020-01-01",
                )

                xcom_push(
                    dag_id=VmCreateWorkflow.DAG_ID,
                    task_id=VmCreateWorkflow.run_terraform.__name__,
                    execution_date=execution_date,
                    key=TerraformRelease.XCOM_PREV_START_TIME_VM,
                    value="2021-01-01",
                )

                xcom_push(
                    dag_id=VmDestroyWorkflow.DAG_ID,
                    task_id=VmCreateWorkflow.run_terraform.__name__,
                    execution_date=execution_date,
                    key=VmDestroyWorkflow.XCOM_DESTROY_TIME_VM,
                    value="2021-01-01",
                )

                class MockDR:
                    def __init__(self):
                        self.start_date = datetime.datetime(2000, 1, 1)
                        self.execution_date = datetime.datetime(2020, 1, 1)
                        self.state = DagRunState.SUCCESS
                        self.dag_id = "dagid"

                class MockDag:
                    def __init__(self):
                        self.normalized_schedule_interval = "@weekly"
                        self.catchup = True

                    def previous_schedule(self, *args):
                        return datetime.datetime(2000, 1, 1)

                    def get_run_dates(self, *args):
                        return [datetime.datetime(2000, 1, 1)]

                with patch("observatory.platform.workflows.vm_workflow.DagRun.find") as m_drfind:
                    with patch("observatory.platform.workflows.vm_workflow.DagBag.get_dag") as m_getdag:
                        m_drfind.return_value = [MockDR()]
                        m_getdag.return_value = MockDag()

                        ti = env.run_task(workflow.check_dags_status.__name__)
                        self.assertEqual(ti.state, State.SUCCESS)

                # update terraform variable
                with patch(
                    "observatory.platform.workflows.vm_workflow.TerraformRelease.update_terraform_vm_create_variable"
                ) as m_update:
                    ti = env.run_task(workflow.update_terraform_variable.__name__)
                    self.assertEqual(ti.state, State.SUCCESS)
                    m_update.assert_called_once_with(False)

                # update terraform variable
                with patch(
                    "observatory.platform.workflows.vm_workflow.TerraformRelease.create_terraform_run"
                ) as m_runterraform:
                    m_runterraform.return_value = "run_id"
                    ti = env.run_task(workflow.run_terraform.__name__)
                    self.assertEqual(ti.state, State.SUCCESS)
                    m_runterraform.assert_called_once()

                # check run status
                with patch(
                    "observatory.platform.workflows.vm_workflow.TerraformRelease.check_terraform_run_status"
                ) as m_checkrun:
                    ti = env.run_task(workflow.check_run_status.__name__)
                    self.assertEqual(ti.state, State.SUCCESS)
                    m_checkrun.assert_called_once()

                # cleanup
                ti = env.run_task(workflow.cleanup.__name__)
                self.assertEqual(ti.state, State.SUCCESS)
                self.assertEqual(
                    xcom_count(
                        execution_date=execution_date,
                        dag_ids=workflow.dag_id,
                    ),
                    2,
                )

    @patch("observatory.platform.workflows.vm_workflow.TerraformApi.list_workspace_variables")
    @patch("observatory.platform.workflows.vm_workflow.TerraformApi.workspace_id")
    def test_vm_destroy_start_time_no_prev_execution(self, m_tapi, m_list_workspace_vars):
        """Test the vm_destroy workflow"""

        m_tapi.return_value = "workspace"

        vm = VirtualMachine(machine_type="vm_type", disk_size=10, disk_type="ssd", create=True)
        m_list_workspace_vars.return_value = [
            TerraformVariable(
                key="airflow_worker_vm",
                value=vm.to_hcl(),
                hcl=True,
            )
        ]

        env = ObservatoryEnvironment()
        with env.create():
            workflow = VmDestroyWorkflow()
            dag = workflow.make_dag()
            execution_date = pendulum.datetime(2021, 1, 1)
            self.setup_env(env, '["vm_destroy"]')

            with env.create_dag_run(dag, execution_date):
                # check dependencies
                ti = env.run_task(workflow.check_dependencies.__name__)
                self.assertEqual(ti.state, State.SUCCESS)

                # check vm state
                ti = env.run_task(workflow.check_vm_state.__name__)
                self.assertEqual(ti.state, State.SUCCESS)

                # check dags status
                xcom_push(
                    dag_id=VmCreateWorkflow.DAG_ID,
                    task_id=VmCreateWorkflow.run_terraform.__name__,
                    execution_date=execution_date,
                    key=TerraformRelease.XCOM_START_TIME_VM,
                    value="2021-01-01",
                )

                class MockDR:
                    def __init__(self):
                        self.start_date = datetime.datetime(2000, 1, 1)
                        self.execution_date = datetime.datetime(2020, 1, 1)
                        self.state = DagRunState.RUNNING
                        self.dag_id = "dagid"

                class MockDag:
                    def __init__(self):
                        self.normalized_schedule_interval = "@weekly"
                        self.catchup = False
                        self.default_args = {"start_date": datetime.datetime(2000, 1, 1)}

                    def previous_schedule(self, *args):
                        return datetime.datetime(2000, 1, 1)

                    def get_run_dates(self, *args):
                        return [datetime.datetime(2000, 1, 1)]

                with patch("observatory.platform.workflows.vm_workflow.DagRun.find") as m_drfind:
                    with patch("observatory.platform.workflows.vm_workflow.DagBag.get_dag") as m_getdag:
                        m_drfind.return_value = [MockDR()]
                        m_getdag.return_value = MockDag()

                        ti = env.run_task(workflow.check_dags_status.__name__)
                        self.assertEqual(ti.state, State.SUCCESS)

                # update terraform variable
                with patch(
                    "observatory.platform.workflows.vm_workflow.TerraformRelease.update_terraform_vm_create_variable"
                ) as m_update:
                    ti = env.run_task(workflow.update_terraform_variable.__name__)
                    self.assertEqual(ti.state, State.SKIPPED)

    @patch("observatory.platform.workflows.vm_workflow.TerraformApi.list_workspace_variables")
    @patch("observatory.platform.workflows.vm_workflow.TerraformApi.workspace_id")
    def test_vm_destroy_dont_destroy_worker_slack_warning(self, m_tapi, m_list_workspace_vars):
        """Test the vm_destroy workflow"""

        m_tapi.return_value = "workspace"

        vm = VirtualMachine(machine_type="vm_type", disk_size=10, disk_type="ssd", create=True)
        m_list_workspace_vars.return_value = [
            TerraformVariable(
                key="airflow_worker_vm",
                value=vm.to_hcl(),
                hcl=True,
            )
        ]

        env = ObservatoryEnvironment()
        with env.create():
            workflow = VmDestroyWorkflow()
            dag = workflow.make_dag()
            execution_date = pendulum.datetime(2021, 1, 1)
            self.setup_env(env, '["vm_destroy"]')

            with env.create_dag_run(dag, execution_date):
                # check dependencies
                ti = env.run_task(workflow.check_dependencies.__name__)
                self.assertEqual(ti.state, State.SUCCESS)

                # check vm state
                ti = env.run_task(workflow.check_vm_state.__name__)
                self.assertEqual(ti.state, State.SUCCESS)

                # check dags status
                xcom_push(
                    dag_id=VmCreateWorkflow.DAG_ID,
                    task_id=VmCreateWorkflow.run_terraform.__name__,
                    execution_date=execution_date,
                    key=TerraformRelease.XCOM_START_TIME_VM,
                    value="2020-12-31",
                )

                xcom_push(
                    dag_id=VmCreateWorkflow.DAG_ID,
                    task_id=VmCreateWorkflow.run_terraform.__name__,
                    execution_date=execution_date,
                    key=TerraformRelease.XCOM_PREV_START_TIME_VM,
                    value="2021-01-01",
                )

                xcom_push(
                    dag_id=VmDestroyWorkflow.DAG_ID,
                    task_id=VmDestroyWorkflow.check_runtime_vm.__name__,
                    execution_date=execution_date,
                    key=VmDestroyWorkflow.XCOM_WARNING_TIME,
                    value="2020-01-01",
                )

                class MockDR:
                    def __init__(self):
                        self.start_date = datetime.datetime(2000, 1, 1)
                        self.execution_date = datetime.datetime(2020, 1, 1)
                        self.state = DagRunState.SUCCESS
                        self.dag_id = "dagid"

                class MockDag:
                    def __init__(self):
                        self.normalized_schedule_interval = "@weekly"
                        self.catchup = False

                    def previous_schedule(self, *args):
                        return datetime.datetime(2000, 1, 1)

                    def get_run_dates(self, *args):
                        return [datetime.datetime(2000, 1, 1)]

                with patch("observatory.platform.workflows.vm_workflow.DagRun.find") as m_drfind:
                    with patch("observatory.platform.workflows.vm_workflow.DagBag.get_dag") as m_getdag:
                        with patch(
                            "observatory.platform.workflows.vm_workflow.VmDestroyWorkflow._check_success_runs"
                        ) as m_check_success_runs:
                            with patch("observatory.platform.workflows.vm_workflow.send_slack_msg") as m_slack:
                                m_drfind.return_value = [MockDR()]
                                m_getdag.return_value = MockDag()
                                m_check_success_runs.return_value = False

                                ti = env.run_task(workflow.check_dags_status.__name__)
                                self.assertEqual(ti.state, State.SUCCESS)

                                self.assertEqual(m_slack.call_count, 1)

    @patch("observatory.platform.workflows.vm_workflow.TerraformApi.list_workspace_variables")
    @patch("observatory.platform.workflows.vm_workflow.TerraformApi.workspace_id")
    def test_vm_destroy_dont_destroy_worker_no_slack_warning(self, m_tapi, m_list_workspace_vars):
        """Test the vm_destroy workflow"""

        m_tapi.return_value = "workspace"

        vm = VirtualMachine(machine_type="vm_type", disk_size=10, disk_type="ssd", create=True)
        m_list_workspace_vars.return_value = [
            TerraformVariable(
                key="airflow_worker_vm",
                value=vm.to_hcl(),
                hcl=True,
            )
        ]

        env = ObservatoryEnvironment()
        with env.create():
            workflow = VmDestroyWorkflow()
            dag = workflow.make_dag()
            execution_date = pendulum.datetime(2021, 1, 1)
            self.setup_env(env, '["vm_destroy"]')

            with env.create_dag_run(dag, execution_date):
                # check dependencies
                ti = env.run_task(workflow.check_dependencies.__name__)
                self.assertEqual(ti.state, State.SUCCESS)

                # check vm state
                ti = env.run_task(workflow.check_vm_state.__name__)
                self.assertEqual(ti.state, State.SUCCESS)

                # check dags status
                class MockDR:
                    def __init__(self):
                        self.start_date = datetime.datetime(2000, 1, 1)
                        self.execution_date = datetime.datetime(2020, 1, 1)
                        self.state = DagRunState.SUCCESS
                        self.dag_id = "dagid"

                class MockDag:
                    def __init__(self):
                        self.normalized_schedule_interval = "@weekly"
                        self.catchup = False

                    def previous_schedule(self, *args):
                        return datetime.datetime(2000, 1, 1)

                    def get_run_dates(self, *args):
                        return [datetime.datetime(2000, 1, 1)]

                with patch("observatory.platform.workflows.vm_workflow.DagRun.find") as m_drfind, patch(
                    "observatory.platform.workflows.vm_workflow.DagBag.get_dag"
                ) as m_getdag, patch(
                    "observatory.platform.workflows.vm_workflow.VmDestroyWorkflow._check_success_runs"
                ) as m_check_success_runs, patch(
                    "observatory.platform.workflows.vm_workflow.send_slack_msg"
                ) as m_slack, patch(
                    "observatory.platform.workflows.vm_workflow.TaskInstance.xcom_pull"
                ) as mock_xcom_pull:
                    mock_xcom_pull.reset_mock()
                    prev_start_time_vm = "2021-01-01"
                    start_time_vm = "2020-12-31"
                    warning_time = "2021-01-01"
                    # First 2 None xcom usages are from 'check_dependencies' task
                    mock_xcom_pull.side_effect = [None, None, prev_start_time_vm, start_time_vm, None, warning_time]

                    m_drfind.return_value = [MockDR()]
                    m_getdag.return_value = MockDag()
                    m_check_success_runs.return_value = False

                    ti = env.run_task(workflow.check_dags_status.__name__)
                    self.assertEqual(ti.state, State.SUCCESS)
                    self.assertEqual(m_slack.call_count, 0)
