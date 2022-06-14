import os
from unittest.mock import PropertyMock, patch

import pendulum
from airflow.models import XCom
from airflow.models.connection import Connection
from airflow.models.variable import Variable
from airflow.utils.session import provide_session
from airflow.utils.state import State

from observatory.platform.observatory_config import VirtualMachine
from observatory.platform.terraform.terraform_api import TerraformVariable
from observatory.platform.utils.airflow_utils import AirflowConns, AirflowVars
from observatory.platform.utils.test_utils import (
    ObservatoryEnvironment,
    ObservatoryTestCase,
    module_file_path,
)
from observatory.platform.workflows.vm_workflow import (
    TerraformRelease,
    VmCreateWorkflow,
)


@provide_session
def xcom_count(*, execution_date, dag_ids, session=None):
    return XCom.get_many(
        execution_date=execution_date,
        dag_ids=dag_ids,
        include_prior_dates=False,
        session=session,
    ).count()


class TestVmCreateWorkflow(ObservatoryTestCase):
    """Test the vm_create dag."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @patch("observatory.platform.workflows.vm_workflow.TerraformApi.list_workspace_variables")
    @patch("observatory.platform.workflows.vm_workflow.TerraformRelease.workspace_id", new_callable=PropertyMock)
    @patch("observatory.platform.workflows.vm_workflow.TerraformRelease.terraform_api", new_callable=PropertyMock)
    def test_get_vm_info_no_vars(self, m_tapi, m_wid, m_list_vars):
        """Test get_vm_info"""

        m_list_vars.return_value = []
        m_wid.return_value = "wid"
        release = TerraformRelease()
        vm, vm_var = release.get_vm_info()
        self.assertIsNone(vm)
        self.assertIsNone(vm_var)

    @patch("observatory.platform.workflows.vm_workflow.TerraformRelease.workspace_id", new_callable=PropertyMock)
    @patch("observatory.platform.workflows.vm_workflow.TerraformRelease.terraform_api", new_callable=PropertyMock)
    def test_get_vm_info_no_target_vars(self, m_tapi, m_wid):
        """Test get_vm_info"""

        vm = VirtualMachine(machine_type="vm_type", disk_size=10, disk_type="ssd", create=False)
        vm_tf = TerraformVariable(
            key="not_target",
            value=vm.to_hcl(),
            hcl=True,
        )

        class MockApi:
            def list_workspace_variables(self, *args):
                return [vm_tf]

        m_tapi.return_value = MockApi()
        release = TerraformRelease()

        vm, vm_var = release.get_vm_info()
        self.assertIsNone(vm)
        self.assertIsNone(vm_var)

    def test_dag_structure(self):
        """Test that vm_create has the correct structure.
        :return: None
        """

        dag = VmCreateWorkflow().make_dag()
        self.assert_dag_structure(
            {
                "check_dependencies": ["check_vm_state"],
                "check_vm_state": ["update_terraform_variable"],
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
            dag_file = os.path.join(module_file_path("observatory.platform.dags"), "vm_create.py")
            self.assert_dag_load("vm_create", dag_file)

    def setup_env(self, env):
        var = Variable(key=AirflowVars.PROJECT_ID, val="project")
        env.add_variable(var)

        var = Variable(key=AirflowVars.TERRAFORM_ORGANIZATION, val="terraform_org")
        env.add_variable(var)

        var = Variable(key=AirflowVars.ENVIRONMENT, val="environment")
        env.add_variable(var)

        conn = Connection(conn_id=AirflowConns.TERRAFORM, uri="http://localhost")
        env.add_connection(conn)

    @patch("observatory.platform.workflows.vm_workflow.TerraformApi.list_workspace_variables")
    @patch("observatory.platform.workflows.vm_workflow.TerraformApi.workspace_id")
    def test_workflow_vm_already_on(self, m_tapi, m_list_workspace_vars):
        "Test the vm_create workflow"

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
            workflow = VmCreateWorkflow()
            dag = workflow.make_dag()
            execution_date = pendulum.datetime(2021, 1, 1)
            self.setup_env(env)

            with env.create_dag_run(dag, execution_date):
                # check dependencies
                ti = env.run_task(workflow.check_dependencies.__name__)
                self.assertEqual(ti.state, State.SUCCESS)

                # check vm state
                ti = env.run_task(workflow.check_vm_state.__name__)
                self.assertEqual(ti.state, State.SUCCESS)

                # update terraform variable
                ti = env.run_task(workflow.update_terraform_variable.__name__)
                self.assertEqual(ti.state, State.SKIPPED)

                # run terraform
                ti = env.run_task(workflow.run_terraform.__name__)
                self.assertEqual(ti.state, State.SKIPPED)

                # check run status
                ti = env.run_task(workflow.check_run_status.__name__)
                self.assertEqual(ti.state, State.SKIPPED)

                # cleanup
                ti = env.run_task(workflow.cleanup.__name__)
                self.assertEqual(ti.state, State.SUCCESS)

    @patch("observatory.platform.workflows.vm_workflow.send_slack_msg")
    @patch("observatory.platform.workflows.vm_workflow.TerraformApi.get_run_details")
    @patch("observatory.platform.workflows.vm_workflow.TerraformApi.create_run")
    @patch("observatory.platform.workflows.vm_workflow.TerraformApi.update_workspace_variable")
    @patch("observatory.platform.workflows.vm_workflow.TerraformApi.list_workspace_variables")
    @patch("observatory.platform.workflows.vm_workflow.TerraformApi.workspace_id")
    def test_workflow_vm_create(
        self, m_tapi, m_list_workspace_vars, m_update, m_create_run, m_run_details, m_send_slack_msg
    ):
        "Test the vm_create workflow"

        m_tapi.return_value = "workspace"

        vm = VirtualMachine(machine_type="vm_type", disk_size=10, disk_type="ssd", create=False)
        vm_tf = TerraformVariable(
            key="airflow_worker_vm",
            value=vm.to_hcl(),
            hcl=True,
        )
        m_list_workspace_vars.return_value = [vm_tf]
        m_create_run.return_value = 1
        m_run_details.return_value = {"data": {"attributes": {"status": "planned_and_finished"}}}

        env = ObservatoryEnvironment()
        with env.create():
            workflow = VmCreateWorkflow()
            dag = workflow.make_dag()
            execution_date = pendulum.datetime(2021, 1, 1)
            self.setup_env(env)

            with env.create_dag_run(dag, execution_date):
                # check dependencies
                ti = env.run_task(workflow.check_dependencies.__name__)
                self.assertEqual(ti.state, State.SUCCESS)

                # check vm state
                ti = env.run_task(workflow.check_vm_state.__name__)
                self.assertEqual(ti.state, State.SUCCESS)

                # update terraform variable
                ti = env.run_task(workflow.update_terraform_variable.__name__)
                self.assertEqual(m_update.call_count, 1)
                call_args, _ = m_update.call_args
                self.assertEqual(call_args[0], vm_tf)
                self.assertEqual(call_args[1], "workspace")
                self.assertEqual(ti.state, State.SUCCESS)

                # run terraform
                ti = env.run_task(workflow.run_terraform.__name__)
                self.assertEqual(ti.state, State.SUCCESS)
                self.assertEqual(m_create_run.call_count, 1)

                # check run status
                ti = env.run_task(workflow.check_run_status.__name__)
                self.assertEqual(ti.state, State.SUCCESS)
                self.assertEqual(m_send_slack_msg.call_count, 1)

                # cleanup
                ti = env.run_task(workflow.cleanup.__name__)
                self.assertEqual(ti.state, State.SUCCESS)
                self.assertEqual(
                    xcom_count(
                        execution_date=execution_date,
                        dag_ids=workflow.dag_id,
                    ),
                    3,
                )
