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

# Author: Aniek Roelofs, Tuan Chien

import logging
from datetime import datetime
from typing import Optional, Tuple, Union, List

import pendulum
from airflow.models.dag import DAG
from airflow.models.dagbag import DagBag
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance
from airflow.utils.state import DagRunState
from croniter import croniter

from observatory.platform.airflow import delete_old_xcoms, get_airflow_connection_password, send_slack_msg
from observatory.platform.config import AirflowConns, Tag
from observatory.platform.observatory_config import (
    VirtualMachine,
)
from observatory.platform.terraform.terraform_api import TerraformApi, TerraformVariable
from observatory.platform.workflows.workflow import Workflow

TERRAFORM_CREATE_VM_KEY = "airflow_worker_vm"
TARGET_ADDRS = "module.airflow_worker_vm"  # Name of module in terraform configuration that will be targeted
XCOM_START_TIME_VM = "start_time_vm"
XCOM_PREV_START_TIME_VM = "prev_start_time_vm"
XCOM_TERRAFORM_RUN_ID = "terraform_run_id"
VM_RUNTIME_H_WARNING = 20  # Uptime threshold for VM before Slack notification
WARNING_FREQUENCY_H = 5  # Frequency of Slack notifications
XCOM_WARNING_TIME = "last_warning_time"
XCOM_DESTROY_TIME_VM = "destroy_time_vm"


class TerraformVirtualMachineAPI:
    def __init__(
        self,
        *,
        organisation: str,
        workspace: str,
        terraform_conn_id: str = AirflowConns.TERRAFORM,
        slack_conn_id: str = AirflowConns.SLACK,
    ):
        """The Terraform VM API.

        :param organisation: the Terraform organisation ID.
        :param workspace: the Terraform workspace name.
        :param terraform_conn_id: the Terraform Airflow Connection ID.
        :param slack_conn_id: the Slack Airflow Connection ID.
        """

        self.organisation = organisation
        self.workspace = workspace
        self.terraform_conn_id = terraform_conn_id
        self.slack_conn_id = slack_conn_id

    @property
    def terraform_api(self) -> TerraformApi:
        """Construct a TerraformApi object from the Airflow connection.

        :return: TerraformApi object.
        """

        token = get_airflow_connection_password(self.terraform_conn_id)
        return TerraformApi(token)

    @property
    def workspace_id(self) -> str:
        """Uses terraform API and workspace name to get the id of this workspace.

        :return: workspace id
        """

        return self.terraform_api.workspace_id(self.organisation, self.workspace)

    def get_vm_info(self) -> Tuple[Optional[VirtualMachine], Optional[TerraformVariable]]:
        """Get the VirtualMachine data object, and TerraformVariable object for airflow_worker_vm.

        :return VirtualMachine and TerraformVariable objects.
        """

        variables = self.terraform_api.list_workspace_variables(self.workspace_id)

        for var in variables:
            if var.key == TERRAFORM_CREATE_VM_KEY:
                return VirtualMachine.from_hcl(var.value), var

        return None, None

    def update_terraform_vm_create_variable(self, value: bool):
        """Update the Terraform VM create flag.

        :param value: New value to set.
        """

        vm, vm_var = self.get_vm_info()
        vm.create = value
        logging.info(f"vm.create: {vm.create}")
        vm_var.value = vm.to_hcl()

        self.terraform_api.update_workspace_variable(vm_var, self.workspace_id)

    def create_terraform_run(self, *, dag_id: str, start_date: pendulum.DateTime) -> str:
        """Create a Terraform run and return the run ID.

        :param dag_id: DAG ID.
        :param start_date: Task instance start date.
        :return Terraform run ID.
        """

        message = f'Triggered from airflow DAG "{dag_id}" at {start_date}'
        run_id = self.terraform_api.create_run(self.workspace_id, TARGET_ADDRS, message)
        logging.info(f"Terraform run_id: {run_id}")

        return run_id

    def check_terraform_run_status(self, *, ti: TaskInstance, execution_date: pendulum.DateTime, run_id: str):
        """Retrieve the terraform run status until it is in a finished state, either successful or errored. See
        https://www.terraform.io/docs/cloud/api/run.html for possible run_status values.
        If the run status is not successful and the environment isn't develop a warning message will be sent to a Slack
        channel.

        :param ti: Task instance.
        :param execution_date: DagRun execution date.
        :param run_id: The run id of the Terraform run
        :return: None
        """

        run_status = None
        while run_status not in [
            "planned_and_finished",
            "applied",
            "errored",
            "discarded",
            "canceled",
            "force_canceled",
        ]:
            run_details = self.terraform_api.get_run_details(run_id)
            run_status = run_details["data"]["attributes"]["status"]

        logging.info(f"Run status: {run_status}")
        comments = f"Terraform run status: {run_status}"
        logging.info(f'Sending slack notification: "{comments}"')
        send_slack_msg(ti=ti, execution_date=execution_date, comments=comments, slack_conn_id=self.slack_conn_id)


def parse_datetime(dt: str) -> Optional[pendulum.DateTime]:
    """Try to parse datetime using pendulum.parse. Do not try to parse None.

    :param dt: Datetime string.
    :return: Datetime object, or None if failed.
    """

    if dt is None:
        return None

    return pendulum.parse(dt)


class VmCreateWorkflow(Workflow):
    """Workflow to spin up an Airflow worker VM (with Terraform)."""

    def __init__(
        self,
        *,
        dag_id: str,
        terraform_organisation: str,
        terraform_workspace: str,
        terraform_conn_id=AirflowConns.TERRAFORM,
        slack_conn_id=AirflowConns.SLACK,
        start_date: pendulum.DateTime = pendulum.datetime(2020, 7, 1),
        schedule_interval: str = "@weekly",
        **kwargs,
    ):
        """Construct the workflow.

        :param dag_id: the DAG id.
        :param terraform_organisation: the Terraform Organisation ID.
        :param terraform_workspace: the full Terraform Workspace name.
        :param terraform_conn_id: the Airflow Connection ID for the Terraform credentials.
        :param slack_conn_id: the Airflow Connection ID for the Slack credentials.
        :param start_date: Start date for the DAG.
        :param schedule_interval: Schedule interval for the DAG.
        :param kwargs: to catch any extra kwargs passed during DAG creation.
        """

        super().__init__(
            dag_id=dag_id,
            start_date=start_date,
            schedule_interval=schedule_interval,
            catchup=False,
            max_active_runs=1,
            airflow_conns=[terraform_conn_id, slack_conn_id],
            tags=[Tag.observatory_platform],
        )

        self.terraform_organisation = terraform_organisation
        self.terraform_workspace = terraform_workspace
        self.vm_api = TerraformVirtualMachineAPI(
            organisation=self.terraform_organisation,
            workspace=self.terraform_workspace,
            terraform_conn_id=terraform_conn_id,
            slack_conn_id=slack_conn_id,
        )
        self.add_setup_task(self.check_dependencies)
        self.add_setup_task(self.check_vm_state)
        self.add_task(self.update_terraform_variable)
        self.add_task(self.run_terraform)
        self.add_task(self.check_run_status)
        self.add_task(self.cleanup, trigger_rule="none_failed")

    def make_release(self, **kwargs) -> None:
        """Required for Workflow class.

        :param kwargs: Unused.
        :return: None.
        """

        return None

    def check_vm_state(self, **kwargs) -> bool:
        """Checks if VM is running. Proceed only if VM is not already running.

        :param kwargs: Unused.
        :return: Whether to continue.
        """

        vm, _ = self.vm_api.get_vm_info()
        logging.info(f"VM is on: {vm.create}")
        return not vm.create

    def update_terraform_variable(self, _, **kwargs):
        """Update Terraform variable for VM to running state.

        :param kwargs: Unused.
        """

        self.vm_api.update_terraform_vm_create_variable(True)

    def run_terraform(self, _, **kwargs):
        """Runs terraform configuration. The current task start time, previous task start time, and Terraform run ID will be pushed to XComs.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are passed to
        this argument.
        """

        ti: TaskInstance = kwargs["ti"]

        prev_start_time_vm = ti.xcom_pull(key=XCOM_START_TIME_VM, include_prior_dates=True)
        ti.xcom_push(XCOM_PREV_START_TIME_VM, prev_start_time_vm)
        ti.xcom_push(XCOM_START_TIME_VM, ti.start_date.isoformat())

        run_id = self.vm_api.create_terraform_run(dag_id=self.dag_id, start_date=ti.start_date)
        ti.xcom_push(XCOM_TERRAFORM_RUN_ID, run_id)

    def check_run_status(self, _, **kwargs):
        """Retrieve the terraform run status until it is in a finished state, either successful or errored. See
        https://www.terraform.io/docs/cloud/api/run.html for possible run_status values.
        If the run status is not successful and the environment isn't develop a warning message will be sent to a slack
        channel.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are passed to
        this argument.
        """

        ti: TaskInstance = kwargs["ti"]
        execution_date = kwargs["execution_date"]

        run_id = ti.xcom_pull(key=XCOM_TERRAFORM_RUN_ID, task_ids=self.run_terraform.__name__)
        self.vm_api.check_terraform_run_status(ti=ti, execution_date=execution_date, run_id=run_id)

    def cleanup(self, _, **kwargs):
        """Delete stale XCom messages.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are passed to
        this argument.
        """

        execution_date = kwargs["execution_date"]
        delete_old_xcoms(dag_id=self.dag_id, execution_date=execution_date, retention_days=15)


class VmDestroyWorkflow(Workflow):
    """Workflow to teardown an Airflow worker VM (with Terraform)."""

    def __init__(
        self,
        *,
        dag_id: str,
        terraform_organisation: str,
        terraform_workspace: str,
        dags_watch_list: List[str],
        vm_create_dag_id: str = "vm_create",
        terraform_conn_id=AirflowConns.TERRAFORM,
        slack_conn_id=AirflowConns.SLACK,
        start_date: pendulum.DateTime = pendulum.datetime(2020, 1, 1),
        schedule_interval: str = "*/10 * * * *",
        **kwargs,
    ):
        """Construct the workflow.

        :param dag_id: the DAG id.
        :param terraform_organisation: the Terraform Organisation ID.
        :param terraform_workspace: the full Terraform Workspace name.
        :param dags_watch_list: the list of DAGs to watch for before destroying the VM.
        :param terraform_conn_id: the Airflow Connection ID for the Terraform credentials.
        :param slack_conn_id: the Airflow Connection ID for the Slack credentials.
        :param start_date: Start date for the DAG.
        :param schedule_interval: Schedule interval for the DAG.
        :param kwargs: to catch any extra kwargs passed during DAG creation.
        """

        super().__init__(
            dag_id=dag_id,
            start_date=start_date,
            schedule_interval=schedule_interval,
            catchup=False,
            max_active_runs=1,
            airflow_conns=[terraform_conn_id, slack_conn_id],
            tags=[Tag.observatory_platform],
        )

        self.terraform_organisation = terraform_organisation
        self.terraform_workspace = terraform_workspace
        self.dags_watch_list = dags_watch_list
        self.vm_create_dag_id = vm_create_dag_id
        self.slack_conn_id = slack_conn_id
        self.vm_api = TerraformVirtualMachineAPI(
            organisation=self.terraform_organisation,
            workspace=self.terraform_workspace,
            terraform_conn_id=terraform_conn_id,
            slack_conn_id=slack_conn_id,
        )

        self.add_setup_task(self.check_dependencies)
        self.add_setup_task(self.check_vm_state)
        self.add_setup_task(self.check_dags_status)
        self.add_task(self.update_terraform_variable)
        self.add_task(self.run_terraform)
        self.add_task(self.check_run_status)
        self.add_task(self.cleanup, trigger_rule="none_failed")

    def make_release(self, **kwargs) -> None:
        """Required for Workflow class.

        :param kwargs: Unused.
        :return: None.
        """

        return None

    def check_vm_state(self, **kwargs) -> bool:
        """Checks if VM is running. Proceed only if VM is running.

        :param kwargs: Unused.
        :return: Whether to continue.
        """

        vm, _ = self.vm_api.get_vm_info()
        logging.info(f"VM is on: {vm.create}")
        return vm.create

    def check_dags_status(self, **kwargs):
        """Check if all expected runs for the DAGs in the watchlist are successful. If they are the task, then proceed, otherwise check how long the VM has run for, and skip the rest of the workflow.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are passed to
        this argument.
        :return: id of task which should be executed next
        """

        ti: TaskInstance = kwargs["ti"]
        destroy_worker_vm = True

        prev_start_time_vm = ti.xcom_pull(
            key=XCOM_PREV_START_TIME_VM,
            task_ids=VmCreateWorkflow.run_terraform.__name__,
            dag_id=self.vm_create_dag_id,
            include_prior_dates=True,
        )
        prev_start_time_vm = parse_datetime(prev_start_time_vm)

        start_time_vm = ti.xcom_pull(
            key=XCOM_START_TIME_VM,
            task_ids=VmCreateWorkflow.run_terraform.__name__,
            dag_id=self.vm_create_dag_id,
            include_prior_dates=True,
        )
        start_time_vm = parse_datetime(start_time_vm)

        destroy_time_vm = ti.xcom_pull(
            key=XCOM_DESTROY_TIME_VM,
            task_ids=self.run_terraform.__name__,
            dag_id=self.dag_id,
            include_prior_dates=True,
        )
        destroy_time_vm = parse_datetime(destroy_time_vm)

        logging.info(
            f"prev_start_time_vm: {prev_start_time_vm}, start_time_vm: {start_time_vm}, "
            f"destroy_time_vm: {destroy_time_vm}\n"
        )

        # Load VM DAGs watch list
        for dag_id in self.dags_watch_list:
            dagbag = DagBag()
            dag = dagbag.get_dag(dag_id)
            logging.info(f"Dag id: {dag_id}")

            # vm turned on manually and never turned on before
            if not start_time_vm and not prev_start_time_vm:
                logging.warning("Both start_time_vm and prev_start_time_vm are None. Unsure whether to turn off DAG.")
                destroy_worker_vm = False
                break

            # returns last execution date of previous vm cycle or None if a DAG is running
            last_execution_prev = self._get_last_execution_prev(dag, dag_id, prev_start_time_vm)
            if not last_execution_prev:
                destroy_worker_vm = False
                break

            logging.info(f"Execution date of last DAG before prev_start_time_vm: {last_execution_prev}\n")

            if destroy_time_vm:
                if start_time_vm < destroy_time_vm:
                    # If the vm is on, but there's no start_time_vm it must have been turned on manually.
                    logging.warning(
                        "start_time_vm is before destroy_time_vm. Perhaps the vm was turned on "
                        "manually in between. This task will continue with the given start_time_vm."
                    )

            # get a backfill of all expected runs between last execution date of prev cycle and the time the vm was
            # started. create iterator starting at the latest planned schedule before start_time_vm
            cron_iter = croniter(dag.normalized_schedule_interval, dag.previous_schedule(start_time_vm))

            # the last_execution_current is expected 1 schedule interval before the DAGs 'previous_schedule',
            # because airflow won't trigger a DAG until 1 schedule interval after the 'execution_date'.
            last_execution_current = cron_iter.get_prev(datetime)

            # if DAG is not set to catchup any backfill, the only run date is the last one.
            if dag.catchup:
                execution_dates = dag.get_run_dates(last_execution_prev, last_execution_current)
            else:
                execution_dates = [last_execution_current]

            # for each execution date check if state is success. This can't be done in all_dag_runs above, because the
            # dag_run might not be in all_dag_runs yet, because it is not scheduled yet.
            destroy_worker_vm = self._check_success_runs(dag_id, execution_dates)
            if destroy_worker_vm is False:
                break

        logging.info(f"Destroying worker VM: {destroy_worker_vm}")

        # If not destroying vm, check VM runtime.
        if not destroy_worker_vm:
            self.check_runtime_vm(start_time_vm, **kwargs)

        return destroy_worker_vm

    def check_runtime_vm(self, start_time_vm: Optional[datetime], **kwargs):
        """Checks how long the VM has been turned on based on the xcom value from the terraform run task.
        A warning message will be sent in a slack channel if it has been on longer than the warning limit,
        the environment isn't develop and a message hasn't been sent already in the last x hours.

        :param start_time_vm: Start time of the vm
        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are passed to
        this argument.
        :return: None
        """

        ti: TaskInstance = kwargs["ti"]
        last_warning_time = ti.xcom_pull(
            key=XCOM_WARNING_TIME,
            task_ids=ti.task_id,
            dag_id=self.dag_id,
            include_prior_dates=True,
        )
        last_warning_time = parse_datetime(last_warning_time)

        if start_time_vm:
            # calculate number of hours passed since start time vm and now
            hours_on = (ti.start_date - start_time_vm).total_seconds() / 3600
            logging.info(
                f"Start time VM: {start_time_vm}, hours passed since start time: {hours_on}, warning limit: "
                f"{VM_RUNTIME_H_WARNING}"
            )

            # check if a warning has been sent previously and if so, how many hours ago
            if last_warning_time:
                hours_since_warning = (ti.start_date - last_warning_time).total_seconds() / 3600
            else:
                hours_since_warning = None

            # check if the VM has been on longer than the limit
            if hours_on > VM_RUNTIME_H_WARNING:
                #  check if no warning was sent before or last time was longer ago than warning frequency
                if not hours_since_warning or hours_since_warning > WARNING_FREQUENCY_H:
                    comments = (
                        f"Worker VM has been on since {start_time_vm}. No. hours passed since then: "
                        f"{hours_on}."
                        f" Warning limit: {VM_RUNTIME_H_WARNING}H"
                    )
                    execution_date = kwargs["execution_date"]
                    send_slack_msg(
                        ti=ti, execution_date=execution_date, comments=comments, slack_conn_id=self.slack_conn_id
                    )

                    ti.xcom_push(XCOM_WARNING_TIME, ti.start_date.isoformat())
        else:
            logging.info(f"Start time VM unknown.")

    def update_terraform_variable(self, _, **kwargs):
        """Update Terraform variable for VM to running state.

        :param kwargs: Unused.
        """

        self.vm_api.update_terraform_vm_create_variable(False)

    def run_terraform(self, _, **kwargs):
        """Runs terraform configuration. The current task start time, previous task start time, and Terraform run ID will be pushed to XComs.

        :param _: None.
        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are passed to
        this argument.
        """

        ti: TaskInstance = kwargs["ti"]
        ti.xcom_push(XCOM_DESTROY_TIME_VM, ti.start_date.isoformat())
        run_id = self.vm_api.create_terraform_run(dag_id=self.dag_id, start_date=ti.start_date)
        ti.xcom_push(XCOM_TERRAFORM_RUN_ID, run_id)

    def check_run_status(self, _, **kwargs):
        """Retrieve the terraform run status until it is in a finished state, either successful or errored. See
        https://www.terraform.io/docs/cloud/api/run.html for possible run_status values.
        If the run status is not successful and the environment isn't develop a warning message will be sent to a slack
        channel.

        :param _: None.
        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are passed to
        this argument.
        """

        ti: TaskInstance = kwargs["ti"]
        execution_date = kwargs["execution_date"]

        run_id = ti.xcom_pull(key=XCOM_TERRAFORM_RUN_ID, task_ids=self.run_terraform.__name__)
        self.vm_api.check_terraform_run_status(ti=ti, execution_date=execution_date, run_id=run_id)

    def cleanup(self, _, **kwargs):
        """Delete stale XCom messages.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are passed to
        this argument.
        """

        execution_date = kwargs["execution_date"]
        delete_old_xcoms(dag_id=self.dag_id, execution_date=execution_date, retention_days=15)

    def _get_last_execution_prev(
        self, dag: DAG, dag_id: str, prev_start_time_vm: Union[datetime, None]
    ) -> Union[datetime, None]:
        """Find the execution date of the last DAG run before the previous time the VM was turned on.
        If there aren't any DAG runs before this time or the time is None (first/second time turning off VM) the
        execution date is set to the start_date of the DAG instead.

        If a DAG is currently running it will return None and the remaining tasks are skipped.

        :param dag: DAG object
        :param dag_id: the dag id
        :param prev_start_time_vm: previous time the VM was turned on
        :return: execution date or None
        """

        # Get execution date of the last run before previous start date
        all_dag_runs = DagRun.find(dag_id=dag_id)
        # sort dag runs by start datetime, newest first
        for dag_run in sorted(all_dag_runs, key=lambda x: x.start_date, reverse=True):
            if dag_run.state == "running":
                logging.info("DAG is currently running.")
                return None
            # None if first time running destroy
            if prev_start_time_vm:
                if pendulum.instance(dag_run.start_date) < prev_start_time_vm:
                    # get execution date of last run from when the VM was previously on
                    return dag_run.execution_date

        # No runs executed previously
        if prev_start_time_vm:
            logging.info("No DAG runs that started before prev_start_time_vm.")
        else:
            # First time running destroy_vm, no previous start date available
            logging.info("No prev_start_time_vm.")
        logging.info("Setting last execution date to start_date of DAG.")
        last_execution_prev = dag.default_args["start_date"]

        return last_execution_prev

    def _check_success_runs(self, dag_id: str, execution_dates: list) -> bool:
        """For each date in the execution dates it checks if a DAG run exists and if so if the state is set to success.

        Only if both of these are true for all dates it will return True.

        :param dag_id: the dag id
        :param execution_dates: list of execution dates
        :return: True or False
        """
        for date in execution_dates:
            dag_runs = DagRun.find(dag_id=dag_id, execution_date=date)
            if not dag_runs:
                logging.info(f"Expected dag run on {date} has not been scheduled yet")
                return False

            for dag_run in dag_runs:
                logging.info(
                    f"id: {dag_run.dag_id}, start date: {dag_run.start_date}, execution date: "
                    f"{dag_run.execution_date}, state: {dag_run.state}"
                )
                if dag_run.state != DagRunState.SUCCESS:
                    return False
        return True
