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

# Author: Aniek Roelofs

import json
import logging
from datetime import datetime
from typing import Union

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.models.dag import DAG
from airflow.models.dagbag import DagBag
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance
from airflow.models.variable import Variable
from croniter import croniter

from observatory.platform.observatory_config import TerraformConfig, VirtualMachine
from observatory.platform.terraform_api import TerraformApi
from observatory.platform.utils.airflow_utils import (
    AirflowConns,
    AirflowVars,
    change_task_log_level,
    check_connections,
    check_variables,
    create_slack_webhook,
)


def get_workspace_id() -> str:
    """Uses terraform API and workspace name to get the id of this workspace.

    :return: workspace id
    """
    token = BaseHook.get_connection(AirflowConns.TERRAFORM).password
    terraform_api = TerraformApi(token)

    # Get organization
    organization = Variable.get(AirflowVars.TERRAFORM_ORGANIZATION)

    # Get workspace
    environment = Variable.get(AirflowVars.ENVIRONMENT)
    workspace = TerraformConfig.WORKSPACE_PREFIX + environment

    # Get workspace ID
    workspace_id = terraform_api.workspace_id(organization, workspace)

    return workspace_id


def get_last_execution_prev(dag: DAG, dag_id: str, prev_start_time_vm: Union[datetime, None]) -> Union[datetime, None]:
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
            if dag_run.start_date < prev_start_time_vm:
                # get execution date of last run from when the VM was previously on
                return dag_run.execution_date
    else:
        # No runs executed previously
        if prev_start_time_vm:
            logging.info("No DAG runs that started before prev_start_time_vm.")
        else:
            # First time running destroy_vm, no previous start date available
            logging.info("No prev_start_time_vm.")
        logging.info("Setting last execution date to start_date of DAG.")
        last_execution_prev = dag.default_args["start_date"]
    return last_execution_prev


def check_success_runs(dag_id: str, execution_dates: list) -> bool:
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
            if dag_run.state != "success":
                return False
    return True


class TerraformTasks:
    """A container for holding the functions for the Terraform telescope. Both DAGs vm_create and vm_destroy use
    functions from this telescope."""

    # DAG variables
    DAG_ID_CREATE_VM = "vm_create"
    DAG_ID_DESTROY_VM = "vm_destroy"

    XCOM_START_TIME_VM = "start_time_vm"
    XCOM_PREV_START_TIME_VM = "prev_start_time_vm"
    XCOM_DESTROY_TIME_VM = "destroy_time_vm"
    XCOM_TERRAFORM_RUN_ID = "terraform_run_id"
    XCOM_WARNING_TIME = "last_warning_time"

    TASK_ID_CHECK_DEPENDENCIES = "check_dependencies"
    TASK_ID_VM_STATUS = "check_vm_status"
    TASK_ID_VAR_UPDATE = "update_terraform_var"
    TASK_ID_RUN = "terraform_run_configuration"
    TASK_ID_DAG_SUCCESS = "check_all_dags_success"
    TASK_ID_RUN_STATUS = "terraform_check_run_status"
    TASK_ID_VM_RUNTIME = "check_runtime_vm"

    # Name of variable in terraform configuration that will be updated
    TERRAFORM_CREATE_VM_KEY = "airflow_worker_vm"
    TERRAFORM_CREATE_KEY = "create"

    # Name of module in terraform configuration that will be targeted
    TERRAFORM_MODULE_WORKER_VM = "module.airflow_worker_vm"

    # No. hours for VM to be turned on until a slack notification is send
    VM_RUNTIME_H_WARNING = 20

    # Frequency in hours of how often a warning message is sent on slack
    WARNING_FREQUENCY_H = 5

    @staticmethod
    def check_dependencies():
        """Check that all variables exist that are required to run the DAG.
        :return: None.
        """

        vars_valid = check_variables(
            AirflowVars.PROJECT_ID, AirflowVars.TERRAFORM_ORGANIZATION, AirflowVars.VM_DAGS_WATCH_LIST
        )
        conns_valid = check_connections(AirflowConns.TERRAFORM)

        if not vars_valid or not conns_valid:
            raise AirflowException("Required variables or connections are missing")

    @staticmethod
    def get_variable_create(**kwargs) -> bool:
        """Retrieves the current value of the terraform variable from the cloud workspace, if this already has the
        value
        that the DAG is planning to set it to, it will return False and remaining tasks are skipped.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are passed to
        this argument.
        :return: True or False
        """

        token = BaseHook.get_connection(AirflowConns.TERRAFORM).password
        terraform_api = TerraformApi(token)

        # Add variable to workspace
        workspace_id = get_workspace_id()
        variables = terraform_api.list_workspace_variables(workspace_id)
        vm = None
        for var in variables:
            if var.key == TerraformTasks.TERRAFORM_CREATE_VM_KEY:
                vm = VirtualMachine.from_hcl(var.value)
                break

        # create_vm dag run and terraform create variable is already true (meaning VM is already on) or
        # destroy_vm dag run and terraform destroy variable is already false (meaning VM is already off)
        vm_is_on = vm.create
        print(f"DAG RUN: {kwargs['dag_run'].dag_id}")
        print(f"VM state: {vm_is_on}")

        if (kwargs["dag_run"].dag_id == TerraformTasks.DAG_ID_CREATE_VM and vm_is_on) or (
            kwargs["dag_run"].dag_id == TerraformTasks.DAG_ID_DESTROY_VM and not vm_is_on
        ):
            logging.info(f'VM is already in this state: {"on" if vm_is_on else "off"}')
            return False

        logging.info(f'Turning vm {"off" if vm.create else "on"}')
        return True

    @staticmethod
    def update_terraform_variable(**kwargs):
        """Updates the value of the terraform variable from the cloud workspace. The value is dependent on which DAG
        this function is executed from.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are passed to
        this argument.
        :return: None
        """

        create_value = kwargs["dag_run"].dag_id == TerraformTasks.DAG_ID_CREATE_VM
        token = BaseHook.get_connection(AirflowConns.TERRAFORM).password
        terraform_api = TerraformApi(token)

        # Get variable
        workspace_id = get_workspace_id()
        variables = terraform_api.list_workspace_variables(workspace_id)
        vm = None
        vm_var = None
        for var in variables:
            if var.key == TerraformTasks.TERRAFORM_CREATE_VM_KEY:
                vm_var = var
                vm = VirtualMachine.from_hcl(var.value)
                break

        # Update vm create value and convert to HCL
        vm.create = create_value
        print(f"CREATE VALUE!: {vm.create}")
        vm_var.value = vm.to_hcl()

        # Update value
        terraform_api.update_workspace_variable(vm_var, workspace_id)

    @staticmethod
    def terraform_run(**kwargs):
        """Runs terraform configuration on specific target.
        xcom values are pushed for the start and destroy time of the vm with the start time of this task. If this
        function is executed from the 'create' DAG, the time from when the previous vm was started will be set to the
        xcom value of this task from the previous run.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are passed to
        this argument.
        :return: None
        """

        # Push xcom with start date of this DAG run both for start and destroy
        ti: TaskInstance = kwargs["ti"]
        if kwargs["dag_run"].dag_id == TerraformTasks.DAG_ID_CREATE_VM:
            prev_start_time_vm = ti.xcom_pull(
                key=TerraformTasks.XCOM_START_TIME_VM,
                task_ids=TerraformTasks.TASK_ID_RUN,
                dag_id=TerraformTasks.DAG_ID_CREATE_VM,
                include_prior_dates=True,
            )
            ti.xcom_push(TerraformTasks.XCOM_PREV_START_TIME_VM, prev_start_time_vm)
            ti.xcom_push(TerraformTasks.XCOM_START_TIME_VM, ti.start_date)
        if kwargs["dag_run"].dag_id == TerraformTasks.DAG_ID_DESTROY_VM:
            ti.xcom_push(TerraformTasks.XCOM_DESTROY_TIME_VM, ti.start_date)

        token = BaseHook.get_connection(AirflowConns.TERRAFORM).password
        terraform_api = TerraformApi(token)

        target_addrs = TerraformTasks.TERRAFORM_MODULE_WORKER_VM
        workspace_id = get_workspace_id()
        message = f'Triggered from airflow DAG "{kwargs["dag_run"].dag_id}" at {ti.start_date}'

        run_id = terraform_api.create_run(workspace_id, target_addrs, message)
        logging.info(run_id)

        # Push run id
        ti.xcom_push(TerraformTasks.XCOM_TERRAFORM_RUN_ID, run_id)

    @staticmethod
    def check_terraform_run_status(**kwargs):
        """Retrieve the terraform run status until it is in a finished state, either successful or errored. See
        https://www.terraform.io/docs/cloud/api/run.html for possible run_status values.
        If the run status is not successful and the environment isn't develop a warning message will be sent to a slack
        channel.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are passed to
        this argument.
        :return: None
        """

        ti: TaskInstance = kwargs["ti"]
        run_id = ti.xcom_pull(key=TerraformTasks.XCOM_TERRAFORM_RUN_ID, task_ids=TerraformTasks.TASK_ID_RUN)
        project_id = Variable.get(AirflowVars.PROJECT_ID)

        token = BaseHook.get_connection(AirflowConns.TERRAFORM).password
        terraform_api = TerraformApi(token)

        run_status = None
        while run_status not in [
            "planned_and_finished",
            "applied",
            "errored",
            "discarded",
            "canceled",
            "force_canceled",
        ]:
            run_details = terraform_api.get_run_details(run_id)
            run_status = run_details["data"]["attributes"]["status"]

        logging.info(f"Run status: {run_status}")
        comments = f"Terraform run status: {run_status}"
        logging.info(f'Sending slack notification: "{comments}"')
        slack_hook = create_slack_webhook(comments, project_id, **kwargs)
        slack_hook.execute()

    @staticmethod
    def check_success_dags(**kwargs) -> str:
        """Check if all expected runs for the DAGs in the watchlist are successful. If they are the task to update the
        terraform variable will be executed, otherwise the task to check how long the VM has been on will be executed.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are passed to
        this argument.
        :return: id of task which should be executed next
        """

        destroy_worker_vm = True

        ti: TaskInstance = kwargs["ti"]
        prev_start_time_vm = ti.xcom_pull(
            key=TerraformTasks.XCOM_PREV_START_TIME_VM,
            task_ids=TerraformTasks.TASK_ID_RUN,
            dag_id=TerraformTasks.DAG_ID_CREATE_VM,
            include_prior_dates=True,
        )
        start_time_vm = ti.xcom_pull(
            key=TerraformTasks.XCOM_START_TIME_VM,
            task_ids=TerraformTasks.TASK_ID_RUN,
            dag_id=TerraformTasks.DAG_ID_CREATE_VM,
            include_prior_dates=True,
        )
        destroy_time_vm = ti.xcom_pull(
            key=TerraformTasks.XCOM_DESTROY_TIME_VM,
            task_ids=TerraformTasks.TASK_ID_RUN,
            dag_id=TerraformTasks.DAG_ID_DESTROY_VM,
            include_prior_dates=True,
        )
        logging.info(
            f"prev_start_time_vm: {prev_start_time_vm}, start_time_vm: {start_time_vm}, "
            f"destroy_time_vm: {destroy_time_vm}\n"
        )

        # Load VM DAGs watch list
        vm_dags_watch_list_str = Variable.get(AirflowVars.VM_DAGS_WATCH_LIST)
        logging.info(f"vm_dags_watch_list_str str: {vm_dags_watch_list_str}")
        vm_dags_watch_list = json.loads(vm_dags_watch_list_str)
        logging.info(f"vm_dags_watch_list: {vm_dags_watch_list}")

        for dag_id in vm_dags_watch_list:
            dagbag = DagBag()
            dag = dagbag.get_dag(dag_id)
            logging.info(f"Dag id: {dag_id}")

            # vm turned on manually and never turned on before
            if not start_time_vm and not prev_start_time_vm:
                logging.warning("Both start_time_vm and prev_start_time_vm are None. Unsure whether to turn off DAG.")
                destroy_worker_vm = False
                break

            # returns last execution date of previous vm cycle or None if a DAG is running
            last_execution_prev = get_last_execution_prev(dag, dag_id, prev_start_time_vm)
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
            destroy_worker_vm = check_success_runs(dag_id, execution_dates)
            if destroy_worker_vm is False:
                break

        logging.info(f"Destroying worker VM: {destroy_worker_vm}")
        if destroy_worker_vm:
            return TerraformTasks.TASK_ID_VAR_UPDATE
        else:
            return TerraformTasks.TASK_ID_VM_RUNTIME

    @staticmethod
    def check_runtime_vm(**kwargs):
        """Checks how long the VM has been turned on based on the xcom value from the terraform run task.
        A warning message will be send in a slack channel if it has been on longer than the warning limit,
        the environment isn't develop and a message hasn't been send already in the last x hours.

        :param kwargs: the context passed from the PythonOperator. See
        https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are passed to
        this argument.
        :return: None
        """

        ti: TaskInstance = kwargs["ti"]
        last_warning_time = ti.xcom_pull(
            key=TerraformTasks.XCOM_WARNING_TIME,
            task_ids=TerraformTasks.TASK_ID_VM_RUNTIME,
            dag_id=TerraformTasks.DAG_ID_DESTROY_VM,
            include_prior_dates=True,
        )
        start_time_vm = ti.xcom_pull(
            key=TerraformTasks.XCOM_START_TIME_VM,
            task_ids=TerraformTasks.TASK_ID_RUN,
            dag_id=TerraformTasks.DAG_ID_CREATE_VM,
            include_prior_dates=True,
        )

        if start_time_vm:
            # calculate number of hours passed since start time vm and now
            hours_on = (ti.start_date - start_time_vm).total_seconds() / 3600
            logging.info(
                f"Start time VM: {start_time_vm}, hours passed since start time: {hours_on}, warning limit: "
                f"{TerraformTasks.VM_RUNTIME_H_WARNING}"
            )

            # check if a warning has been sent previously and if so, how many hours ago
            if last_warning_time:
                hours_since_warning = (ti.start_date - last_warning_time).total_seconds() / 3600
            else:
                hours_since_warning = None

            # check if the VM has been on longer than the limit
            if hours_on > TerraformTasks.VM_RUNTIME_H_WARNING:
                #  check if no warning was sent before or last time was longer ago than warning frequency
                if not hours_since_warning or hours_since_warning > TerraformTasks.WARNING_FREQUENCY_H:
                    comments = (
                        f"Worker VM has been on since {start_time_vm}. No. hours passed since then: "
                        f"{hours_on}."
                        f" Warning limit: {TerraformTasks.VM_RUNTIME_H_WARNING}H"
                    )
                    project_id = Variable.get(AirflowVars.PROJECT_ID)
                    slack_hook = create_slack_webhook(comments, project_id, **kwargs)

                    # http_hook outputs the secret token, suppressing logging 'info' by setting level to 'warning'
                    old_levels = change_task_log_level(logging.WARNING)
                    slack_hook.execute()
                    # change back to previous levels
                    change_task_log_level(old_levels)

                    ti.xcom_push(TerraformTasks.XCOM_WARNING_TIME, ti.start_date)
        else:
            logging.info(f"Start time VM unknown.")
