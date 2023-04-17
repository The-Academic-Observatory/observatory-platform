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

# Author: Author: Aniek Roelofs, Tuan Chien

from __future__ import annotations

import json
import logging
import traceback
from datetime import timedelta
from pydoc import locate
from typing import List, Union
from typing import Optional

import pendulum
import six
import validators
from airflow import AirflowException
from airflow.hooks.base import BaseHook
from airflow.models import TaskInstance, DagBag, Variable, XCom, DagRun
from airflow.providers.slack.operators.slack_webhook import SlackWebhookHook
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.db import provide_session
from airflow.utils.state import State
from dateutil.relativedelta import relativedelta
from sqlalchemy import and_
from sqlalchemy.orm import Session

from observatory.platform.config import AirflowConns, AirflowVars
from observatory.platform.observatory_config import Workflow

ScheduleInterval = Union[str, timedelta, relativedelta]


def change_task_log_level(new_levels: Union[List, int]) -> list:
    """Change the logging levels of all handlers for an airflow task.

    :param new_levels: New logging levels that all handlers will be set to
    :return: List of the old logging levels, can be used to restore logging levels.
    """
    logger = logging.getLogger("airflow.task")
    # stores logging levels
    old_levels = []
    for count, handler in enumerate(logger.handlers):
        old_levels.append(handler.level)
        if isinstance(new_levels, int):
            handler.setLevel(new_levels)
        else:
            handler.setLevel(new_levels[count])
    return old_levels


def check_variables(*variables):
    """Checks whether all given airflow variables exist.

    :param variables: name of airflow variable
    :return: True if all variables are valid
    """
    is_valid = True
    for name in variables:
        try:
            Variable.get(name)
        except KeyError:
            logging.error(f"Airflow variable '{name}' not set.")
            is_valid = False
    return is_valid


def check_connections(*connections):
    """Checks whether all given airflow connections exist.

    :param connections: name of airflow connection
    :return: True if all connections are valid
    """
    is_valid = True
    for name in connections:
        try:
            BaseHook.get_connection(name)
        except KeyError:
            logging.error(f"Airflow connection '{name}' not set.")
            is_valid = False
    return is_valid


def send_slack_msg(
    *, ti: TaskInstance, execution_date: pendulum.DateTime, comments: str = "", slack_conn_id: str = AirflowConns.SLACK
):
    """
    Send a Slack message using the token in the slack airflow connection.
    :param ti: Task instance.
    :param execution_date: DagRun execution date.
    :param comments: Additional comments in slack message
    :param slack_conn_id: the Airflow connection id for the Slack connection.
    """

    message = """
    :red_circle: Task Alert.
    *Task*: {task}
    *Dag*: {dag}
    *Execution Time*: {exec_date}
    *Log Url*: {log_url}
    *Comments*: {comments}
    """.format(
        task=ti.task_id,
        dag=ti.dag_id,
        exec_date=execution_date,
        log_url=ti.log_url,
        comments=comments,
    )
    slack_conn = BaseHook.get_connection(slack_conn_id)
    slack_hook = SlackWebhookHook(http_conn_id=slack_conn.conn_id, webhook_token=slack_conn.password, message=message)

    # http_hook outputs the secret token, suppressing logging 'info' by setting level to 'warning'
    old_levels = change_task_log_level(logging.WARNING)
    slack_hook.execute()
    # change back to previous levels
    change_task_log_level(old_levels)


def get_airflow_connection_url(conn_id: str) -> str:
    """Get the Airflow connection host, validate it is a valid url, and return it if it is, with a trailing /,
        otherwise throw an exception.  Assumes the connection_id exists.

    :param conn_id: Airflow connection id.
    :return: Connection URL with a trailing / added if necessary, or raise an exception if it is not a valid URL.
    """

    conn = BaseHook.get_connection(conn_id)
    url = conn.get_uri()

    if validators.url(url) != True:
        raise AirflowException(f"Airflow connection id {conn_id} does not have a valid url: {url}")

    if url[-1] != "/":
        url += "/"

    return url


def get_airflow_connection_login(conn_id: str) -> str:
    """Get the Airflow connection login. Assumes the connection_id exists.

    :param conn_id: Airflow connection id.
    :return: Connection login.
    """

    conn = BaseHook.get_connection(conn_id)
    login = conn.login

    if login is None:
        raise AirflowException(f"get_airflow_connection_login: login for Airflow Connection {conn_id} is set to None")

    return login


def get_airflow_connection_password(conn_id: str) -> str:
    """Get the Airflow connection password. Assumes the connection_id exists.

    :param conn_id: Airflow connection id.
    :return: Connection password.
    """

    conn = BaseHook.get_connection(conn_id)
    password = conn.get_password()

    if password is None:
        raise AirflowException(
            f"get_airflow_connection_password: password for Airflow Connection {conn_id} is set to None"
        )

    return password


def on_failure_callback(context):
    """
    Function that is called on failure of an airflow task. Will create a slack webhook and send a notification.

    :param context: the context passed from the PythonOperator. See
    https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are passed to
    this  argument.
    :return: None.
    """

    exception = context.get("exception")
    if isinstance(exception, Exception):
        formatted_exception = "".join(
            traceback.format_exception(etype=type(exception), value=exception, tb=exception.__traceback__)
        ).strip()
    else:
        formatted_exception = exception

    comments = f"Task failed, exception:\n{formatted_exception}"
    ti = context["ti"]
    execution_date = context["execution_date"]
    send_slack_msg(ti=ti, execution_date=execution_date, comments=comments, slack_conn_id=AirflowConns.SLACK)


def normalized_schedule_interval(schedule_interval: Optional[str]) -> Optional[ScheduleInterval]:
    """
    Copied from https://github.com/apache/airflow/blob/main/airflow/models/dag.py#L851-L866

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.
    Returns Normalized Schedule Interval. This is used internally by the Scheduler to
    schedule DAGs.

    1. Converts Cron Preset to a Cron Expression (e.g ``@monthly`` to ``0 0 1 * *``)
    2. If Schedule Interval is "@once" return "None"
    3. If not (1) or (2) returns schedule_interval
    """
    cron_presets = {
        "@hourly": "0 * * * *",
        "@daily": "0 0 * * *",
        "@weekly": "0 0 * * 0",
        "@monthly": "0 0 1 * *",
        "@quarterly": "0 0 1 */3 *",
        "@yearly": "0 0 1 1 *",
    }
    if isinstance(schedule_interval, six.string_types) and schedule_interval in cron_presets:
        _schedule_interval = cron_presets.get(schedule_interval)  # type: Optional[ScheduleInterval]
    elif schedule_interval == "@once":
        _schedule_interval = None
    else:
        _schedule_interval = schedule_interval
    return _schedule_interval


def get_data_path() -> str:
    """Grabs the DATA_PATH airflow vairable

    :raises AirflowException: Raised if the variable does not exist
    :return: DATA_PATH variable contents
    """
    # Try to get value from env variable first, saving costs from GC secret usage
    data_path = Variable.get(AirflowVars.DATA_PATH)
    if not data_path:
        raise AirflowException("DATA_PATH variable could not be found.")
    return data_path


def fetch_workflows() -> List[Workflow]:
    """Get the workflows from the Airflow Variable

    :return: the workflows to create.
    """

    workflows_str = Variable.get(AirflowVars.WORKFLOWS)
    logging.info(f"workflows_str: {workflows_str}")

    try:
        workflows = json.loads(workflows_str)
        workflows = Workflow.parse_workflows(workflows)
        logging.info(f"workflows: {workflows}")
    except json.decoder.JSONDecodeError as e:
        e.msg = f"workflows_str: {workflows_str}\n\n{e.msg}"
        raise e

    return workflows


def fetch_dags_modules() -> dict:
    """Get the dags modules from the Airflow Variable

    :return: Dags modules
    """

    dags_modules_str = Variable.get(AirflowVars.DAGS_MODULE_NAMES)
    logging.info(f"dags_modules_str: {dags_modules_str}")

    try:
        dags_modules_ = json.loads(dags_modules_str)
        logging.info(f"dags_modules: {dags_modules_}")
    except json.decoder.JSONDecodeError as e:
        e.msg = f"dags_modules_str: {dags_modules_str}\n\n{e.msg}"
        raise e

    return dags_modules_


def make_workflow(workflow: Workflow):
    """Make a workflow instance.
    :param workflow: the workflow configuration.
    :return: the workflow instance.
    """

    cls = locate(workflow.class_name)
    if cls is None:
        raise ModuleNotFoundError(f"dag_id={workflow.dag_id}: could not locate class_name={workflow.class_name}")

    return cls(dag_id=workflow.dag_id, cloud_workspace=workflow.cloud_workspace, **workflow.kwargs)


def fetch_dag_bag(path: str, include_examples: bool = False) -> DagBag:
    """Load a DAG Bag from a given path.

    :param path: the path to the DAG bag.
    :param include_examples: whether to include example DAGs or not.
    :return: None.
    """
    logging.info(f"Loading DAG bag from path: {path}")
    dag_bag = DagBag(path, include_examples=include_examples)

    if dag_bag is None:
        raise Exception(f"DagBag could not be loaded from path: {path}")

    if len(dag_bag.import_errors):
        # Collate loading errors as single string and raise it as exception
        results = []
        for path, exception in dag_bag.import_errors.items():
            results.append(f"DAG import exception: {path}\n{exception}\n\n")
        raise Exception("\n".join(results))

    return dag_bag


@provide_session
def delete_old_xcoms(
    session: Session = None,
    dag_id: str = None,
    execution_date: pendulum.DateTime = None,
    retention_days: int = 31,
):
    """Delete XCom messages created by the DAG with the given ID that are as old or older than than
    execution_date - retention_days.  Defaults to 31 days of retention.

    :param session: DB session.
    :param dag_id: DAG ID.
    :param execution_date: DAG execution date.
    :param retention_days: Days of messages to retain.
    """

    cut_off_date = execution_date.subtract(days=retention_days)
    session.query(XCom).filter(
        and_(
            XCom.dag_id == dag_id,
            XCom.execution_date <= cut_off_date,
        )
    ).delete()


def is_first_dag_run(dag_run: DagRun) -> bool:
    """Whether the DAG Run is the first run or not

    :param dag_run: A Dag Run instance
    :return: Whether the DAG run is the first run or not
    """

    return dag_run.get_previous_dagrun() is None


class PreviousDagRunSensor(ExternalTaskSensor):
    def __init__(
        self,
        dag_id: str,
        task_id: str = "wait_for_prev_dag_run",
        external_task_id: str = "dag_run_complete",
        allowed_states: List[str] = None,
        *args,
        **kwargs,
    ):
        """Custom ExternalTaskSensor designed to wait for a previous DAG run of the same DAG. This sensor also
        skips on the first DAG run, as the DAG hasn't run before.

        Add the following as the last tag of your DAG:
            DummyOperator(
                task_id=external_task_id,
            )

        :param dag_id: the DAG id of the DAG to wait for.
        :param task_id: the task id for this sensor.
        :param external_task_id: the task id to wait for.
        :param allowed_states: to override allowed_states.
        :param args: args for ExternalTaskSensor.
        :param kwargs: kwargs for ExternalTaskSensor.
        """

        if allowed_states is None:
            # sensor can skip a run if previous dag run skipped for some reason
            allowed_states = [
                State.SUCCESS,
                State.SKIPPED,
            ]

        super().__init__(
            task_id=task_id,
            external_dag_id=dag_id,
            external_task_id=external_task_id,
            allowed_states=allowed_states,
            *args,
            **kwargs,
        )

    @provide_session
    def poke(self, context, session=None):
        # Custom poke to allow the sensor to skip on the first DAG run

        ti = context["task_instance"]
        dag_run = context["dag_run"]

        if is_first_dag_run(dag_run):
            self.log.info("Skipping the sensor check on the first DAG run")
            ti.set_state(State.SKIPPED)
            return True

        return super().poke(context, session=session)
