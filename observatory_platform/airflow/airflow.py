# Copyright 2020-2023 Curtin University
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

import logging
import textwrap
import traceback
from datetime import timedelta
from typing import List, Union
from typing import Optional

import pendulum
import six
import validators
from airflow import AirflowException
from airflow.hooks.base import BaseHook
from airflow.models import TaskInstance, XCom, DagRun
from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
from airflow.utils.db import provide_session
from dateutil.relativedelta import relativedelta
from sqlalchemy import and_
from sqlalchemy.orm import Session

from observatory_platform.config import AirflowConns

ScheduleInterval = Union[str, timedelta, relativedelta]


def get_airflow_connection_url(conn_id: str) -> str:
    """Get the Airflow connection host, validate it is a valid url, and return it if it is, with a trailing /,
    otherwise throw an exception. Assumes the connection_id exists.

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


def is_first_dag_run(dag_run: DagRun) -> bool:
    """Whether the DAG Run is the first run or not

    :param dag_run: A Dag Run instance
    :return: Whether the DAG run is the first run or not
    """

    return dag_run.get_previous_dagrun() is None


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
    """Function that is called on failure of an airflow task. Will create a slack webhook and send a notification.

    :param context: the context passed from the PythonOperator. See
    https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are passed to
    this  argument.
    :return: None.
    """

    exception = context.get("exception")
    if isinstance(exception, Exception):
        formatted_exception = "".join(
            traceback.format_exception(type(exception), value=exception, tb=exception.__traceback__)
        ).strip()
    else:
        formatted_exception = exception

    comments = f"Task failed, exception:\n{formatted_exception}"
    ti = context["ti"]
    execution_date = context["execution_date"]
    send_slack_msg(ti=ti, execution_date=execution_date, comments=comments, slack_conn_id=AirflowConns.SLACK)


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

    message = textwrap.dedent(
        """
        :red_circle: Task Alert.
        *Task*: {task}
        *Dag*: {dag}
        *Execution Time*: {exec_date}
        *Log Url*: {log_url}
        *Comments*: {comments}
        """
    ).format(
        task=ti.task_id,
        dag=ti.dag_id,
        exec_date=execution_date,
        log_url=ti.log_url,
        comments=comments,
    )
    hook = SlackWebhookHook(slack_webhook_conn_id=slack_conn_id)

    # http_hook outputs the secret token, suppressing logging 'info' by setting level to 'warning'
    old_levels = change_task_log_level(logging.WARNING)
    hook.send_text(message)
    # change back to previous levels
    change_task_log_level(old_levels)


def normalized_schedule_interval(schedule_interval: Optional[str]) -> Optional[ScheduleInterval]:
    """Copied from https://github.com/apache/airflow/blob/main/airflow/models/dag.py#L851-L866

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
    results = session.query(XCom).filter(
        and_(
            XCom.dag_id == dag_id,
            XCom.execution_date <= cut_off_date,
        )
    )
    # set synchronize_session="fetch" to prevent the following error: sqlalchemy.exc.InvalidRequestError: Could not evaluate current criteria in Python: "Cannot evaluate SelectStatementGrouping". Specify 'fetch' or False for the synchronize_session execution option.
    results.delete(synchronize_session="fetch")