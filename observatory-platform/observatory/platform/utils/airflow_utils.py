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

"""
Airflow utility functions (independent of telescope or google cloud usage)
"""

import logging
from typing import List, Union

import pendulum
import validators
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.models import TaskInstance, Variable
from airflow.providers.slack.operators.slack_webhook import SlackWebhookHook


class AirflowVars:
    """Common Airflow Variable names used with the Observatory Platform"""

    DATA_PATH = "data_path"
    ENVIRONMENT = "environment"
    PROJECT_ID = "project_id"
    DATA_LOCATION = "data_location"
    DOWNLOAD_BUCKET = "download_bucket"
    TRANSFORM_BUCKET = "transform_bucket"
    TERRAFORM_ORGANIZATION = "terraform_organization"
    DAGS_MODULE_NAMES = "dags_module_names"
    ORCID_BUCKET = "orcid_bucket"
    VM_DAGS_WATCH_LIST = "vm_dags_watch_list"
    BIGQUERY_DAILY_BYTES_LIMIT = "bigquery_daily_bytes_limit"


class AirflowConns:
    """Common Airflow Connection names used with the Observatory Platform"""

    CROSSREF = "crossref"
    TERRAFORM = "terraform"
    SLACK = "slack"
    GEOIP_LICENSE_KEY = "geoip_license_key"
    OAPEN_IRUS_UK_API = "oapen_irus_uk_api"
    OAPEN_IRUS_UK_LOGIN = "oapen_irus_uk_login"
    OAEBU_SERVICE_ACCOUNT = "oaebu_service_account"
    SFTP_SERVICE = "sftp_service"
    OBSERVATORY_API = "observatory_api"
    GMAIL_API = "gmail_api"
    ORCID = "orcid"


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
    *,
    ti: TaskInstance,
    execution_date: pendulum.DateTime,
    comments: str = "",
    project_id: str = "?",
):
    """
    Send a slack message using the token in the slack airflow connection.
    :param ti: Task instance.
    :param execution_date: DagRun execution date.
    :param comments: Additional comments in slack message
    :param project_id: The google cloud project id that will be displayed in the slack message
    """

    message = """
    :red_circle: Task Alert.
    *Task*: {task}
    *Dag*: {dag}
    *Execution Time*: {exec_date}
    *Log Url*: {log_url}
    *Project id*: {project_id}
    *Comments*: {comments}
    """.format(
        task=ti.task_id,
        dag=ti.dag_id,
        exec_date=execution_date,
        log_url=ti.log_url,
        comments=comments,
        project_id=project_id,
    )
    slack_conn = BaseHook.get_connection(AirflowConns.SLACK)
    slack_hook = SlackWebhookHook(http_conn_id=slack_conn.conn_id, webhook_token=slack_conn.password, message=message)

    # http_hook outputs the secret token, suppressing logging 'info' by setting level to 'warning'
    old_levels = change_task_log_level(logging.WARNING)
    slack_hook.execute()
    # change back to previous levels
    change_task_log_level(old_levels)


def set_task_state(success: bool, task_id: str):
    """Update the state of the Airflow task.
    :param success: whether the task was successful or not.
    :param task_id: the task id.
    :return: None.
    """

    if success:
        logging.info(f"{task_id} success")
    else:
        msg_failed = f"{task_id} failed"
        logging.error(msg_failed)
        raise AirflowException(msg_failed)


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
    return conn.login


def get_airflow_connection_password(conn_id: str) -> str:
    """Get the Airflow connection password. Assumes the connection_id exists.

    :param conn_id: Airflow connection id.
    :return: Connection password.
    """

    conn = BaseHook.get_connection(conn_id)
    password = conn.get_password()
    return password
