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

import json
import logging
from typing import Any, List, Optional, Union

import airflow.secrets
from airflow.contrib.hooks.slack_webhook_hook import SlackWebhookHook
from airflow.hooks.base_hook import BaseHook
from airflow.models import Connection, TaskInstance, Variable
from airflow.utils.db import create_session
from google.api_core.exceptions import PermissionDenied


class AirflowVars:
    """ Common Airflow Variable names used with the Observatory Platform """

    TEST_DATA_PATH = "test_data_path"
    DATA_PATH = "data_path"
    ENVIRONMENT = "environment"
    PROJECT_ID = "project_id"
    DATA_LOCATION = "data_location"
    DOWNLOAD_BUCKET = "download_bucket"
    TRANSFORM_BUCKET = "transform_bucket"
    TERRAFORM_ORGANIZATION = "terraform_organization"
    DAGS_MODULE_NAMES = "dags_module_names"
    KIBANA_SPACES = "kibana_spaces"


class AirflowConns:
    """ Common Airflow Connection names used with the Observatory Platform """

    CROSSREF = "crossref"
    MAG_RELEASES_TABLE = "mag_releases_table"
    MAG_SNAPSHOTS_CONTAINER = "mag_snapshots_container"
    TERRAFORM = "terraform"
    SLACK = "slack"
    ELASTIC = "elastic"
    KIBANA = "kibana"
    SFTP_SERVICE = "sftp_service"


def get_variable(key: str) -> Optional[str]:
    """ Get Airflow Variable by iterating over all Secret Backends.

    :param key: Variable Key
    :return: Variable Value
    """
    for secrets_backend in airflow.secrets.ensure_secrets_loaded():
        # Added try/except statement.
        try:
            var_val = secrets_backend.get_variable(key=key)
        except PermissionDenied as err:
            print(f'Secret does not exist or cannot be accessed: {err}')
            var_val = None
        if var_val is not None:
            return var_val

    return None


class AirflowVariable(Variable):
    __NO_DEFAULT_SENTINEL = object()

    @classmethod
    def get(cls, key: str, default_var: Any = __NO_DEFAULT_SENTINEL, deserialize_json: bool = False, session=None):
        var_val = get_variable(key=key)

        if var_val is None:
            if default_var is not cls.__NO_DEFAULT_SENTINEL:
                return default_var
            else:
                raise KeyError('Variable {} does not exist'.format(key))
        else:
            if deserialize_json:
                return json.loads(var_val)
            else:
                return var_val


def change_task_log_level(new_levels: Union[List, int]) -> list:
    """ Change the logging levels of all handlers for an airflow task.

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


def list_connections(source):
    """ Get a list of data source connections with name starting with <source>_, e.g., wos_curtin.

    :param source: Data source (conforming to name convention) as a string, e.g., 'wos'.
    :return: A list of connection id strings with the prefix <source>_, e.g., ['wos_curtin', 'wos_auckland'].
    """
    with create_session() as session:
        query = session.query(Connection)
        query = query.filter(Connection.conn_id.like(f'{source}_%'))
        return query.all()


def check_variables(*variables):
    """ Checks whether all given airflow variables exist.

    :param variables: name of airflow variable
    :return: True if all variables are valid
    """
    is_valid = True
    for name in variables:
        try:
            AirflowVariable.get(name)
        except KeyError:
            logging.error(f"Airflow variable '{name}' not set.")
            is_valid = False
    return is_valid


def check_connections(*connections):
    """ Checks whether all given airflow connections exist.

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


def create_slack_webhook(comments: str = "", project_id: str = "?", **kwargs) -> SlackWebhookHook:
    """
    Creates a slack webhook using the token in the slack airflow connection.
    :param comments: Additional comments in slack message
    :param project_id: The google cloud project id that will be displayed in the slack message
    :param kwargs: the context passed from the PythonOperator. See
    https://airflow.apache.org/docs/stable/macros-ref.html for a list of the keyword arguments that are passed to
    this  argument.
    :return: slack webhook
    """
    ti: TaskInstance = kwargs['ti']

    message = """
    :red_circle: Task Alert. 
    *Task*: {task}  
    *Dag*: {dag} 
    *Execution Time*: {exec_date}  
    *Log Url*: {log_url} 
    *Project id*: {project_id}
    *Comments*: {comments}
    """.format(task=ti.task_id, dag=ti.dag_id, ti=ti, exec_date=kwargs['execution_date'], log_url=ti.log_url,
               comments=comments, project_id=project_id)
    slack_conn = BaseHook.get_connection(AirflowConns.SLACK)
    slack_hook = SlackWebhookHook(http_conn_id=slack_conn.conn_id, webhook_token=slack_conn.password, message=message)
    return slack_hook
