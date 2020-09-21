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

# Author: Author: Aniek Roelofs

import json
import logging
from typing import Any, List, Optional, Union

import airflow.secrets
from airflow.models import Variable
from google.api_core.exceptions import PermissionDenied


def get_variable(key: str) -> Optional[str]:
    """
    Get Airflow Variable by iterating over all Secret Backends.

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
    """
    Change the logging levels of all handlers for an airflow task.

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
