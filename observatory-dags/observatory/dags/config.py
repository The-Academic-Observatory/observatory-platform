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

# Author: James Diprose

from observatory.platform.utils.config_utils import module_file_path


def schema_path() -> str:
    """Return the path to the database schema templates.

    :return: the path.
    """

    return module_file_path("observatory.dags.database.schema")


def workflow_sql_templates_path() -> str:
    """Return the path to the workflow SQL templates.

    :return: the path.
    """

    return module_file_path("observatory.dags.database.sql")


def make_elastic_mappings_path() -> str:
    """Get the Elasticsearch mappings path.

    :return: the elastic search schema path.
    """

    return module_file_path("observatory.dags.database.mappings")

