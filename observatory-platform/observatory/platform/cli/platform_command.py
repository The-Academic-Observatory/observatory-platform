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

from typing import Union

from observatory.platform.platform_builder import (PlatformBuilder, BUILD_PATH, DAGS_MODULE, DATA_PATH, LOGS_PATH,
                                                   POSTGRES_PATH, HOST_UID, HOST_GID, REDIS_PORT, FLOWER_UI_PORT,
                                                   AIRFLOW_UI_PORT, ELASTIC_PORT, KIBANA_PORT,
                                                   DOCKER_NETWORK_NAME, DEBUG)
from observatory.platform.utils.url_utils import wait_for_url


class PlatformCommand(PlatformBuilder):

    def __init__(self, config_path: str, build_path: str = BUILD_PATH, dags_path: str = DAGS_MODULE,
                 data_path: str = DATA_PATH, logs_path: str = LOGS_PATH, postgres_path: str = POSTGRES_PATH,
                 host_uid: int = HOST_UID, host_gid: int = HOST_GID, redis_port: int = REDIS_PORT,
                 flower_ui_port: int = FLOWER_UI_PORT, airflow_ui_port: int = AIRFLOW_UI_PORT,
                 elastic_port: int = ELASTIC_PORT, kibana_port: int = KIBANA_PORT,
                 docker_network_name: Union[None, int] = DOCKER_NETWORK_NAME, debug: bool = DEBUG):
        """ Create a PlatformCommand, which can be used to start and stop Observatory Platform instances.

        :param config_path: The path to the config.yaml configuration file.
        :param dags_path: The path on the host machine to mount as the Apache Airflow DAGs folder.
        :param data_path: The path on the host machine to mount as the data folder.
        :param logs_path: The path on the host machine to mount as the logs folder.
        :param postgres_path: The path on the host machine to mount as the PostgreSQL data folder.
        :param host_uid: The user id of the host system. Used to set the user id in the Docker containers.
        :param host_gid: The group id of the host system. Used to set the group id in the Docker containers.
        :param redis_port: The host Redis port number.
        :param flower_ui_port: The host's Flower UI port number.
        :param airflow_ui_port: The host's Apache Airflow UI port number.
        :param elastic_port: The host's Elasticsearch port number.
        :param kibana_port: The host's Kibana port number.
        :param docker_network_name: The Docker Network name, used to specify a custom Docker Network.
        :param debug: Print debugging information.
        """

        is_env_local = True
        super().__init__(config_path, build_path=build_path, dags_path=dags_path,
                         data_path=data_path, logs_path=logs_path, postgres_path=postgres_path,
                         host_uid=host_uid, host_gid=host_gid, redis_port=redis_port,
                         flower_ui_port=flower_ui_port, airflow_ui_port=airflow_ui_port,
                         elastic_port=elastic_port, kibana_port=kibana_port,
                         docker_network_name=docker_network_name, debug=debug, is_env_local=is_env_local)

    @property
    def ui_url(self) -> str:
        """ Return the URL to Apache Airflow UI.

        :return: Apache Airflow UI URL.
        """

        return f'http://localhost:{self.airflow_ui_port}'

    def wait_for_airflow_ui(self, timeout: int = 60) -> bool:
        """ Wait for the Apache Airflow UI to start.

        :param timeout: the number of seconds to wait before timing out.
        :return: whether connecting to the Apache Airflow UI was successful or not.
        """

        return wait_for_url(self.ui_url, timeout=timeout)
