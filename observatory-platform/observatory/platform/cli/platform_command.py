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

import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Union

from observatory.platform.platform_builder import PlatformBuilder


class PlatformCommand(PlatformBuilder):

    def __init__(self, config_path: str, dags_path: str, data_path: str, logs_path: str, postgres_path: str,
                 host_uid: int, host_gid: int, redis_port: int, flower_ui_port: int, airflow_ui_port: int,
                 elastic_port: int, kibana_port: int, docker_network_name: Union[None, int], debug: bool):
        """ Create a PlatformCommand, which is able to start and stop Observatory Platform instances.

        :param config_path: the path to the configuration file.
        :param dags_path: the path to the Observatory Platform DAGs.
        :param data_path: the path to where data is stored.
        :param logs_path: the path to log files.
        :param postgres_path: the path to postgres SQL data files.
        :param host_uid: the user of the host machine user.
        :param host_gid: the group id of the host machine user.
        :param redis_port: the host Redis port.
        :param flower_ui_port: the host Flower UI port.
        :param airflow_ui_port: the host Apache Airflow UI port.
        :param elastic_port: the host Elasticsearch port.
        :param kibana_port: the host Kibana port.
        :param docker_network_name: the name of an external Docker network.
        :param debug: whether to run the Observatory in debug mode or not; in which case it prints extra information.
        """

        is_local_env = True
        super().__init__(config_path, dags_path, data_path, logs_path, postgres_path, host_uid, host_gid,
                         redis_port, flower_ui_port, airflow_ui_port, elastic_port, kibana_port, docker_network_name,
                         debug, is_local_env)

    @property
    def ui_url(self) -> str:
        """ Return the URL to Apache Airflow UI.

        :return: Apache Airflow UI URL.
        """

        return f'http://localhost:{self.airflow_ui_port}'

    def wait_for_airflow_ui(self, timeout: int = 60) -> bool:
        """ Wait for the Apache Airflow UI to start.

        :param ui_url: the URL to the Apache Airflow UI.
        :param timeout: the number of seconds to wait before timing out.
        :return: whether connecting to the Apache Airflow UI was successful or not.
        """

        start = time.time()
        ui_started = False
        while True:
            duration = time.time() - start
            if duration >= timeout:
                break

            try:
                if urllib.request.urlopen(self.ui_url).getcode() == 200:
                    ui_started = True
                    break
                time.sleep(0.5)
            except ConnectionResetError:
                pass
            except ConnectionRefusedError:
                pass
            except urllib.error.URLError:
                pass

        return ui_started
