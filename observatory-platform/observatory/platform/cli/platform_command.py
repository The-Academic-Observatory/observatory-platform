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

from observatory.platform.observatory_config import BackendType
from observatory.platform.platform_builder import PlatformBuilder, HOST_UID, DEBUG
from observatory.platform.utils.url_utils import wait_for_url


class PlatformCommand(PlatformBuilder):
    def __init__(self, config_path: str, host_uid: int = HOST_UID, debug: bool = DEBUG):
        """Create a PlatformCommand, which can be used to start and stop Observatory Platform instances.

        :param config_path: The path to the config.yaml configuration file.
        :param host_uid: The user id of the host system. Used to set the user id in the Docker containers.
        :param debug: Print debugging information.
        """

        super().__init__(
            config_path=config_path, host_uid=host_uid, debug=debug, backend_type=BackendType.local
        )

    @property
    def ui_url(self) -> str:
        """Return the URL to Apache Airflow UI.

        :return: Apache Airflow UI URL.
        """

        return f"http://localhost:{self.config.observatory.airflow_ui_port}"

    def wait_for_airflow_ui(self, timeout: int = 60) -> bool:
        """Wait for the Apache Airflow UI to start.

        :param timeout: the number of seconds to wait before timing out.
        :return: whether connecting to the Apache Airflow UI was successful or not.
        """

        return wait_for_url(self.ui_url, timeout=timeout)
