# Copyright 2021 Curtin University
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

# Author: James Diprose, Aniek Roelofs

import logging
import os
import requests
import time
from elasticsearch import Elasticsearch
from typing import Dict

from observatory.platform.docker.compose_runner import ComposeRunner, ProcessOutput
from observatory.platform.docker.platform_runner import ELASTIC_VERSION, KIBANA_VERSION
from observatory.platform.utils.config_utils import module_file_path


class ElasticEnvironment(ComposeRunner):
    HTTP_OK = 200

    def __init__(
        self,
        build_path: str,
        elastic_port: int = 9200,
        kibana_port: int = 5601,
        password: str = "observatory",
        wait: bool = True,
        wait_time_secs: int = 120,
        elastic_version: str = ELASTIC_VERSION,
        kibana_version: str = KIBANA_VERSION,
    ):
        """Construct an Elasticsearch and Kibana environment.

        :param build_path: the path to the build directory.
        :param elastic_port: the Elastic port.
        :param kibana_port: the Kibana port.
        :param wait: whether to wait until Elastic and Kibana have started.
        :param wait_time_secs: the maximum wait time in seconds.
        :param elastic_version: the ElasticSearch Docker version.
        :param kibana_version: the Kibana Docker version.
        """

        self.elastic_module_path = module_file_path("observatory.platform.elastic")
        self.wait = wait
        self.wait_time_secs = wait_time_secs
        self.elastic_port = elastic_port
        self.elastic_uri = f"http://localhost:{elastic_port}/"
        self.kibana_uri = f"http://localhost:{kibana_port}/"
        self.password = password
        self.build_path = build_path
        super().__init__(
            compose_template_path=os.path.join(self.elastic_module_path, "docker-compose.yml.jinja2"),
            build_path=build_path,
            compose_template_kwargs={
                "elastic_port": elastic_port,
                "kibana_port": kibana_port,
                "password": password,
                "elastic_version": elastic_version,
                "kibana_version": kibana_version,
            },
            debug=True,
        )

        # Stop the awful unnecessary Elasticsearch connection warnings being logged
        logging.basicConfig()
        logging.getLogger().setLevel(logging.ERROR)

    def make_environment(self) -> Dict:
        """Make the environment when running the Docker Compose command.

        :return: the environment.
        """
        return os.environ.copy()

    def start(self) -> ProcessOutput:
        """Start the Elastic environment.

        :return: ProcessOutput.
        """

        self.stop()
        process_output = super().start()
        if self.wait:
            self.wait_until_started()
        time.sleep(20)
        return process_output

    def kibana_ping(self):
        """Check if Kibana has started or not.

        :return: whether Kibana has started or not.
        """

        try:
            # Have to call a specific API endpoint, not jus the base Kibana URI, as the base Kibana URI will return 200
            # before Kibana is actually ready.
            response = requests.get(f"{self.kibana_uri}api/spaces/space")
            return response.status_code == self.HTTP_OK
        except (ConnectionResetError, requests.exceptions.ConnectionError):
            pass
        return False

    def wait_until_started(self):
        """Wait until Elastic and Kibana have started.

        :return: whether started or not.
        """
        es = Elasticsearch([self.elastic_uri])
        start = time.time()
        while True:
            try:
                elastic_found = es.ping()
            except OverflowError:
                elastic_found = False
            kibana_found = self.kibana_ping()
            services_found = elastic_found and kibana_found
            if services_found:
                break

            # Break out if time lasts too long
            elapsed = time.time() - start
            if elapsed > self.wait_time_secs:
                break

        return services_found
