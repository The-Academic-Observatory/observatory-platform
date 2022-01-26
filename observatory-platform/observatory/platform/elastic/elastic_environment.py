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
import time
from typing import Dict

import requests
from elasticsearch import Elasticsearch

from observatory.platform.docker.compose import ComposeRunner, ProcessOutput
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
    ):
        """Construct an Elasticsearch and Kibana environment.

        :param build_path: the path to the build directory.
        :param elastic_port: the Elastic port.
        :param kibana_port: the Kibana port.
        :param wait: whether to wait until Elastic and Kibana have started.
        :param wait_time_secs: the maximum wait time in seconds.
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
            compose_template_kwargs={"elastic_port": elastic_port, "kibana_port": kibana_port, "password": password},
            debug=True,
        )

        # Add files
        self.add_file(
            path=os.path.join(self.elastic_module_path, "elasticsearch.yml"), output_file_name="elasticsearch.yml"
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
        return process_output

    def kibana_ping(self):
        """Check if Kibana has started or not.

        :return: whether Kibana has started or not.
        """

        try:
            response = requests.get(self.kibana_uri)
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
            elastic_found = es.ping()
            kibana_found = self.kibana_ping()
            services_found = elastic_found and kibana_found
            if services_found:
                break

            # Break out if time lasts too long
            elapsed = time.time() - start
            if elapsed > self.wait_time_secs:
                break

        return services_found
