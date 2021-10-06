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

# Author: James Diprose

import os.path
import unittest

from click.testing import CliRunner

from observatory.platform.docker.compose_runner import ComposeRunner
from observatory.platform.utils.test_utils import find_free_port
from observatory.platform.utils.url_utils import retry_session

DOCKERFILE = """
FROM nginx
RUN echo "hello world"
"""

DOCKER_COMPOSE_TEMPLATE = """
version: '3'
services:
  web:
    build: .
    ports:
      - {{ port }}:80
"""


class TestComposeRunner(unittest.TestCase):
    def test_compose_file_name(self):
        compose_template_path = "docker-compose.yml.jinja2"
        runner = ComposeRunner(compose_template_path=compose_template_path, build_path="/")
        self.assertEqual("docker-compose.yml", runner.compose_file_name)

    def test_build_start_stop(self):
        with CliRunner().isolated_filesystem() as t:
            build = os.path.join(t, "build")
            port = find_free_port()

            # Save docker compose template
            compose_template_path = os.path.join(t, "docker-compose.yml.jinja2")
            with open(compose_template_path, mode="w") as f:
                f.write(DOCKER_COMPOSE_TEMPLATE)

            # Save Dockerfile
            dockerfile_name = "Dockerfile"
            dockerfile_path = os.path.join(t, dockerfile_name)
            with open(dockerfile_path, mode="w") as f:
                f.write(DOCKERFILE)

            # Create compose runner
            runner = ComposeRunner(
                compose_template_path=compose_template_path,
                compose_template_kwargs={"port": port},
                build_path=build,
                debug=True,
            )
            runner.add_file(path=dockerfile_path, output_file_name=dockerfile_name)

            # Build
            p = runner.build()
            self.assertEqual(0, p.return_code)

            # Start and stop
            try:
                p = runner.start()
                self.assertEqual(0, p.return_code)

                # Test that the port is up
                response = retry_session().get(f"http://localhost:{port}")
                self.assertEqual(200, response.status_code)

                p = runner.stop()
                self.assertEqual(0, p.return_code)
            finally:
                runner.stop()
