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

import os

import docker
from click.testing import CliRunner

from observatory.platform.docker.builder import Builder
from observatory.platform.observatory_config import BuildConfig
from observatory.platform.utils.config_utils import module_file_path
from observatory.platform.utils.jinja2_utils import make_jinja2_filename


class PlatformBuilder:

    DOCKERFILE = "Dockerfile.observatory"
    ENTRYPOINT_AIRFLOW = "entrypoint-airflow.sh"
    ENTRYPOINT_ROOT = "entrypoint-root.sh"
    ELASTICSEARCH = "elasticsearch.yml"

    def __init__(self, *, config: BuildConfig, tag: str):
        """Platform builder constructor."""

        self.config = config
        self.tag = tag

    @property
    def docker_module_path(self) -> str:
        """The path to the Observatory Platform docker module.
        :return: the path.
        """

        return module_file_path("observatory.platform.docker")

    def build(self) -> bool:
        """ Build the Docker image.
        :return: whether the image built successfully or not.
        """

        with CliRunner().isolated_filesystem() as t:
            # Create builder and add files
            builder = Builder(build_path=t)
            builder.add_template(
                path=os.path.join(self.docker_module_path, make_jinja2_filename(self.DOCKERFILE)), config=self.config
            )
            builder.add_template(
                path=os.path.join(self.docker_module_path, make_jinja2_filename(self.ENTRYPOINT_AIRFLOW)),
                config=self.config,
            )
            builder.add_file(
                path=os.path.join(self.docker_module_path, self.ENTRYPOINT_ROOT), output_file_name=self.ENTRYPOINT_ROOT
            )
            builder.add_file(
                path=os.path.join(self.docker_module_path, self.ELASTICSEARCH), output_file_name=self.ELASTICSEARCH
            )

            # Add all project requirements files for local projects
            for package in self.config.python_packages:
                if package.type == "editable":
                    # Add requirements.sh
                    builder.add_file(
                        path=os.path.join(package.host_package, "requirements.sh"),
                        output_file_name=f"requirements.{package.name}.sh",
                    )

                    # Add project requirements files for local projects
                    builder.add_file(
                        path=os.path.join(package.host_package, "requirements.txt"),
                        output_file_name=f"requirements.{package.name}.txt",
                    )
                elif package.type == "sdist":
                    # Add sdist package file
                    builder.add_file(path=package.host_package, output_file_name=package.docker_package)

            # Generate files
            builder.make_files()

            # Build image
            client = docker.from_env()
            success = False
            for line in client.api.build(path=t, dockerfile=self.DOCKERFILE, rm=True, decode=True, tag=self.tag):
                if "stream" in line:
                    stream = line["stream"].rstrip()
                    print(stream)
                elif "error" in line:
                    error = line["error"].rstrip()
                    print(error)
                    success = False
                elif "aux" in line:
                    success = True

        return success