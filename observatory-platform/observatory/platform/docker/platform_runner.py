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

# Author: James Diprose, Aniek Roelofs


import os
import shutil

import docker
import requests

from observatory.platform.docker.compose_runner import ComposeRunner
from observatory.platform.observatory_config import Config
from observatory.platform.utils.config_utils import module_file_path

HOST_UID = os.getuid()
DEBUG = False


class PlatformRunner(ComposeRunner):
    def __init__(self, *, config: Config, host_uid: int = HOST_UID, docker_build_path: str = None, debug: bool = DEBUG):
        """Create a PlatformRunner instance, which is used to build, start and stop an Observatory Platform instance.

        :param config: the config.
        :param host_uid: The user id of the host system. Used to set the user id in the Docker containers.
        :param docker_build_path: the Docker build path.
        :param debug: Print debugging information.
        """

        self.config = config
        self.host_uid = host_uid

        # Set default values when config is invalid
        observatory_home = self.config.observatory.observatory_home

        if docker_build_path is None:
            docker_build_path = os.path.join(observatory_home, "build", "docker")

        super().__init__(
            compose_template_path=os.path.join(self.docker_module_path, "docker-compose.observatory.yml.jinja2"),
            build_path=docker_build_path,
            compose_template_kwargs={"config": self.config},
            debug=debug,
        )

        # Add files
        self.add_template(
            path=os.path.join(self.docker_module_path, "Dockerfile.observatory.jinja2"), config=self.config
        )
        self.add_template(
            path=os.path.join(self.docker_module_path, "entrypoint-airflow.sh.jinja2"), config=self.config
        )
        self.add_template(
            path=os.path.join(self.docker_module_path, "Dockerfile.apiserver.jinja2"), config=self.config
        )
        self.add_template(
            path=os.path.join(self.docker_module_path, "Dockerfile.seed_db.jinja2"), config=self.config
        )
        self.add_template(
            path=os.path.join(self.docker_module_path, "entrypoint-api.sh.jinja2"), config=self.config
        )
        self.add_file(
            path=os.path.join(self.docker_module_path, "entrypoint-root.sh"), output_file_name="entrypoint-root.sh"
        )
        self.add_template(path=os.path.join(self.docker_module_path, "seed_db.sh.jinja2"), config=self.config)

        # Add all project requirements files for local projects
        for package in self.config.python_packages:
            if package.type == "editable":
                # Add requirements.sh
                self.add_file(
                    path=os.path.join(package.host_package, "requirements.sh"),
                    output_file_name=f"requirements.{package.name}.sh",
                )
            elif package.type == "sdist":
                # Add sdist package file
                self.add_file(path=package.host_package, output_file_name=package.docker_package)

    @property
    def is_environment_valid(self) -> bool:
        """Return whether the environment for building the Observatory Platform is valid.

        :return: whether the environment for building the Observatory Platform is valid.
        """

        return all([self.docker_exe_path is not None, self.docker_compose_path is not None, self.is_docker_running])

    @property
    def docker_module_path(self) -> str:
        """The path to the Observatory Platform docker module.

        :return: the path.
        """

        return module_file_path("observatory.platform.docker")

    @property
    def docker_exe_path(self) -> str:
        """The path to the Docker executable.

        :return: the path or None.
        """

        return shutil.which("docker")

    @property
    def docker_compose_path(self) -> str:
        """The path to the Docker Compose executable.

        :return: the path or None.
        """

        return shutil.which("docker-compose")

    @property
    def is_docker_running(self) -> bool:
        """Checks whether Docker is running or not.

        :return: whether Docker is running or not.
        """

        client = docker.from_env()
        try:
            is_running = client.ping()
        except requests.exceptions.ConnectionError:
            is_running = False
        return is_running

    def make_environment(self):
        """Make an environment containing the environment variables that are required to build and start the
        Observatory docker environment.

        :return: None.
        """

        env = os.environ.copy()

        # Sets the name
        env["COMPOSE_PROJECT_NAME"] = self.config.observatory.docker_compose_project_name

        # Host settings
        env["HOST_USER_ID"] = str(self.host_uid)
        env["HOST_OBSERVATORY_HOME"] = os.path.normpath(self.config.observatory.observatory_home)
        env["HOST_REDIS_PORT"] = str(self.config.observatory.redis_port)
        env["HOST_FLOWER_UI_PORT"] = str(self.config.observatory.flower_ui_port)
        env["HOST_AIRFLOW_UI_PORT"] = str(self.config.observatory.airflow_ui_port)
        env["HOST_API_PORT"] = str(self.config.observatory.api_port)

        # Secrets
        if self.config.google_cloud is not None and self.config.google_cloud.credentials is not None:
            env["HOST_GOOGLE_APPLICATION_CREDENTIALS"] = self.config.google_cloud.credentials
        env["AIRFLOW_FERNET_KEY"] = self.config.observatory.airflow_fernet_key
        env["AIRFLOW_SECRET_KEY"] = self.config.observatory.airflow_secret_key
        env["AIRFLOW_UI_USER_EMAIL"] = self.config.observatory.airflow_ui_user_email
        env["AIRFLOW_UI_USER_PASSWORD"] = self.config.observatory.airflow_ui_user_password
        env["POSTGRES_PASSWORD"] = self.config.observatory.postgres_password

        # Create Airflow variables
        airflow_variables = self.config.make_airflow_variables()
        for variable in airflow_variables:
            env[variable.env_var_name] = str(variable.value)

        # Airflow connections
        for conn in self.config.airflow_connections:
            env[conn.conn_name] = conn.value

        return env

    def make_files(self):
        """Create directories that are mounted as volumes as defined in the docker-compose file.

        :return: None.
        """
        super(PlatformRunner, self).make_files()
        observatory_home = os.path.normpath(self.config.observatory.observatory_home)
        # Create data directory
        data_dir = os.path.join(observatory_home, "data")
        os.makedirs(data_dir, exist_ok=True)

        # Create logs directory
        logs_dir = os.path.join(observatory_home, "logs")
        os.makedirs(logs_dir, exist_ok=True)

        # Create postgres directory
        postgres_dir = os.path.join(observatory_home, "postgres")
        os.makedirs(postgres_dir, exist_ok=True)
