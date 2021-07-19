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
from typing import Union

import docker
import requests

from observatory.platform.docker.compose import ComposeRunner
from observatory.platform.observatory_config import ObservatoryConfig, BackendType, TerraformConfig, DagsProject
from observatory.platform.utils.config_utils import observatory_home, module_file_path

DAGS_MODULE = module_file_path("observatory.platform.dags")
DATA_PATH = observatory_home("data")
LOGS_PATH = observatory_home("logs")
POSTGRES_PATH = observatory_home("postgres")
BUILD_PATH = observatory_home("build", "local")
HOST_UID = os.getuid()
HOST_GID = os.getgid()
REDIS_PORT = 6379
FLOWER_UI_PORT = 5555
AIRFLOW_UI_PORT = 8080
ELASTIC_PORT = 9200
KIBANA_PORT = 5601
DOCKER_NETWORK_NAME = None
DOCKER_COMPOSE_PROJECT_NAME = "observatory"
DEBUG = False


class PlatformBuilder(ComposeRunner):
    def __init__(
        self,
        config_path: str,
        build_path: str = BUILD_PATH,
        dags_path: str = DAGS_MODULE,
        data_path: str = DATA_PATH,
        logs_path: str = LOGS_PATH,
        postgres_path: str = POSTGRES_PATH,
        host_uid: int = HOST_UID,
        host_gid: int = HOST_GID,
        redis_port: int = REDIS_PORT,
        flower_ui_port: int = FLOWER_UI_PORT,
        airflow_ui_port: int = AIRFLOW_UI_PORT,
        elastic_port: int = ELASTIC_PORT,
        kibana_port: int = KIBANA_PORT,
        docker_network_name: Union[None, int] = DOCKER_NETWORK_NAME,
        docker_compose_project_name: str = DOCKER_COMPOSE_PROJECT_NAME,
        debug: bool = DEBUG,
        backend_type: BackendType = BackendType.local,
    ):
        """ Create a PlatformBuilder instance, which is used to build, start and stop an Observatory Platform instance.

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
        :param docker_compose_project_name: The namespace for the Docker Compose containers: https://docs.docker.com/compose/reference/envvars/#compose_project_name.
        :param debug: Print debugging information.
        :param backend_type: whether we are running the local or terraform environment.
        """

        self.config_path = config_path
        self.dags_path = dags_path
        self.data_path = data_path
        self.logs_path = logs_path
        self.postgres_path = postgres_path
        self.host_uid = host_uid
        self.host_gid = host_gid
        self.platform_package_path = module_file_path("observatory.platform", nav_back_steps=-3)
        self.api_package_path = module_file_path("observatory.api", nav_back_steps=-3)
        self.docker_build_path = os.path.join(build_path, "docker")
        self.redis_port = redis_port
        self.flower_ui_port = flower_ui_port
        self.airflow_ui_port = airflow_ui_port
        self.elastic_port = elastic_port
        self.kibana_port = kibana_port
        self.docker_compose_project_name = docker_compose_project_name
        self.backend_type = backend_type

        # Set config class based on type of backend
        self.config_class = ObservatoryConfig
        if backend_type == BackendType.terraform:
            self.config_class = TerraformConfig

        # Set Docker Network name
        self.docker_network_name = "observatory-network"
        self.docker_network_is_external = docker_network_name is not None
        if self.docker_network_is_external:
            self.docker_network_name = docker_network_name

        # Load config
        self.config_exists = os.path.exists(config_path)
        self.config_is_valid = False
        self.config = None
        if self.config_exists:
            self.config: Union[ObservatoryConfig, TerraformConfig] = self.config_class.load(config_path)
            self.config_is_valid = self.config.is_valid

        path = self.docker_module_path
        super().__init__(
            compose_template_path=os.path.join(path, "docker-compose.observatory.yml.jinja2"),
            build_path=self.docker_build_path,
            compose_template_kwargs={
                "config": self.config,
                "docker_network_is_external": self.docker_network_is_external,
                "docker_network_name": self.docker_network_name,
                "dags_projects_to_str": DagsProject.dags_projects_to_str,
            },
            debug=debug,
        )

        # Add files
        self.add_template(path=os.path.join(path, "Dockerfile.observatory.jinja2"), config=self.config)
        self.add_template(path=os.path.join(path, "entrypoint-airflow.sh.jinja2"), config=self.config)
        self.add_file(path=os.path.join(path, "entrypoint-root.sh"), output_file_name="entrypoint-root.sh")
        self.add_file(path=os.path.join(path, "elasticsearch.yml"), output_file_name="elasticsearch.yml")
        self.add_file(
            path=os.path.join(self.platform_package_path, "requirements.txt"),
            output_file_name="requirements.observatory-platform.txt",
        )
        self.add_file(
            path=os.path.join(self.api_package_path, "requirements.txt"),
            output_file_name="requirements.observatory-api.txt",
        )

        # Add all project requirements files for local projects
        if self.config is not None:
            for project in self.config.dags_projects:
                if project.type == "local":
                    self.add_file(
                        path=os.path.join(project.path, "requirements.txt"),
                        output_file_name=f"requirements.{project.package_name}.txt",
                    )

    @property
    def is_environment_valid(self) -> bool:
        """ Return whether the environment for building the Observatory Platform is valid.

        :return: whether the environment for building the Observatory Platform is valid.
        """

        return all(
            [
                self.docker_exe_path is not None,
                self.docker_compose_path is not None,
                self.is_docker_running,
                self.config_exists,
                self.config_is_valid,
                self.config is not None,
            ]
        )

    @property
    def docker_module_path(self) -> str:
        """ The path to the Observatory Platform docker module.

        :return: the path.
        """

        return module_file_path("observatory.platform.docker")

    @property
    def docker_exe_path(self) -> str:
        """ The path to the Docker executable.

        :return: the path or None.
        """

        return shutil.which("docker")

    @property
    def docker_compose_path(self) -> str:
        """ The path to the Docker Compose executable.

        :return: the path or None.
        """

        return shutil.which("docker-compose")

    @property
    def is_docker_running(self) -> bool:
        """ Checks whether Docker is running or not.

        :return: whether Docker is running or not.
        """

        client = docker.from_env()
        try:
            is_running = client.ping()
        except requests.exceptions.ConnectionError:
            is_running = False
        return is_running

    def make_environment(self):
        """ Make an environment containing the environment variables that are required to build and start the
        Observatory docker environment.

        :return: None.
        """

        env = os.environ.copy()

        # Sets the name
        env["COMPOSE_PROJECT_NAME"] = self.docker_compose_project_name

        # Host settings
        env["HOST_USER_ID"] = str(self.host_uid)
        env["HOST_GROUP_ID"] = str(self.host_gid)
        env["HOST_LOGS_PATH"] = self.logs_path
        env["HOST_DAGS_PATH"] = self.dags_path
        env["HOST_DATA_PATH"] = self.data_path
        env["HOST_POSTGRES_PATH"] = self.postgres_path
        env["HOST_PLATFORM_PACKAGE_PATH"] = self.platform_package_path
        env["HOST_API_PACKAGE_PATH"] = self.api_package_path
        env["HOST_REDIS_PORT"] = str(self.redis_port)
        env["HOST_FLOWER_UI_PORT"] = str(self.flower_ui_port)
        env["HOST_AIRFLOW_UI_PORT"] = str(self.airflow_ui_port)
        env["HOST_ELASTIC_PORT"] = str(self.elastic_port)
        env["HOST_KIBANA_PORT"] = str(self.kibana_port)

        # Secrets
        if self.config.google_cloud.credentials is not None:
            env["HOST_GOOGLE_APPLICATION_CREDENTIALS"] = self.config.google_cloud.credentials
        env["FERNET_KEY"] = self.config.airflow.fernet_key
        env["SECRET_KEY"] = self.config.airflow.secret_key

        # Create Airflow variables
        airflow_variables = self.config.make_airflow_variables()
        for variable in airflow_variables:
            env[variable.env_var_name] = str(variable.value)

        # Airflow connections
        for conn in self.config.airflow_connections:
            env[conn.conn_name] = conn.value

        return env
