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
import subprocess
from subprocess import Popen
from typing import List, Tuple, Union

import docker
import requests

from observatory.platform.utils.config_utils import observatory_home, module_file_path
from observatory.platform.observatory_config import ObservatoryConfig
from observatory.platform.utils.jinja2_utils import render_template
from observatory.platform.utils.proc_utils import stream_process


class PlatformBuilder:
    COMPOSE_ARGS = ['docker-compose', '-f', 'docker-compose.observatory.yml']
    COMPOSE_BUILD_ARGS = ['build']
    COMPOSE_START_ARGS = ['up', '-d']
    COMPOSE_STOP_ARGS = ['down']

    def __init__(self, config_path: str, dags_path: str, data_path: str, logs_path: str, postgres_path: str,
                 host_uid: int, host_gid: int, redis_port: int, flower_ui_port: int, airflow_ui_port: int,
                 elastic_port: int, kibana_port: int, docker_network_name: Union[None, str], debug: bool,
                 is_env_local: bool):
        """

        :param config_path:
        :param dags_path: the path to the DAGs folder on the host system.
        :param data_path: the path to the data path on the host system.
        :param logs_path: the path to the logs path on the host system.
        :param postgres_path: the path to the Apache Airflow Postgres data folder on the host system.
        :param host_uid: the user id of the host system.
        :param host_gid: the group id of the host system.
        :param redis_port:
        :param flower_ui_port:
        :param airflow_ui_port:
        :param elastic_port:
        :param kibana_port:
        :param docker_network_name:
        :param debug:
        :param is_env_local:
        """

        self.config_path = config_path
        self.dags_path = dags_path
        self.data_path = data_path
        self.logs_path = logs_path
        self.postgres_path = postgres_path
        self.host_uid = host_uid
        self.host_gid = host_gid
        self.debug = debug
        self.package_path = module_file_path('observatory.platform', nav_back_steps=-3)
        self.working_dir = observatory_home('build')
        self.redis_port = redis_port
        self.flower_ui_port = flower_ui_port
        self.airflow_ui_port = airflow_ui_port
        self.elastic_port = elastic_port
        self.kibana_port = kibana_port
        self.is_env_local = is_env_local

        # Set Docker Network name
        self.docker_network_name = 'observatory-network'
        self.docker_network_is_external = docker_network_name is not None
        if self.docker_network_is_external:
            self.docker_network_name = docker_network_name

        # Load config
        self.config_exists = os.path.exists(config_path)
        self.config_is_valid = False
        self.config = None
        if self.config_exists:
            self.config: ObservatoryConfig = ObservatoryConfig.load(config_path)
            self.config_is_valid = self.config.is_valid

    @property
    def is_environment_valid(self):
        return all([self.docker_exe_path is not None,
                    self.docker_compose_path is not None,
                    self.is_docker_running,
                    self.config_exists,
                    self.config_is_valid,
                    self.config is not None])

    @property
    def docker_module_path(self) -> str:
        """ The path to the Observatory Platform docker module.

        :return: the path.
        """

        return module_file_path('observatory.platform.docker')

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

    def make_observatory_files(self):
        # Build Docker files
        self.__make_file('Dockerfile.observatory.jinja2', 'Dockerfile.observatory', self.working_dir,
                         config=self.config,
                         is_env_local=self.is_env_local)
        self.__make_file('docker-compose.observatory.yml.jinja2', 'docker-compose.observatory.yml', self.working_dir,
                         config=self.config,
                         docker_network_is_external=self.docker_network_is_external,
                         docker_network_name=self.docker_network_name,
                         is_env_local=self.is_env_local)
        self.__make_file('entrypoint-airflow.sh.jinja2', 'entrypoint-airflow.sh', self.working_dir,
                         config=self.config,
                         is_env_local=self.is_env_local)

        # Build requirements files
        self.__make_requirements_files()

        # Copy other files
        file_names = ['entrypoint-root.sh', 'elasticsearch.yml']
        for file_name in file_names:
            input_file = os.path.join(self.docker_module_path, file_name)
            output_file = os.path.join(self.working_dir, file_name)
            shutil.copy(input_file, output_file)

    def __make_file(self, template_file_name: str, output_file_name: str, working_dir: str, **kwargs):
        template_path = os.path.join(self.docker_module_path, template_file_name)
        render = render_template(template_path, **kwargs)

        output_path = os.path.join(working_dir, output_file_name)
        with open(output_path, 'w') as f:
            f.write(render)

    def __make_requirements_files(self):
        # Copy observatory requirements.txt
        input_file = os.path.join(self.package_path, 'requirements.txt')
        output_file = os.path.join(self.working_dir, 'requirements.txt')
        shutil.copy(input_file, output_file)

        # Copy all project requirements files for local projects
        for project in self.config.dags_projects:
            if project.type == 'local':
                input_file = os.path.join(project.path, 'requirements.txt')
                output_file = os.path.join(self.working_dir, f'requirements.{project.package_name}.txt')
                shutil.copy(input_file, output_file)

    def make_environment(self):
        """ Make an environment containing the environment variables that are required to build and start the
        Observatory docker environment.
        """

        env = os.environ.copy()

        # Host settings
        env['HOST_USER_ID'] = str(self.host_uid)
        env['HOST_GROUP_ID'] = str(self.host_gid)
        env['HOST_LOGS_PATH'] = self.logs_path
        env['HOST_DAGS_PATH'] = self.dags_path
        env['HOST_DATA_PATH'] = self.data_path
        env['HOST_POSTGRES_PATH'] = self.postgres_path
        env['HOST_PACKAGE_PATH'] = self.package_path
        env['HOST_REDIS_PORT'] = str(self.redis_port)
        env['HOST_FLOWER_UI_PORT'] = str(self.flower_ui_port)
        env['HOST_AIRFLOW_UI_PORT'] = str(self.airflow_ui_port)
        env['HOST_ELASTIC_PORT'] = str(self.elastic_port)
        env['HOST_KIBANA_PORT'] = str(self.kibana_port)

        # Secrets
        if self.config.google_cloud.credentials is not None:
            env['HOST_GOOGLE_APPLICATION_CREDENTIALS'] = self.config.google_cloud.credentials
        env['FERNET_KEY'] = self.config.airflow.fernet_key

        # Create Airflow variables
        airflow_variables = self.config.make_airflow_variables()
        for variable in airflow_variables:
            env[variable.env_var_name] = str(variable.value)

        # Airflow connections
        for conn in self.config.airflow_connections:
            env[conn.conn_name] = conn.value

        return env

    def build(self) -> Tuple[str, str, int]:
        """ Build the Observatory Platform.

        :return: output and error stream results and proc return code.
        """

        # Make observatory Docker files
        self.make_observatory_files()

        # Build observatory Docker containers
        return self.__run_docker_compose_cmd(PlatformBuilder.COMPOSE_BUILD_ARGS)

    def start(self) -> Tuple[str, str, int]:
        """ Start the Observatory Platform.

        :return: output and error stream results and proc return code.
        """

        return self.__run_docker_compose_cmd(PlatformBuilder.COMPOSE_START_ARGS)

    def stop(self) -> Tuple[str, str, int]:
        """ Stop the Observatory Platform.

        :return: output and error stream results and proc return code.
        """

        return self.__run_docker_compose_cmd(PlatformBuilder.COMPOSE_STOP_ARGS)

    def __run_docker_compose_cmd(self, args: List) -> Tuple[str, str, int]:
        """ Run a set of Docker Compose arguments.

        :param args: the list of arguments.
        :return: output and error stream results and proc return code.
        """

        # Make the environment for running the Docker Compose process
        env = self.make_environment()

        # Build the containers first
        proc: Popen = subprocess.Popen(PlatformBuilder.COMPOSE_ARGS + args,
                                       stdout=subprocess.PIPE,
                                       stderr=subprocess.PIPE,
                                       env=env,
                                       cwd=self.working_dir)

        # Wait for results
        output, error = stream_process(proc, self.debug)

        return output, error, proc.returncode
