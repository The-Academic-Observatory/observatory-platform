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

import logging
import os
import pathlib
import unittest
from typing import Any, Dict
from unittest.mock import Mock, patch
from observatory.platform.observatory_config import save_yaml
import requests
from click.testing import CliRunner
from redis import Redis

import observatory.platform.docker as docker_module
from observatory.platform.cli.cli import HOST_UID, HOST_GID
from observatory.platform.observatory_config import (
    ObservatoryConfig,
    Observatory,
    CloudStorageBucket,
    Terraform,
    Backend,
    Environment,
    BackendType,
    GoogleCloud,
    AirflowVariable,
    AirflowConnection,
    DagsProject,
)
from observatory.platform.platform_builder import PlatformBuilder
from observatory.platform.utils.config_utils import module_file_path
from observatory.platform.utils.url_utils import wait_for_url


class MockFromEnv(Mock):
    def __init__(self, is_running: bool, **kwargs: Any):
        super().__init__(**kwargs)
        self.is_running = is_running

    def ping(self):
        if self.is_running:
            return True
        raise requests.exceptions.ConnectionError()


def make_expected_env(cmd: PlatformBuilder) -> Dict:
    """ Make an expected environment.

    :param cmd: the PlatformBuilder.
    :return: the environment.
    """

    return {
        "COMPOSE_PROJECT_NAME": cmd.config.observatory.docker_compose_project_name,
        "HOST_USER_ID": str(HOST_UID),
        "HOST_GROUP_ID": str(HOST_GID),
        "HOST_OBSERVATORY_HOME": cmd.config.observatory.observatory_home,
        "HOST_DAGS_PATH": module_file_path("observatory.platform.dags", nav_back_steps=-1),
        "HOST_PLATFORM_PACKAGE_PATH": module_file_path("observatory.platform", nav_back_steps=-3),
        "HOST_API_PACKAGE_PATH": module_file_path("observatory.api", nav_back_steps=-3),
        "HOST_REDIS_PORT": str(cmd.config.observatory.redis_port),
        "HOST_FLOWER_UI_PORT": str(cmd.config.observatory.flower_ui_port),
        "HOST_AIRFLOW_UI_PORT": str(cmd.config.observatory.airflow_ui_port),
        "HOST_ELASTIC_PORT": str(cmd.config.observatory.elastic_port),
        "HOST_KIBANA_PORT": str(cmd.config.observatory.kibana_port),
        "AIRFLOW_FERNET_KEY": cmd.config.observatory.airflow_fernet_key,
        "AIRFLOW_SECRET_KEY": cmd.config.observatory.airflow_secret_key,
        "AIRFLOW_UI_USER_EMAIL": cmd.config.observatory.airflow_ui_user_email,
        "AIRFLOW_UI_USER_PASSWORD": cmd.config.observatory.airflow_ui_user_password,
        "POSTGRES_PASSWORD": cmd.config.observatory.postgres_password,
        "AIRFLOW_VAR_ENVIRONMENT": "develop",
    }


class TestPlatformBuilder(unittest.TestCase):
    def setUp(self) -> None:
        self.is_env_local = True

    def save_config(self, file_path: str, observatory_home: str):
        dict_ = {
            "backend": {"type": "local", "environment": "develop"},
            "observatory": {
                "observatory_home": observatory_home,
                "airflow_fernet_key": "random-fernet-key",
                "airflow_secret_key": "random-secret-key",
            }
        }

        save_yaml(file_path, dict_)

    def test_is_environment_valid(self):
        with CliRunner().isolated_filesystem() as t:
            config_path = os.path.join(t, "config.yaml")

            # Raise FileExistsError because is no config.yaml
            with self.assertRaises(FileExistsError):
                PlatformBuilder(config_path=config_path)

            # Environment should be valid because there is a config.yaml
            # Assumes that Docker is setup on the system where the tests are run
            self.save_config(config_path, t)
            cmd = PlatformBuilder(config_path=config_path)
            self.assertTrue(cmd.is_environment_valid)

    def test_docker_module_path(self):
        """ Test that the path to the Docker module  is found """

        with CliRunner().isolated_filesystem() as t:
            config_path = os.path.join(t, "config.yaml")
            self.save_config(config_path, t)
            cmd = PlatformBuilder(config_path=config_path)
            expected_path = str(pathlib.Path(*pathlib.Path(docker_module.__file__).resolve().parts[:-1]).resolve())
            self.assertEqual(expected_path, cmd.docker_module_path)

    def test_docker_exe_path(self):
        """ Test that the path to the Docker executable is found """

        with CliRunner().isolated_filesystem() as t:
            config_path = os.path.join(t, "config.yaml")
            self.save_config(config_path, t)
            cmd = PlatformBuilder(config_path=config_path)
            result = cmd.docker_exe_path
            self.assertIsNotNone(result)
            self.assertTrue(result.endswith("docker"))

    def test_docker_compose_path(self):
        """ Test that the path to the Docker Compose executable is found """

        with CliRunner().isolated_filesystem() as t:
            config_path = os.path.join(t, "config.yaml")
            self.save_config(config_path, t)
            cmd = PlatformBuilder(config_path=config_path)
            result = cmd.docker_compose_path
            self.assertIsNotNone(result)
            self.assertTrue(result.endswith("docker-compose"))

    @patch("observatory.platform.platform_builder.docker.from_env")
    def test_is_docker_running_true(self, mock_from_env):
        """ Test the property is_docker_running returns True when Docker is running  """

        mock_from_env.return_value = MockFromEnv(True)

        with CliRunner().isolated_filesystem() as t:
            config_path = os.path.join(t, "config.yaml")
            self.save_config(config_path, t)
            cmd = PlatformBuilder(config_path=config_path)
            self.assertTrue(cmd.is_docker_running)

    @patch("observatory.platform.platform_builder.docker.from_env")
    def test_is_docker_running_false(self, mock_from_env):
        """ Test the property is_docker_running returns False when Docker is not running  """

        mock_from_env.return_value = MockFromEnv(False)

        with CliRunner().isolated_filesystem() as t:
            config_path = os.path.join(t, "config.yaml")
            self.save_config(config_path, t)
            cmd = PlatformBuilder(config_path=config_path)
            self.assertFalse(cmd.is_docker_running)

    def test_make_observatory_files(self):
        """ Test building of the observatory files """

        with CliRunner().isolated_filesystem() as t:
            config_path = os.path.join(t, "config.yaml")
            self.save_config(config_path, t)
            cmd = PlatformBuilder(config_path=config_path)
            cmd.build()

            # Test that the expected files have been written
            build_file_names = [
                "docker-compose.observatory.yml",
                "Dockerfile.observatory",
                "elasticsearch.yml",
                "entrypoint-airflow.sh",
                "entrypoint-root.sh",
                "requirements.observatory-platform.txt",
                "requirements.observatory-api.txt",
            ]
            for file_name in build_file_names:
                path = os.path.join(cmd.build_path, file_name)
                self.assertTrue(os.path.isfile(path))
                self.assertTrue(os.stat(path).st_size > 0)

    def test_make_environment_minimal(self):
        """ Test making of the minimal observatory platform files """

        # Check that the environment variables are set properly for the default config
        with CliRunner().isolated_filesystem() as t:
            config_path = os.path.join(t, "config.yaml")
            self.save_config(config_path, t)
            cmd = PlatformBuilder(config_path=config_path)

            # Make the environment
            expected_env = make_expected_env(cmd)
            env = cmd.make_environment()

            # Check that expected keys and values exist
            for key, value in expected_env.items():
                self.assertTrue(key in env)
                self.assertEqual(value, env[key])

            # Check that Google Application credentials not in default config
            self.assertTrue("HOST_GOOGLE_APPLICATION_CREDENTIALS" not in env)

    def test_make_environment_all_settings(self):
        """ Test making of the observatory platform files with all settings """

        # Check that the environment variables are set properly for a complete config file
        with CliRunner().isolated_filesystem() as t:
            config_path = os.path.join(t, "config.yaml")
            self.save_config(config_path, t)
            cmd = PlatformBuilder(config_path=config_path)
            expected_env = make_expected_env(cmd)

            # Manually override the platform command with a more fleshed out config file
            bucket = CloudStorageBucket(id="download_bucket", name="my-download-bucket-name")
            var = AirflowVariable(name="my-var", value="my-variable-value")
            conn = AirflowConnection(name="my-conn", value="http://my-username:my-password@")
            dags_project = DagsProject(
                package_name="observatory-dags",
                path="/path/to/observatory-platform/observatory-dags/observatory/dags",
                dags_module="observatory.dags.dags",
            )
            backend = Backend(type=BackendType.local, environment=Environment.develop)
            observatory = Observatory(
                observatory_home=t,
                airflow_fernet_key="DOJLLgvRnhy51gxLbxznn4w7MxD5kZ53bOZEoPr8wCg=",
                airflow_secret_key="f95d60dc61b3a2b703ece8904b93947af88c2f609df8855514af097ea254",
            )
            google_cloud = GoogleCloud(
                project_id="my-project-id", credentials="/path/to/creds.json", data_location="us", buckets=[bucket]
            )
            terraform = Terraform(organization="my-terraform-org-name")
            config = ObservatoryConfig(
                backend=backend,
                observatory=observatory,
                google_cloud=google_cloud,
                terraform=terraform,
                airflow_variables=[var],
                airflow_connections=[conn],
                dags_projects=[dags_project],
            )
            cmd.config = config
            cmd.config_exists = True

            # Make environment and test
            env = cmd.make_environment()

            # Set FERNET_KEY, HOST_GOOGLE_APPLICATION_CREDENTIALS, AIRFLOW_VAR_DAGS_MODULE_NAMES
            # and airflow variables and connections
            expected_env["AIRFLOW_FERNET_KEY"] = cmd.config.observatory.airflow_fernet_key
            expected_env["AIRFLOW_SECRET_KEY"] = cmd.config.observatory.airflow_secret_key
            expected_env["HOST_GOOGLE_APPLICATION_CREDENTIALS"] = cmd.config.google_cloud.credentials
            expected_env["AIRFLOW_VAR_ENVIRONMENT"] = cmd.config.backend.environment.value
            expected_env["AIRFLOW_VAR_PROJECT_ID"] = google_cloud.project_id
            expected_env["AIRFLOW_VAR_DATA_LOCATION"] = google_cloud.data_location
            expected_env["AIRFLOW_VAR_TERRAFORM_ORGANIZATION"] = terraform.organization
            expected_env["AIRFLOW_VAR_DOWNLOAD_BUCKET"] = bucket.name

            # TODO: it seems a little inconsistent to name these vars like this
            expected_env["AIRFLOW_VAR_MY-VAR"] = var.value
            expected_env["AIRFLOW_CONN_MY-CONN"] = conn.value

            # Check that expected keys and values exist
            for key, value in expected_env.items():
                self.assertTrue(key in env)
                self.assertEqual(value, env[key])

    def test_build(self):
        """ Test building of the observatory platform """

        # Check that the environment variables are set properly for the default config
        with CliRunner().isolated_filesystem() as t:
            config_path = os.path.join(t, "config.yaml")
            self.save_config(config_path, t)
            cmd = PlatformBuilder(config_path=config_path)
            cmd.debug = True

            # Build the platform
            response = cmd.build()

            # Assert that the platform builds
            expected_return_code = 0
            self.assertEqual(expected_return_code, response.return_code)

    @unittest.skip
    def test_build_start_stop(self):
        """ Test buildng, starting and stopping of the Observatory Platform """

        # Check that the environment variables are set properly for the default config
        with CliRunner().isolated_filesystem() as t:
            config_path = os.path.join(t, "config.yaml")
            self.save_config(config_path, t)
            cmd = PlatformBuilder(config_path=config_path)

            cmd.config.observatory.airflow_ui_port = 8082
            cmd.config.observatory.flower_ui_port = 5557
            cmd.config.observatory.elastic_port = 9202
            cmd.config.observatory.kibana_port = 5603
            cmd.config.observatory.redis_port = 6381
            cmd.debug = True

            # Expected values
            expected_return_code = 0
            expected_ports = [
                cmd.config.observatory.airflow_ui_port,
                cmd.config.observatory.flower_ui_port,
                cmd.config.observatory.elastic_port,
                cmd.config.observatory.kibana_port,
            ]

            # Build the platform and assert that the platform builds
            response = cmd.build()
            logging.info(response.output)
            logging.error(response.error)
            self.assertEqual(expected_return_code, response.return_code)

            try:
                # Start the platform
                response = cmd.start()
                logging.info(response.output)
                logging.error(response.error)
                self.assertEqual(expected_return_code, response.return_code)

                # Verify that ports are active
                urls = []
                states = []
                for port in expected_ports:
                    url = f"http://localhost:{port}"
                    urls.append(url)
                    logging.info(f"Waiting for URL: {url}")
                    state = wait_for_url(url, timeout=120)
                    logging.info(f"URL {url} state: {state}")
                    states.append(state)

                # Assert states
                for state in states:
                    self.assertTrue(state)

                # Check if Redis is active
                redis = Redis(port=cmd.config.observatory.redis_port, socket_connect_timeout=1)
                self.assertTrue(redis.ping())

                # Verify that platform stops
                response = cmd.stop()
                logging.info(response.output)
                logging.error(response.error)
                self.assertEqual(expected_return_code, response.return_code)
            finally:
                # Always the observatory, e.g. in case the start command got half way through
                cmd.stop()
