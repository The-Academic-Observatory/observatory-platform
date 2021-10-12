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

import glob
import json
import logging
import os
import shutil
import subprocess
import time
import unittest
import uuid
from subprocess import Popen
from typing import Set
from unittest.mock import patch

import requests
import stringcase
from click.testing import CliRunner
from cryptography.fernet import Fernet
from redis import Redis
from observatory.platform.utils.test_utils import test_fixtures_path, find_free_port, save_empty_file, build_sdist
from observatory.platform.cli.cli import cli
from observatory.platform.observatory_config import (
    ObservatoryConfig,
    Backend,
    Observatory,
    BackendType,
    Environment,
    module_file_path,
    WorkflowsProject,
)
from observatory.platform.utils.proc_utils import stream_process
from observatory.platform.utils.test_utils import test_fixtures_path, find_free_port, save_empty_file
from observatory.platform.utils.url_utils import wait_for_url


def list_dag_ids(
    host: str = "http://localhost", port: int = None, user: str = "airflow@airflow.com", pwd: str = "airflow"
) -> Set:
    """List the DAG ids that have been loaded in an Airflow instance.

    :param host: the hostname.
    :param port: the port.
    :param user: the username.
    :param pwd: the password.
    :return: the set of DAG ids.
    """

    parts = [host]
    if port is not None:
        parts.append(f":{port}")
    parts.append("/api/v1/dags")
    url = "".join(parts)

    dag_ids = []
    response = requests.get(url, headers={"Content-Type": "application/json"}, auth=(user, pwd))
    if response.status_code == 200:
        dags = json.loads(response.text)["dags"]
        dag_ids = [dag["dag_id"] for dag in dags]

    return set(dag_ids)





class TestCliFunctional(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.observatory_api_path = module_file_path("observatory.api", nav_back_steps=-3)
        self.observatory_api_package_name = "observatory-api"
        self.observatory_platform_path = module_file_path("observatory.platform", nav_back_steps=-3)
        self.observatory_platform_package_name = "observatory-platform"
        self.airflow_ui_user_email = "airflow@airflow.com"
        self.airflow_ui_user_password = "airflow"
        self.airflow_fernet_key = Fernet.generate_key()
        self.airflow_secret_key = uuid.uuid4().hex
        self.docker_network_name = "observatory-unit-test-network"
        self.docker_compose_project_name = "observatory_unit_test"
        self.expected_platform_dag_ids = {"dummy_telescope", "vm_create", "vm_destroy"}
        self.expected_workflows_dag_ids = {"dummy_telescope", "vm_create", "vm_destroy", "my_dag", "hello_world_dag"}
        self.start_cmd = ["platform", "start", "--debug", "--config-path"]
        self.stop_cmd = ["platform", "stop", "--debug", "--config-path"]
        self.workflows_package_name = "my-workflows-project"
        self.dag_check_timeout = 180
        self.port_wait_timeout = 180
        self.config_file_name = "config.yaml"

    def make_editable_observatory_config(self, temp_dir: str) -> ObservatoryConfig:
        """Make an editable observatory config.

        :param temp_dir: the temp dir.
        :return: ObservatoryConfig.
        """

        return ObservatoryConfig(
            backend=Backend(type=BackendType.local, environment=Environment.develop),
            observatory=Observatory(
                package=os.path.join(temp_dir, self.observatory_platform_package_name),
                package_type="editable",
                airflow_fernet_key=self.airflow_fernet_key,
                airflow_secret_key=self.airflow_secret_key,
                airflow_ui_user_email=self.airflow_ui_user_email,
                airflow_ui_user_password=self.airflow_ui_user_password,
                observatory_home=temp_dir,
                redis_port=find_free_port(),
                flower_ui_port=find_free_port(),
                airflow_ui_port=find_free_port(),
                elastic_port=find_free_port(),
                kibana_port=find_free_port(),
                docker_network_name=self.docker_network_name,
                docker_compose_project_name=self.docker_compose_project_name,
                enable_elk=False,
                api_package_type="editable",
                api_package=os.path.join(temp_dir, self.observatory_api_package_name),
            ),
        )

    def copy_observatory_api(self, temp_dir: str):
        """Copy the workflows project to the test dir"""

        shutil.copytree(self.observatory_api_path, os.path.join(temp_dir, self.observatory_api_package_name))

    def copy_observatory_platform(self, temp_dir: str):
        """Copy the workflows project to the test dir"""

        shutil.copytree(self.observatory_platform_path, os.path.join(temp_dir, self.observatory_platform_package_name))

    def copy_workflows_project(self, temp_dir: str):
        """Copy the workflows project to the test dir"""

        shutil.copytree(
            test_fixtures_path("cli", self.workflows_package_name), os.path.join(temp_dir, self.workflows_package_name)
        )

    def assert_dags_loaded(self, expected_dag_ids: Set, config: ObservatoryConfig, dag_check_timeout: int = 30):
        """Assert that DAGs loaded into Airflow.

        :param expected_dag_ids: the expected DAG ids.
        :param config: the Observatory Config.
        :param dag_check_timeout: how long to check for DAGs.
        :return: None.
        """

        start = time.time()
        while True:
            duration = time.time() - start
            actual_dag_ids = list_dag_ids(
                port=config.observatory.airflow_ui_port,
                user=config.observatory.airflow_ui_user_email,
                pwd=config.observatory.airflow_ui_user_password,
            )
            if expected_dag_ids == actual_dag_ids or duration > dag_check_timeout:
                break
        self.assertSetEqual(expected_dag_ids, actual_dag_ids)

    def assert_ports_open(self, observatory: Observatory, timeout: int = 120):
        """Check that the ports given in the observatory object are accepting connections.

        :param observatory: the observatory object.
        :param timeout:  the length of time to wait until timing out.
        :return: None.
        """

        # Expected values
        expected_ports = [observatory.airflow_ui_port, observatory.flower_ui_port]
        if observatory.enable_elk:
            expected_ports += [
                observatory.elastic_port,
                observatory.kibana_port,
            ]

        # Verify that ports are active
        urls = []
        states = []
        for port in expected_ports:
            url = f"http://localhost:{port}"
            urls.append(url)
            logging.info(f"Waiting for URL: {url}")
            state = wait_for_url(url, timeout=timeout)
            logging.info(f"URL {url} state: {state}")
            states.append(state)

        # Assert states
        for state in states:
            self.assertTrue(state)

        # Check if Redis is active
        redis = Redis(port=observatory.redis_port, socket_connect_timeout=1)
        self.assertTrue(redis.ping())

    @patch("observatory.platform.platform_builder.ObservatoryConfig.load")
    def test_run_platform_editable(self, mock_config_load):
        """Test that the platform runs when built from an editable project. API installed from PyPI."""

        runner = CliRunner()
        with runner.isolated_filesystem() as t:
            # Save empty config
            config_path = save_empty_file(t, self.config_file_name)

            # Copy platform project
            self.copy_observatory_api(t)
            self.copy_observatory_platform(t)

            # Make config object
            config = self.make_editable_observatory_config(t)
            mock_config_load.return_value = config

            try:
                # Test that start command works
                result = runner.invoke(cli, self.start_cmd + [config_path], catch_exceptions=False)
                self.assertEqual(os.EX_OK, result.exit_code)

                # Assert that ports are open
                self.assert_ports_open(config.observatory, timeout=self.port_wait_timeout)

                # Test that default DAGs are loaded
                self.assert_dags_loaded(
                    self.expected_platform_dag_ids, config, dag_check_timeout=self.dag_check_timeout
                )

                # Test that stop command works
                result = runner.invoke(cli, self.stop_cmd + [config_path], catch_exceptions=False)
                self.assertEqual(os.EX_OK, result.exit_code)
            finally:
                runner.invoke(cli, self.stop_cmd + [config_path])

    @patch("observatory.platform.platform_builder.ObservatoryConfig.load")
    def test_dag_load_workflows_project_editable(self, mock_config_load):
        """Test that the DAGs load when build from an editable workflows project. API installed from PyPI."""

        runner = CliRunner()
        with runner.isolated_filesystem() as t:
            # Save empty config
            config_path = save_empty_file(t, self.config_file_name)

            # Copy projects
            self.copy_observatory_api(t)
            self.copy_observatory_platform(t)
            self.copy_workflows_project(t)

            # Make config object
            config = self.make_editable_observatory_config(t)
            config.workflows_projects = [
                WorkflowsProject(
                    package_name=self.workflows_package_name,
                    package=os.path.join(t, self.workflows_package_name),
                    package_type="editable",
                    dags_module=f"{stringcase.snakecase(self.workflows_package_name)}.dags",
                )
            ]
            mock_config_load.return_value = config

            try:
                # Test that start command works
                result = runner.invoke(cli, self.start_cmd + [config_path], catch_exceptions=False)
                print("test_dag_load_workflows_project_editable errors")
                print(f"Output: {result.output}")
                self.assertEqual(os.EX_OK, result.exit_code)

                # Assert that ports are open
                self.assert_ports_open(config.observatory, timeout=self.port_wait_timeout)

                # Test that default DAGs are loaded
                self.assert_dags_loaded(
                    self.expected_workflows_dag_ids, config, dag_check_timeout=self.dag_check_timeout
                )

                # Test that stop command works
                result = runner.invoke(cli, self.stop_cmd + [config_path], catch_exceptions=False)
                self.assertEqual(os.EX_OK, result.exit_code)
            finally:
                runner.invoke(cli, self.stop_cmd + [config_path])

    def make_sdist_observatory_config(
        self,
        temp_dir: str,
        observatory_api_sdist_path: str,
        observatory_sdist_path: str,
    ) -> ObservatoryConfig:
        """Make an sdist observatory config.

        :param temp_dir: the temp dir.
        :param observatory_api_sdist_path: the observatory-api sdist path.
        :param observatory_sdist_path: the observatory-platform sdist path.
        :return: ObservatoryConfig.
        """

        return ObservatoryConfig(
            backend=Backend(type=BackendType.local, environment=Environment.develop),
            observatory=Observatory(
                package=observatory_sdist_path,
                package_type="sdist",
                airflow_fernet_key=self.airflow_fernet_key,
                airflow_secret_key=self.airflow_secret_key,
                airflow_ui_user_email=self.airflow_ui_user_email,
                airflow_ui_user_password=self.airflow_ui_user_password,
                observatory_home=temp_dir,
                redis_port=find_free_port(),
                flower_ui_port=find_free_port(),
                airflow_ui_port=find_free_port(),
                elastic_port=find_free_port(),
                kibana_port=find_free_port(),
                docker_network_name=self.docker_network_name,
                docker_compose_project_name=self.docker_compose_project_name,
                enable_elk=False,
                api_package=observatory_api_sdist_path,
                api_package_type="sdist",
            ),
        )

    @patch("observatory.platform.platform_builder.ObservatoryConfig.load")
    def test_run_platform_sdist(self, mock_config_load):
        """Test that the platform runs when built from a source distribution. API package installed from PyPI."""

        runner = CliRunner()
        with runner.isolated_filesystem() as t:
            # Save empty config
            config_path = save_empty_file(t, self.config_file_name)

            # Copy platform project
            self.copy_observatory_api(t)
            self.copy_observatory_platform(t)

            # Build sdist
            observatory_api_sdist_path = build_sdist(os.path.join(t, self.observatory_api_package_name))
            observatory_platform_sdist_path = build_sdist(os.path.join(t, self.observatory_platform_package_name))

            # Make config object
            config = self.make_sdist_observatory_config(t, observatory_api_sdist_path, observatory_platform_sdist_path)
            mock_config_load.return_value = config

            try:
                # Test that start command works
                result = runner.invoke(cli, self.start_cmd + [config_path], catch_exceptions=False)
                self.assertEqual(os.EX_OK, result.exit_code)

                # Assert that ports are open
                self.assert_ports_open(config.observatory, timeout=self.port_wait_timeout)

                # Test that default DAGs are loaded
                self.assert_dags_loaded(
                    self.expected_platform_dag_ids, config, dag_check_timeout=self.dag_check_timeout
                )

                # Test that stop command works
                result = runner.invoke(cli, self.stop_cmd + [config_path], catch_exceptions=False)
                self.assertEqual(os.EX_OK, result.exit_code)
            finally:
                runner.invoke(cli, self.stop_cmd + [config_path])

    @patch("observatory.platform.platform_builder.ObservatoryConfig.load")
    def test_dag_load_workflows_project_sdist(self, mock_config_load):
        """Test that DAGs load from an sdist workflows project. API package installed from PyPI."""

        runner = CliRunner()
        with runner.isolated_filesystem() as t:
            # Save empty config
            config_path = save_empty_file(t, self.config_file_name)

            # Copy projects
            self.copy_observatory_api(t)
            self.copy_observatory_platform(t)
            self.copy_workflows_project(t)

            # Build sdists
            observatory_api_sdist_path = build_sdist(os.path.join(t, self.observatory_api_package_name))
            observatory_sdist_path = build_sdist(os.path.join(t, self.observatory_platform_package_name))
            workflows_sdist_path = build_sdist(os.path.join(t, self.workflows_package_name))

            # Make config object
            config = self.make_sdist_observatory_config(t, observatory_api_sdist_path, observatory_sdist_path)
            config.workflows_projects = [
                WorkflowsProject(
                    package_name=self.workflows_package_name,
                    package=workflows_sdist_path,
                    package_type="sdist",
                    dags_module=f"{stringcase.snakecase(self.workflows_package_name)}.dags",
                )
            ]
            mock_config_load.return_value = config

            try:
                # Test that start command works
                result = runner.invoke(cli, self.start_cmd + [config_path], catch_exceptions=False)
                self.assertEqual(os.EX_OK, result.exit_code)

                # Assert that ports are open
                self.assert_ports_open(config.observatory, timeout=self.port_wait_timeout)

                # Test that default DAGs are loaded
                self.assert_dags_loaded(
                    self.expected_workflows_dag_ids, config, dag_check_timeout=self.dag_check_timeout
                )

                # Test that stop command works
                result = runner.invoke(cli, self.stop_cmd + [config_path], catch_exceptions=False)
                self.assertEqual(os.EX_OK, result.exit_code)
            finally:
                runner.invoke(cli, self.stop_cmd + [config_path])
