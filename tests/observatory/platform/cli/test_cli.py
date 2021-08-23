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

import json
import os
import unittest
from typing import Any, List
from unittest.mock import Mock, patch

from click.testing import CliRunner
from observatory.platform.cli.cli import cli
from observatory.platform.docker.compose import ProcessOutput
from observatory.platform.observatory_config import TerraformConfig, ValidationError
from observatory.platform.platform_builder import DEBUG, HOST_GID, HOST_UID
from observatory.platform.terraform_api import TerraformApi
from tests.observatory.test_utils import random_id


class TestObservatoryGenerate(unittest.TestCase):
    @patch("click.confirm")
    @patch("os.path.exists")
    def test_generate(self, mock_path_exists, mock_click_confirm):
        """Test that the fernet key and default config files are generated"""

        # Test generate fernet key
        runner = CliRunner()
        result = runner.invoke(cli, ["generate", "secrets", "fernet-key"])
        self.assertEqual(result.exit_code, os.EX_OK)

        result = runner.invoke(cli, ["generate", "secrets", "secret-key"])
        self.assertEqual(result.exit_code, os.EX_OK)

        # Test that files are generated
        with runner.isolated_filesystem():
            mock_click_confirm.return_value = True
            mock_path_exists.return_value = False

            # Test generate local config
            config_path = os.path.abspath("config.yaml")
            result = runner.invoke(cli, ["generate", "config", "local", "--config-path", config_path])
            self.assertEqual(result.exit_code, os.EX_OK)
            self.assertTrue(os.path.isfile(config_path))
            self.assertIn("Observatory Config saved to:", result.output)

            # Test generate terraform config
            config_path = os.path.abspath("config-terraform.yaml")
            result = runner.invoke(cli, ["generate", "config", "terraform", "--config-path", config_path])
            self.assertEqual(result.exit_code, os.EX_OK)
            self.assertTrue(os.path.isfile(config_path))
            self.assertIn("Terraform Config saved to:", result.output)

        # Test that files are not generated when confirm is set to n
        runner = CliRunner()
        with runner.isolated_filesystem():
            mock_click_confirm.return_value = False
            mock_path_exists.return_value = True

            # Test generate local config
            config_path = os.path.abspath("config.yaml")
            result = runner.invoke(cli, ["generate", "config", "local", "--config-path", config_path])
            self.assertEqual(result.exit_code, os.EX_OK)
            self.assertFalse(os.path.isfile(config_path))
            self.assertIn("Not generating Observatory Config", result.output)

            # Test generate terraform config
            config_path = os.path.abspath("config-terraform.yaml")
            result = runner.invoke(cli, ["generate", "config", "terraform", "--config-path", config_path])
            self.assertEqual(result.exit_code, os.EX_OK)
            self.assertFalse(os.path.isfile(config_path))
            self.assertIn("Not generating Terraform Config", result.output)

    @patch("observatory.platform.cli.cli.GenerateCommand.generate_local_config_interactive")
    def test_generate_local_interactive(self, m_gen_config):
        runner = CliRunner()
        with runner.isolated_filesystem():
            config_path = os.path.abspath("config.yaml")
            result = runner.invoke(cli, ["generate", "config", "local", "--config-path", config_path, "--interactive"])
            self.assertEqual(result.exit_code, os.EX_OK)
            self.assertEqual(m_gen_config.call_count, 1)
            self.assertEqual(m_gen_config.call_args.kwargs["install_odags"], False)
            self.assertEqual(m_gen_config.call_args.kwargs["config_path"], config_path)


    @patch("observatory.platform.cli.cli.GenerateCommand.generate_local_config_interactive")
    def test_generate_local_interactive_install_odags(self, m_gen_config):
        runner = CliRunner()
        with runner.isolated_filesystem():
            config_path = os.path.abspath("config.yaml")
            result = runner.invoke(cli, args=["generate", "config", "local", "--config-path", config_path, "--interactive"], env={"install_odags":"y"})
            self.assertEqual(result.exit_code, os.EX_OK)
            self.assertEqual(m_gen_config.call_count, 1)
            self.assertEqual(m_gen_config.call_args.kwargs["install_odags"], True)
            self.assertEqual(m_gen_config.call_args.kwargs["config_path"], config_path)


    @patch("observatory.platform.cli.cli.GenerateCommand.generate_terraform_config_interactive")
    def test_generate_terraform_interactive(self, m_gen_config):
        runner = CliRunner()
        with runner.isolated_filesystem():
            config_path = os.path.abspath("config.yaml")
            result = runner.invoke(cli, ["generate", "config", "terraform", "--config-path", config_path, "--interactive"])
            self.assertEqual(result.exit_code, os.EX_OK)
            self.assertEqual(m_gen_config.call_count, 1)
            self.assertEqual(m_gen_config.call_args.kwargs["install_odags"], False)
            self.assertEqual(m_gen_config.call_args.kwargs["config_path"], config_path)


class MockConfig(Mock):
    def __init__(self, is_valid: bool, errors: List = None, **kwargs: Any):
        super().__init__(**kwargs)
        self._is_valid = is_valid
        self._errors = errors

    @property
    def observatory(self):
        mock = Mock()
        mock.observatory_home = "/path/to/home"
        return mock

    @property
    def is_valid(self):
        return self._is_valid

    @property
    def errors(self):
        return self._errors


class MockPlatformCommand:
    def __init__(
        self,
        is_environment_valid: bool,
        docker_exe_path: str,
        is_docker_running: bool,
        docker_compose_path: str,
        config_exists: bool,
        config: Any,
        build_return_code: int,
        start_return_code: int,
        stop_return_code: int,
        wait_for_airflow_ui: bool,
        config_path: str,
        dags_path: str,
        **kwargs: Any,
    ):
        # super().__init__(**kwargs)
        self.is_environment_valid = is_environment_valid
        self.docker_exe_path = docker_exe_path
        self.is_docker_running = is_docker_running
        self.docker_compose_path = docker_compose_path
        self.config_exists = config_exists
        self.config = config
        self.config_path = config_path
        self.host_uid = HOST_UID
        self.host_gid = HOST_GID
        self.debug = DEBUG
        self.dags_path = dags_path
        self._build_return_code = build_return_code
        self._start_return_code = start_return_code
        self._stop_return_code = stop_return_code
        self._wait_for_airflow_ui = wait_for_airflow_ui

    def make_files(self):
        pass

    @property
    def ui_url(self):
        return "http://localhost:8080"

    def build(self):
        return ProcessOutput("output", "error", self._build_return_code)

    def start(self):
        return ProcessOutput("output", "error", self._start_return_code)

    def stop(self):
        return ProcessOutput("output", "error", self._stop_return_code)

    def wait_for_airflow_ui(self, timeout: int = 60):
        return self._wait_for_airflow_ui


class TestObservatoryPlatform(unittest.TestCase):
    @patch("observatory.platform.cli.cli.PlatformCommand")
    def test_platform_start_stop_success(self, mock_cmd):
        """Test that the start and stop command are successful"""

        runner = CliRunner()
        with runner.isolated_filesystem() as t:
            # Make empty config
            config_path = os.path.join(t, "config.yaml")
            open(config_path, "a").close()

            # Mock platform command
            is_environment_valid = True
            docker_exe_path = "/path/to/docker"
            is_docker_running = True
            docker_compose_path = "/path/to/docker-compose"
            config_exists = True
            config = MockConfig(is_valid=True)
            build_return_code = 0
            start_return_code = 0
            stop_return_code = 0
            wait_for_airflow_ui = True
            dags_path = "/path/to/dags"

            mock_cmd.return_value = MockPlatformCommand(
                is_environment_valid,
                docker_exe_path,
                is_docker_running,
                docker_compose_path,
                config_exists,
                config,
                build_return_code,
                start_return_code,
                stop_return_code,
                wait_for_airflow_ui,
                config_path,
                dags_path,
            )

            # Test that start command works
            result = runner.invoke(cli, ["platform", "start", "--config-path", config_path])
            self.assertEqual(result.exit_code, os.EX_OK)

            # Test that stop command works
            result = runner.invoke(cli, ["platform", "stop", "--config-path", config_path])
            self.assertEqual(result.exit_code, os.EX_OK)

    @patch("observatory.platform.cli.cli.PlatformCommand")
    def test_platform_start_fail(self, mock_cmd):
        """Test that the start command error messages and return codes"""

        # Check that no config file generates an error
        runner = CliRunner()
        with runner.isolated_filesystem() as t:
            # Environment invalid, no Docker, Docker not running, no Docker Compose, no config file
            default_config_path = os.path.join(t, "config.yaml")
            is_environment_valid = False
            docker_exe_path = None
            is_docker_running = False
            docker_compose_path = None
            config_exists = False
            config = None
            build_return_code = 0
            start_return_code = 0
            stop_return_code = 0
            wait_for_airflow_ui = True
            dags_path = "/path/to/dags"
            mock_cmd.return_value = MockPlatformCommand(
                is_environment_valid,
                docker_exe_path,
                is_docker_running,
                docker_compose_path,
                config_exists,
                config,
                build_return_code,
                start_return_code,
                stop_return_code,
                wait_for_airflow_ui,
                default_config_path,
                dags_path,
            )

            # Test that start command fails
            result = runner.invoke(cli, ["platform", "start", "--config-path", default_config_path])
            self.assertEqual(result.exit_code, os.EX_CONFIG)

            # config.yaml
            self.assertIn("- file not found, generating a default file", result.output)
            self.assertTrue(os.path.isfile(default_config_path))

            # Check return code
            self.assertEqual(result.exit_code, os.EX_CONFIG)

        # Test that docker and docker compose not installed errors show up
        runner = CliRunner()
        with runner.isolated_filesystem() as t:
            # Environment invalid, no Docker, Docker not running, no Docker Compose, no config file
            default_config_path = os.path.join(t, "config.yaml")
            is_environment_valid = False
            docker_exe_path = None
            is_docker_running = False
            docker_compose_path = None
            config_exists = True
            validation_error = ValidationError("google_cloud.credentials", "required field")
            config = MockConfig(is_valid=False, errors=[validation_error])
            build_return_code = 0
            start_return_code = 0
            stop_return_code = 0
            wait_for_airflow_ui = True
            dags_path = "/path/to/dags"
            mock_cmd.return_value = MockPlatformCommand(
                is_environment_valid,
                docker_exe_path,
                is_docker_running,
                docker_compose_path,
                config_exists,
                config,
                build_return_code,
                start_return_code,
                stop_return_code,
                wait_for_airflow_ui,
                default_config_path,
                dags_path,
            )

            # Make empty config
            open(default_config_path, "a").close()

            # Test that start command fails
            result = runner.invoke(cli, ["platform", "start", "--config-path", default_config_path])
            self.assertEqual(result.exit_code, os.EX_CONFIG)

            # Docker not installed
            self.assertIn("https://docs.docker.com/get-docker/", result.output)

            # Docker Compose not installed
            self.assertIn("https://docs.docker.com/compose/install/", result.output)

            # Check return code
            self.assertEqual(result.exit_code, os.EX_CONFIG)

        # Test that invalid config errors show up
        # Test that error message is printed when Docker is installed but not running
        runner = CliRunner()
        with runner.isolated_filesystem() as t:
            # Environment invalid, Docker installed but not running
            default_config_path = os.path.join(t, "config.yaml")
            is_environment_valid = False
            docker_exe_path = "/path/to/docker"
            is_docker_running = False
            docker_compose_path = "/path/to/docker-compose"
            config_exists = True
            validation_error = ValidationError("google_cloud.credentials", "required field")
            config = MockConfig(is_valid=False, errors=[validation_error])
            build_return_code = 0
            start_return_code = 0
            stop_return_code = 0
            wait_for_airflow_ui = True
            mock_cmd.return_value = MockPlatformCommand(
                is_environment_valid,
                docker_exe_path,
                is_docker_running,
                docker_compose_path,
                config_exists,
                config,
                build_return_code,
                start_return_code,
                stop_return_code,
                wait_for_airflow_ui,
                default_config_path,
                dags_path,
            )

            # Make empty config
            open(default_config_path, "a").close()

            # Test that start command fails
            result = runner.invoke(cli, ["platform", "start", "--config-path", default_config_path])
            self.assertEqual(result.exit_code, os.EX_CONFIG)

            # Check that google credentials file does not exist is printed
            self.assertIn(f"google_cloud.credentials: required field", result.output)

            # Check that Docker is not running message printed
            self.assertIn("not running, please start", result.output)

            # Check return code
            self.assertEqual(result.exit_code, os.EX_CONFIG)


class TestObservatoryTerraform(unittest.TestCase):
    organisation = os.getenv("TEST_TERRAFORM_ORGANISATION")
    token = os.getenv("TEST_TERRAFORM_TOKEN")
    terraform_api = TerraformApi(token)
    version = TerraformApi.TERRAFORM_WORKSPACE_VERSION
    description = "test"

    @patch("click.confirm")
    @patch("observatory.platform.observatory_config.TerraformConfig.load")
    def test_terraform_create_update(self, mock_load_config, mock_click_confirm):
        """Test creating and updating a terraform cloud workspace"""

        # Create token json
        token_json = {"credentials": {"app.terraform.io": {"token": self.token}}}
        runner = CliRunner()
        with runner.isolated_filesystem() as working_dir:
            # File paths
            terraform_credentials_path = os.path.join(working_dir, "token.json")
            config_file_path = os.path.join(working_dir, "config-terraform.yaml")
            credentials_file_path = os.path.join(working_dir, "google_application_credentials.json")
            TerraformConfig.WORKSPACE_PREFIX = random_id() + "-"

            # Create token file
            with open(terraform_credentials_path, "w") as f:
                json.dump(token_json, f)

            # Make a fake google application credentials as it is required schema validation
            with open(credentials_file_path, "w") as f:
                f.write("")

            # Make a fake config-terraform.yaml file
            with open(config_file_path, "w") as f:
                f.write("")

            # Create config instance
            config = TerraformConfig.from_dict(
                {
                    "backend": {"type": "terraform", "environment": "develop"},
                    "observatory": {
                        "airflow_fernet_key": "ez2TjBjFXmWhLyVZoZHQRTvBcX2xY7L4A7Wjwgr6SJU=",
                        "airflow_secret_key": "a" * 16,
                        "airflow_ui_user_password": "password",
                        "airflow_ui_user_email": "password",
                        "postgres_password": "my-password",
                    },
                    "terraform": {"organization": self.organisation},
                    "google_cloud": {
                        "project_id": "my-project",
                        "credentials": credentials_file_path,
                        "region": "us-west1",
                        "zone": "us-west1-c",
                        "data_location": "us",
                    },
                    "cloud_sql_database": {"tier": "db-custom-2-7680", "backup_start_time": "23:00"},
                    "airflow_main_vm": {
                        "machine_type": "n2-standard-2",
                        "disk_size": 1,
                        "disk_type": "pd-ssd",
                        "create": True,
                    },
                    "airflow_worker_vm": {
                        "machine_type": "n2-standard-2",
                        "disk_size": 1,
                        "disk_type": "pd-standard",
                        "create": False,
                    },
                    "elasticsearch": {"host": "https://address.region.gcp.cloud.es.io:port", "api_key": "API_KEY"},
                    "api": {"domain_name": "api.custom.domain", "subdomain": "project_id"},
                }
            )
            self.assertTrue(config.is_valid)
            mock_load_config.return_value = config

            # Create terraform api instance
            terraform_api = TerraformApi(self.token)
            workspace = TerraformConfig.WORKSPACE_PREFIX + config.backend.environment.value

            # As a safety measure, delete workspace even though it shouldn't exist yet
            terraform_api.delete_workspace(self.organisation, workspace)

            # Create workspace, confirm yes
            mock_click_confirm.return_value = "y"
            result = runner.invoke(
                cli,
                [
                    "terraform",
                    "create-workspace",
                    config_file_path,
                    "--terraform-credentials-path",
                    terraform_credentials_path,
                ],
            )
            self.assertIn("Successfully created workspace", result.output)

            # Create workspace, confirm no
            mock_click_confirm.return_value = False
            result = runner.invoke(
                cli,
                [
                    "terraform",
                    "create-workspace",
                    config_file_path,
                    "--terraform-credentials-path",
                    terraform_credentials_path,
                ],
            )
            self.assertNotIn("Creating workspace...", result.output)

            # Update workspace, same config file but sensitive values will be replaced
            mock_click_confirm.return_value = "y"
            result = runner.invoke(
                cli,
                [
                    "terraform",
                    "update-workspace",
                    config_file_path,
                    "--terraform-credentials-path",
                    terraform_credentials_path,
                ],
            )
            self.assertIn("Successfully updated workspace", result.output)

            # Update workspace, confirm no
            mock_click_confirm.return_value = False
            result = runner.invoke(
                cli,
                [
                    "terraform",
                    "update-workspace",
                    config_file_path,
                    "--terraform-credentials-path",
                    terraform_credentials_path,
                ],
            )
            self.assertNotIn("Updating workspace...", result.output)

            # Delete workspace
            terraform_api.delete_workspace(self.organisation, workspace)

    @patch("observatory.platform.observatory_config.TerraformConfig.load")
    def test_terraform_check_dependencies(self, mock_load_config):
        """Test that checking for dependencies prints the correct output when files are missing"""
        runner = CliRunner()
        with runner.isolated_filesystem() as working_dir:
            credentials_file_path = os.path.join(working_dir, "google_application_credentials.json")
            TerraformConfig.WORKSPACE_PREFIX = random_id() + "-"

            # No config file should exist because we are in a new isolated filesystem
            config_file_path = os.path.join(working_dir, "config-terraform.yaml")
            terraform_credentials_path = os.path.join(working_dir, "terraform-creds.yaml")

            # Check that correct exit code and output are returned
            result = runner.invoke(
                cli,
                [
                    "terraform",
                    "create-workspace",
                    config_file_path,
                    "--terraform-credentials-path",
                    terraform_credentials_path,
                ],
            )

            # No config file
            self.assertIn(
                f"Error: Invalid value for 'CONFIG_PATH': File '{config_file_path}' does not exist.", result.output
            )

            # Check return code, exit from click invalid option
            self.assertEqual(result.exit_code, 2)

            # Make a fake config-terraform.yaml file
            with open(config_file_path, "w") as f:
                f.write("")

            # Create config instance
            config = TerraformConfig.from_dict(
                {
                    "backend": {"type": "terraform", "environment": "develop"},
                    "observatory": {
                        "airflow_fernet_key": "ez2TjBjFXmWhLyVZoZHQRTvBcX2xY7L4A7Wjwgr6SJU=",
                        "airflow_secret_key": "a" * 16,
                        "airflow_ui_user_password": "password",
                        "airflow_ui_user_email": "password",
                        "postgres_password": "my-password",
                    },
                    "terraform": {"organization": self.organisation},
                    "google_cloud": {
                        "project_id": "my-project",
                        "credentials": credentials_file_path,
                        "region": "us-west1",
                        "zone": "us-west1-c",
                        "data_location": "us",
                    },
                    "cloud_sql_database": {"tier": "db-custom-2-7680", "backup_start_time": "23:00"},
                    "airflow_main_vm": {
                        "machine_type": "n2-standard-2",
                        "disk_size": 1,
                        "disk_type": "pd-ssd",
                        "create": True,
                    },
                    "airflow_worker_vm": {
                        "machine_type": "n2-standard-2",
                        "disk_size": 1,
                        "disk_type": "pd-standard",
                        "create": False,
                    },
                    "elasticsearch": {"host": "https://address.region.gcp.cloud.es.io:port", "api_key": "API_KEY"},
                    "api": {"domain_name": "api.custom.domain", "subdomain": "project_id"},
                }
            )
            mock_load_config.return_value = config

            # Run again with existing config, specifying terraform files that don't exist. Check that correct exit
            # code and output are returned
            result = runner.invoke(
                cli,
                [
                    "terraform",
                    "create-workspace",
                    config_file_path,
                    "--terraform-credentials-path",
                    terraform_credentials_path,
                ],
            )

            # No terraform credentials file
            self.assertIn(
                "Terraform credentials file:\n   - file not found, create one by running 'terraform login'",
                result.output,
            )

            # Check return code
            self.assertEqual(result.exit_code, os.EX_CONFIG)
