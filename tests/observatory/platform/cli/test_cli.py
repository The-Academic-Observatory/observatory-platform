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
import sys
import unittest
from typing import Any, List
from unittest.mock import Mock, patch

from click.testing import CliRunner

from observatory.platform.cli.cli import (
    LOCAL_CONFIG_PATH,
    TERRAFORM_CONFIG_PATH,
    cli,
    generate,
)
from observatory.platform.cli.generate_command import GenerateCommand
from observatory.platform.docker.compose_runner import ProcessOutput
from observatory.platform.docker.platform_runner import DEBUG, HOST_UID
from observatory.platform.observatory_config import TerraformConfig, ValidationError
from observatory.platform.terraform.terraform_api import TerraformApi
from observatory.platform.utils.test_utils import random_id


class TestObservatoryGenerate(unittest.TestCase):
    @patch("click.confirm")
    @patch("os.path.exists")
    def test_generate_secrets(self, mock_path_exists, mock_click_confirm):
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
            self.assertIn("Not generating Observatory config\n", result.output)

            # Test generate terraform config
            config_path = os.path.abspath("config-terraform.yaml")
            result = runner.invoke(cli, ["generate", "config", "terraform", "--config-path", config_path])
            self.assertEqual(result.exit_code, os.EX_OK)
            self.assertFalse(os.path.isfile(config_path))
            self.assertIn("Not generating Terraform config\n", result.output)

    @patch("observatory.platform.cli.cli.stream_process")
    @patch("observatory.platform.cli.cli.subprocess.Popen")
    @patch.object(GenerateCommand, "generate_workflows_project")
    def test_generate_project(self, mock_generate_project, mock_subprocess, mock_stream_process):
        """Test generating a new workflows project, with and without installing the created package.

        :return: None.
        """
        mock_subprocess.return_value = "proc"
        runner = CliRunner()
        with runner.isolated_filesystem():
            # Test creating project and installing package with pip
            os.makedirs("project_path")
            result = runner.invoke(generate, ["project", "project_path", "package_name", "Author Name"], input="y")
            self.assertEqual(0, result.exit_code)
            mock_generate_project.assert_called_once_with("project_path", "package_name", "Author Name")
            mock_subprocess.assert_called_once_with(
                [sys.executable, "-m", "pip", "install", "-e", "."], cwd="project_path", stderr=-1, stdout=-1
            )
            mock_stream_process.called_once_with("proc", True)

            # Test creating project without installing
            mock_generate_project.reset_mock()
            mock_subprocess.reset_mock()
            mock_stream_process.reset_mock()
            result = runner.invoke(generate, ["project", "project_path", "package_name", "Author Name"], input="n")
            self.assertEqual(0, result.exit_code)
            mock_generate_project.assert_called_once_with("project_path", "package_name", "Author Name")
            mock_subprocess.assert_not_called()
            mock_stream_process.assert_not_called()

    @patch.object(GenerateCommand, "generate_workflow")
    def test_generate_workflow(self, mock_generate_workflow):
        """Test generating workflow files for valid and invalid project dirs

        :return: None.
        """
        runner = CliRunner()
        with runner.isolated_filesystem():
            workflow_type = "Workflow"
            workflow_name = "MyWorkflow"

            # Test running with --project-path parameter for valid project dir
            project_dir = "project1"
            package_name = "package1"
            os.makedirs(os.path.join(project_dir, "test.egg-info"))
            with open(os.path.join(project_dir, "test.egg-info", "top_level.txt"), "w") as f:
                f.write(package_name)
            result = runner.invoke(generate, ["workflow", workflow_type, workflow_name, "-p", project_dir])
            self.assertEqual(0, result.exit_code)
            mock_generate_workflow.assert_called_once_with(
                package_name=package_name,
                project_path=project_dir,
                workflow_class=workflow_name,
                workflow_type=workflow_type,
            )

            # Test running with --project-path parameter for invalid project dirs
            mock_generate_workflow.reset_mock()
            project_dir = "project2"
            os.makedirs(project_dir)
            result = runner.invoke(generate, ["workflow", workflow_type, workflow_name, "-p", project_dir])
            self.assertEqual(78, result.exit_code)
            mock_generate_workflow.assert_not_called()

            os.makedirs(os.path.join(project_dir, "test1.egg-info"))
            os.makedirs(os.path.join(project_dir, "test2.egg-info"))
            result = runner.invoke(generate, ["workflow", workflow_type, workflow_name, "-p", project_dir])
            self.assertEqual(78, result.exit_code)
            mock_generate_workflow.assert_not_called()

    @patch("observatory.platform.cli.cli.click.confirm")
    @patch("observatory.platform.cli.cli.GenerateCommand.generate_terraform_config_interactive")
    @patch("observatory.platform.cli.cli.GenerateCommand.generate_local_config_interactive")
    def test_generate_default_configs(self, m_gen_config, m_gen_terra, m_click):
        m_click.return_value = True
        runner = CliRunner()

        # Default local
        result = runner.invoke(cli, ["generate", "config", "local", "--interactive"])
        self.assertEqual(result.exit_code, os.EX_OK)
        self.assertEqual(m_gen_config.call_count, 1)
        self.assertEqual(m_gen_config.call_args.kwargs["workflows"], [])
        self.assertEqual(m_gen_config.call_args.args[0], LOCAL_CONFIG_PATH)

        # Default terraform
        result = runner.invoke(cli, ["generate", "config", "terraform", "--interactive"])
        self.assertEqual(result.exit_code, os.EX_OK)
        self.assertEqual(m_gen_terra.call_count, 1)
        self.assertEqual(m_gen_terra.call_args.kwargs["workflows"], [])
        self.assertEqual(m_gen_terra.call_args.args[0], TERRAFORM_CONFIG_PATH)

    @patch("observatory.platform.cli.cli.GenerateCommand.generate_local_config_interactive")
    def test_generate_local_interactive(self, m_gen_config):
        runner = CliRunner()
        with runner.isolated_filesystem():
            config_path = os.path.abspath("config.yaml")
            result = runner.invoke(cli, ["generate", "config", "local", "--config-path", config_path, "--interactive"])
            self.assertEqual(result.exit_code, os.EX_OK)
            self.assertEqual(m_gen_config.call_count, 1)
            self.assertEqual(m_gen_config.call_args.kwargs["workflows"], [])
            self.assertEqual(m_gen_config.call_args.args[0], config_path)

    @patch("observatory.platform.cli.cli.GenerateCommand.generate_local_config_interactive")
    def test_generate_local_interactive_install_oworkflows(self, m_gen_config):
        runner = CliRunner()
        with runner.isolated_filesystem():
            config_path = os.path.abspath("config.yaml")
            result = runner.invoke(
                cli,
                args=[
                    "generate",
                    "config",
                    "local",
                    "--config-path",
                    config_path,
                    "--interactive",
                    "--ao-wf",
                    "--oaebu-wf",
                ],
            )
            self.assertEqual(result.exit_code, os.EX_OK)
            self.assertEqual(m_gen_config.call_count, 1)
            self.assertEqual(
                m_gen_config.call_args.kwargs["workflows"], ["academic-observatory-workflows", "oaebu-workflows"]
            )
            self.assertEqual(m_gen_config.call_args.args[0], config_path)

    @patch("observatory.platform.cli.cli.GenerateCommand.generate_terraform_config_interactive")
    def test_generate_terraform_interactive(self, m_gen_config):
        runner = CliRunner()
        with runner.isolated_filesystem():
            config_path = os.path.abspath("config.yaml")
            result = runner.invoke(
                cli, ["generate", "config", "terraform", "--config-path", config_path, "--interactive"]
            )
            self.assertEqual(result.exit_code, os.EX_OK)
            self.assertEqual(m_gen_config.call_count, 1)
            self.assertEqual(m_gen_config.call_args.kwargs["workflows"], [])
            self.assertEqual(m_gen_config.call_args.args[0], config_path)

    @patch("observatory.platform.cli.cli.GenerateCommand.generate_terraform_config_interactive")
    def test_generate_terraform_interactive_install_oworkflows(self, m_gen_config):
        runner = CliRunner()
        with runner.isolated_filesystem():
            config_path = os.path.abspath("config.yaml")
            result = runner.invoke(
                cli,
                [
                    "generate",
                    "config",
                    "terraform",
                    "--config-path",
                    config_path,
                    "--interactive",
                    "--ao-wf",
                    "--oaebu-wf",
                ],
            )
            self.assertEqual(result.exit_code, os.EX_OK)
            self.assertEqual(m_gen_config.call_count, 1)
            self.assertEqual(
                m_gen_config.call_args.kwargs["workflows"], ["academic-observatory-workflows", "oaebu-workflows"]
            )
            self.assertEqual(m_gen_config.call_args.args[0], config_path)


class MockConfig(Mock):
    def __init__(self, is_valid: bool, errors: List[ValidationError] = None, **kwargs: Any):
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


class MockPlatformCommand(Mock):
    def __init__(
        self,
        config: MockConfig,
        *,
        is_environment_valid: bool,
        docker_exe_path: str,
        is_docker_running: bool,
        docker_compose_path: str,
        build_return_code: int,
        start_return_code: int,
        stop_return_code: int,
        wait_for_airflow_ui: bool,
        dags_path: str,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.config = config
        self.is_environment_valid = is_environment_valid
        self.docker_exe_path = docker_exe_path
        self.is_docker_running = is_docker_running
        self.docker_compose_path = docker_compose_path
        self.host_uid = HOST_UID
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
    @patch("observatory.platform.cli.cli.ObservatoryConfig.load")
    @patch("observatory.platform.cli.cli.PlatformCommand")
    def test_platform_start_stop_success(self, mock_cmd, mock_config):
        """Test that the start and stop command are successful"""

        runner = CliRunner()
        with runner.isolated_filesystem() as t:
            # Make empty config
            config_path = os.path.join(t, "config.yaml")

            # Mock platform command
            is_environment_valid = True
            docker_exe_path = "/path/to/docker"
            is_docker_running = True
            docker_compose_path = "/path/to/docker-compose"
            config = MockConfig(is_valid=True)
            mock_config.return_value = config
            build_return_code = 0
            start_return_code = 0
            stop_return_code = 0
            wait_for_airflow_ui = True
            dags_path = "/path/to/dags"
            mock_cmd.return_value = MockPlatformCommand(
                config,
                is_environment_valid=is_environment_valid,
                docker_exe_path=docker_exe_path,
                is_docker_running=is_docker_running,
                docker_compose_path=docker_compose_path,
                build_return_code=build_return_code,
                start_return_code=start_return_code,
                stop_return_code=stop_return_code,
                wait_for_airflow_ui=wait_for_airflow_ui,
                dags_path=dags_path,
            )

            # Make empty config
            open(config_path, "a").close()

            # Test that start command works
            result = runner.invoke(cli, ["platform", "start", "--config-path", config_path])
            self.assertEqual(result.exit_code, os.EX_OK)

            # Test that stop command works
            result = runner.invoke(cli, ["platform", "stop", "--config-path", config_path])
            self.assertEqual(result.exit_code, os.EX_OK)

    @patch("observatory.platform.cli.cli.PlatformCommand")
    def test_platform_start_fail_generate(self, mock_cmd):
        """Check that no config file generates an error"""

        runner = CliRunner()
        with runner.isolated_filesystem() as t:
            # Environment invalid, no Docker, Docker not running, no Docker Compose, no config file
            default_config_path = os.path.join(t, "config.yaml")
            is_environment_valid = False
            docker_exe_path = None
            is_docker_running = False
            docker_compose_path = None
            config = None
            build_return_code = 0
            start_return_code = 0
            stop_return_code = 0
            wait_for_airflow_ui = True
            dags_path = "/path/to/dags"
            mock_cmd.return_value = MockPlatformCommand(
                config,
                is_environment_valid=is_environment_valid,
                docker_exe_path=docker_exe_path,
                is_docker_running=is_docker_running,
                docker_compose_path=docker_compose_path,
                build_return_code=build_return_code,
                start_return_code=start_return_code,
                stop_return_code=stop_return_code,
                wait_for_airflow_ui=wait_for_airflow_ui,
                dags_path=dags_path,
            )

            # Test that start command fails
            result = runner.invoke(cli, ["platform", "start", "--config-path", default_config_path])
            self.assertEqual(result.exit_code, os.EX_CONFIG)

            # config.yaml
            self.assertIn("- file not found, generating a default file", result.output)
            self.assertTrue(os.path.isfile(default_config_path))

            # Check return code
            self.assertEqual(result.exit_code, os.EX_CONFIG)

    @patch("observatory.platform.cli.cli.ObservatoryConfig.load")
    @patch("observatory.platform.cli.cli.PlatformCommand")
    def test_platform_start_fail_docker_install_errors(self, mock_cmd, mock_config):
        """Test that docker and docker compose not installed errors show up"""

        runner = CliRunner()
        with runner.isolated_filesystem() as t:
            # Environment invalid, no Docker, Docker not running, no Docker Compose, no config file
            default_config_path = os.path.join(t, "config.yaml")
            is_environment_valid = False
            docker_exe_path = None
            is_docker_running = False
            docker_compose_path = None
            config = MockConfig(is_valid=True)
            mock_config.return_value = config
            build_return_code = 0
            start_return_code = 0
            stop_return_code = 0
            wait_for_airflow_ui = True
            dags_path = "/path/to/dags"
            mock_cmd.return_value = MockPlatformCommand(
                config,
                is_environment_valid=is_environment_valid,
                docker_exe_path=docker_exe_path,
                is_docker_running=is_docker_running,
                docker_compose_path=docker_compose_path,
                build_return_code=build_return_code,
                start_return_code=start_return_code,
                stop_return_code=stop_return_code,
                wait_for_airflow_ui=wait_for_airflow_ui,
                dags_path=dags_path,
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

    @patch("observatory.platform.cli.cli.ObservatoryConfig.load")
    @patch("observatory.platform.cli.cli.PlatformCommand")
    def test_platform_start_fail_docker_run_errors(self, mock_cmd, mock_config):
        """Test that error message is printed when Docker is installed but not running"""

        runner = CliRunner()
        with runner.isolated_filesystem() as t:
            # Environment invalid, Docker installed but not running
            default_config_path = os.path.join(t, "config.yaml")
            is_environment_valid = False
            docker_exe_path = "/path/to/docker"
            is_docker_running = False
            docker_compose_path = "/path/to/docker-compose"
            config = MockConfig(is_valid=True)
            mock_config.return_value = config
            build_return_code = 0
            start_return_code = 0
            stop_return_code = 0
            wait_for_airflow_ui = True
            dags_path = "/path/to/dags"
            mock_cmd.return_value = MockPlatformCommand(
                config,
                is_environment_valid=is_environment_valid,
                docker_exe_path=docker_exe_path,
                is_docker_running=is_docker_running,
                docker_compose_path=docker_compose_path,
                build_return_code=build_return_code,
                start_return_code=start_return_code,
                stop_return_code=stop_return_code,
                wait_for_airflow_ui=wait_for_airflow_ui,
                dags_path=dags_path,
            )

            # Make empty config
            open(default_config_path, "a").close()

            # Test that start command fails
            result = runner.invoke(cli, ["platform", "start", "--config-path", default_config_path])
            self.assertEqual(result.exit_code, os.EX_CONFIG)

            # Check that Docker is not running message printed
            self.assertIn("not running, please start", result.output)

            # Check return code
            self.assertEqual(result.exit_code, os.EX_CONFIG)

    @patch("observatory.platform.cli.cli.ObservatoryConfig.load")
    @patch("observatory.platform.cli.cli.PlatformCommand")
    def test_platform_start_fail_invalid_config_errors(self, mock_cmd, mock_config):
        """Test that invalid config errors show up"""

        runner = CliRunner()
        with runner.isolated_filesystem() as t:
            # Environment invalid, Docker installed but not running
            default_config_path = os.path.join(t, "config.yaml")
            is_environment_valid = False
            docker_exe_path = "/path/to/docker"
            is_docker_running = False
            docker_compose_path = "/path/to/docker-compose"
            validation_error = ValidationError("google_cloud.credentials", "required field")
            config = MockConfig(is_valid=False, errors=[validation_error])
            mock_config.return_value = config
            build_return_code = 0
            start_return_code = 0
            stop_return_code = 0
            wait_for_airflow_ui = True
            dags_path = "/path/to/dags"
            mock_cmd.return_value = MockPlatformCommand(
                config,
                is_environment_valid=is_environment_valid,
                docker_exe_path=docker_exe_path,
                is_docker_running=is_docker_running,
                docker_compose_path=docker_compose_path,
                build_return_code=build_return_code,
                start_return_code=start_return_code,
                stop_return_code=stop_return_code,
                wait_for_airflow_ui=wait_for_airflow_ui,
                dags_path=dags_path,
            )

            # Make empty config
            open(default_config_path, "a").close()

            # Test that start command fails
            result = runner.invoke(cli, ["platform", "start", "--config-path", default_config_path])
            self.assertEqual(result.exit_code, os.EX_CONFIG)

            # Check that google credentials file does not exist is printed
            self.assertIn(f"google_cloud.credentials: required field", result.output)

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
                        "package": "observatory-platform",
                        "package_type": "pypi",
                        "airflow_fernet_key": "IWt5jFGSw2MD1shTdwzLPTFO16G8iEAU3A6mGo_vJTY=",
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
                    "api": {
                        "domain_name": "api.custom.domain",
                        "subdomain": "project_id",
                        "api_image": "us-docker.pkg.dev/gcp-project-id/observatory-platform/observatory-api:latest",
                    },
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

            # Make a fake google credentials file
            with open(credentials_file_path, "w") as f:
                f.write("")

            # Create config instance
            config = TerraformConfig.from_dict(
                {
                    "backend": {"type": "terraform", "environment": "develop"},
                    "observatory": {
                        "package": "observatory-platform",
                        "package_type": "pypi",
                        "airflow_fernet_key": "IWt5jFGSw2MD1shTdwzLPTFO16G8iEAU3A6mGo_vJTY=",
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
                    "api": {
                        "domain_name": "api.custom.domain",
                        "subdomain": "project_id",
                        "api_image": "us-docker.pkg.dev/gcp-project-id/observatory-platform/observatory-api:latest",
                    },
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
