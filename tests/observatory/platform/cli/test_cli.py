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
from typing import Any, List, Optional
from unittest.mock import MagicMock, Mock, PropertyMock, call, patch

from click.testing import CliRunner
from observatory.platform.cli.cli import (
    LOCAL_CONFIG_PATH,
    PLATFORM_NAME,
    TERRAFORM_API_CONFIG_PATH,
    TERRAFORM_CONFIG_PATH,
    TERRAFORM_NAME,
    TerraformCommand,
    cli,
    generate,
    platform_start,
    platform_stop,
    terraform_check_dependencies,
)
from observatory.platform.cli.generate_command import GenerateCommand
from observatory.platform.docker.compose import ProcessOutput
from observatory.platform.observatory_config import GoogleCloud, INDENT2, TerraformConfig, ValidationError, indent
from observatory.platform.platform_builder import DEBUG, HOST_UID
from observatory.platform.terraform_api import TerraformApi
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
            self.assertIn("Not generating config file\n", result.output)

            # Test generate terraform config
            config_path = os.path.abspath("config-terraform.yaml")
            result = runner.invoke(cli, ["generate", "config", "terraform", "--config-path", config_path])
            self.assertEqual(result.exit_code, os.EX_OK)
            self.assertFalse(os.path.isfile(config_path))
            self.assertIn("Not generating config file\n", result.output)

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
    @patch("observatory.platform.cli.cli.GenerateCommand.generate_terraform_api_config")
    @patch("observatory.platform.cli.cli.GenerateCommand.generate_terraform_config")
    @patch("observatory.platform.cli.cli.GenerateCommand.generate_local_config")
    def test_generate_default_configs(self, m_gen_config, m_gen_terra, m_gen_terra_api, m_click):
        m_click.return_value = True
        runner = CliRunner()

        # Default local
        result = runner.invoke(cli, ["generate", "config", "local"])
        self.assertEqual(result.exit_code, os.EX_OK)
        m_gen_config.assert_called_once_with(LOCAL_CONFIG_PATH, workflows=[], editable=False)

        # Default terraform
        result = runner.invoke(cli, ["generate", "config", "terraform"])
        self.assertEqual(result.exit_code, os.EX_OK)
        m_gen_terra.assert_called_once_with(TERRAFORM_CONFIG_PATH, workflows=[], editable=False)

        # Default terraform api
        result = runner.invoke(cli, ["generate", "config", "terraform-api"])
        self.assertEqual(result.exit_code, os.EX_OK)
        m_gen_terra_api.assert_called_once_with(TERRAFORM_API_CONFIG_PATH)

    @patch("observatory.platform.cli.cli.click.confirm")
    @patch("observatory.platform.cli.cli.GenerateCommand.generate_local_config")
    def test_generate_config_exists(self, m_gen_config, m_confirm):
        runner = CliRunner()
        with runner.isolated_filesystem():
            config_path = os.path.abspath("config.yaml")

            # First test don't allow overwrite, second test allow overwrite
            m_confirm.side_effect = [False, True]

            # Create config file
            with open(config_path, "w") as f:
                f.write("foo")

            # Test when config exists and don't overwrite
            runner.invoke(cli, ["generate", "config", "local", "--config-path", config_path])
            m_gen_config.assert_not_called()

            # Test when config exists and overwrite
            runner.invoke(cli, ["generate", "config", "local", "--config-path", config_path])
            m_gen_config.assert_called_once_with(config_path, workflows=[], editable=False)

    @patch("observatory.platform.cli.cli.GenerateCommand.generate_local_config_interactive")
    def test_generate_local_interactive(self, m_gen_config):
        runner = CliRunner()
        with runner.isolated_filesystem():
            config_path = os.path.abspath("config.yaml")
            result = runner.invoke(cli, ["generate", "config", "local", "--config-path", config_path, "--interactive"])
            self.assertEqual(result.exit_code, os.EX_OK)
            m_gen_config.assert_called_once_with(config_path, workflows=[], editable=False)

    @patch("observatory.platform.cli.cli.GenerateCommand.generate_local_config_interactive")
    def test_generate_local_interactive_install_workflows(self, m_gen_config):
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
            m_gen_config.assert_called_once_with(
                config_path, workflows=["academic-observatory-workflows", "oaebu-workflows"], editable=False
            )

    @patch("observatory.platform.cli.cli.GenerateCommand.generate_terraform_config_interactive")
    def test_generate_terraform_interactive(self, m_gen_config):
        runner = CliRunner()
        with runner.isolated_filesystem():
            config_path = os.path.abspath("config.yaml")
            result = runner.invoke(
                cli, ["generate", "config", "terraform", "--config-path", config_path, "--interactive"]
            )
            self.assertEqual(result.exit_code, os.EX_OK)
            m_gen_config.assert_called_once_with(config_path, workflows=[], editable=False)

    @patch("observatory.platform.cli.cli.GenerateCommand.generate_terraform_config_interactive")
    def test_generate_terraform_interactive_install_workflows(self, m_gen_config):
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
            m_gen_config.assert_called_once_with(
                config_path, workflows=["academic-observatory-workflows", "oaebu-workflows"], editable=False
            )


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


class MockPlatformCommand(Mock):
    def __init__(
        self,
        *,
        is_environment_valid: bool = True,
        docker_exe_path: Optional[str] = "/path/to/docker",
        is_docker_running: bool = True,
        docker_compose_path: Optional[str] = "/path/to/docker-compose",
        config_exists: bool = True,
        config: Any = MockConfig(is_valid=True),
        build_return_code: int = 0,
        start_return_code: int = 0,
        stop_return_code: int = 0,
        wait_for_airflow_ui: bool = True,
        config_path: str = "config.yaml",
        dags_path: str = "/path/to/dags",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.is_environment_valid = is_environment_valid
        self.docker_exe_path = docker_exe_path
        self.is_docker_running = is_docker_running
        self.docker_compose_path = docker_compose_path
        self.config_exists = config_exists
        self.config = config
        self.config_path = config_path
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
    def test_platform_start(self):
        runner = CliRunner()
        with runner.isolated_filesystem():
            # Test platform start function when build has failed
            platform_cmd = MockPlatformCommand(build_return_code=1)
            with self.assertRaises(SystemExit):
                platform_start(platform_cmd)

            # Test platform start function when starting ui is successful
            with patch("builtins.print") as mock_print:
                platform_cmd = MockPlatformCommand(wait_for_airflow_ui=True)
                platform_start(platform_cmd)
                mock_print.assert_called_with(f"View the Apache Airflow UI at {platform_cmd.ui_url}")

            # Test platform start function when starting ui has failed
            with patch("builtins.print") as mock_print:
                platform_cmd = MockPlatformCommand(wait_for_airflow_ui=False)
                platform_start(platform_cmd)
                mock_print.assert_called_with(f"Could not find the Airflow UI at {platform_cmd.ui_url}")

            # Test platform start function when starting platform has failed
            platform_cmd = MockPlatformCommand(start_return_code=1)
            with self.assertRaises(SystemExit):
                platform_start(platform_cmd)

    def test_platform_stop(self):
        runner = CliRunner()
        with runner.isolated_filesystem():
            # Test platform stop function when stop is successful
            with patch("builtins.print") as mock_print:
                platform_cmd = MockPlatformCommand()
                platform_stop(platform_cmd, min_line_chars=10)
                mock_print.assert_called_with(f"{PLATFORM_NAME}: stopped".ljust(10))

            # Test platform stop function when stop has failed
            platform_cmd = MockPlatformCommand(stop_return_code=1)
            with self.assertRaises(SystemExit):
                platform_stop(platform_cmd)

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
                is_environment_valid=is_environment_valid,
                docker_exe_path=docker_exe_path,
                is_docker_running=is_docker_running,
                docker_compose_path=docker_compose_path,
                config_exists=config_exists,
                config=config,
                build_return_code=build_return_code,
                start_return_code=start_return_code,
                stop_return_code=stop_return_code,
                wait_for_airflow_ui=wait_for_airflow_ui,
                config_path=config_path,
                dags_path=dags_path,
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
                is_environment_valid=is_environment_valid,
                docker_exe_path=docker_exe_path,
                is_docker_running=is_docker_running,
                docker_compose_path=docker_compose_path,
                config_exists=config_exists,
                config=config,
                build_return_code=build_return_code,
                start_return_code=start_return_code,
                stop_return_code=stop_return_code,
                wait_for_airflow_ui=wait_for_airflow_ui,
                config_path=default_config_path,
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
                is_environment_valid=is_environment_valid,
                docker_exe_path=docker_exe_path,
                is_docker_running=is_docker_running,
                docker_compose_path=docker_compose_path,
                config_exists=config_exists,
                config=config,
                build_return_code=build_return_code,
                start_return_code=start_return_code,
                stop_return_code=stop_return_code,
                wait_for_airflow_ui=wait_for_airflow_ui,
                config_path=default_config_path,
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
                is_environment_valid=is_environment_valid,
                docker_exe_path=docker_exe_path,
                is_docker_running=is_docker_running,
                docker_compose_path=docker_compose_path,
                config_exists=config_exists,
                config=config,
                build_return_code=build_return_code,
                start_return_code=start_return_code,
                stop_return_code=stop_return_code,
                wait_for_airflow_ui=wait_for_airflow_ui,
                config_path=default_config_path,
                dags_path=dags_path,
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

    @patch.object(TerraformCommand, "build_terraform")
    @patch("observatory.platform.cli.terraform_command.TerraformCommand")
    def test_terraform_build_terraform(self, mock_terraform_command, mock_build):
        runner = CliRunner()
        with runner.isolated_filesystem():
            # Create google credentials file
            google_credentials_path = os.path.abspath("gcp_credentials.json")
            with open(google_credentials_path, "w") as f:
                f.write("foo")

            # Create terraform credentials file
            token_json = {"credentials": {"app.terraform.io": {"token": self.token}}}
            terraform_credentials_path = os.path.abspath("token.json")
            with open(terraform_credentials_path, "w") as f:
                json.dump(token_json, f)

            # Create config file
            config_path = os.path.abspath("config-terraform.yaml")
            TerraformConfig(google_cloud=GoogleCloud(credentials=google_credentials_path)).save(config_path)

            result = runner.invoke(
                cli,
                [
                    "terraform",
                    "build-terraform",
                    config_path,
                    "--terraform-credentials-path",
                    terraform_credentials_path,
                    "--config-type",
                    "terraform",
                ],
            )
            self.assertEqual(0, result.exit_code)
            mock_build.assert_called_once_with()

    @patch.object(TerraformCommand, "build_image")
    @patch("observatory.platform.cli.terraform_command.TerraformCommand")
    def test_terraform_build_image(self, mock_terraform_command, mock_build):
        runner = CliRunner()
        with runner.isolated_filesystem():
            # Create google credentials file
            google_credentials_path = os.path.abspath("gcp_credentials.json")
            with open(google_credentials_path, "w") as f:
                f.write("foo")

            # Create terraform credentials file
            token_json = {"credentials": {"app.terraform.io": {"token": self.token}}}
            terraform_credentials_path = os.path.abspath("token.json")
            with open(terraform_credentials_path, "w") as f:
                json.dump(token_json, f)

            # Create config file
            config_path = os.path.abspath("config-terraform.yaml")
            TerraformConfig(google_cloud=GoogleCloud(credentials=google_credentials_path)).save(config_path)

            result = runner.invoke(
                cli,
                [
                    "terraform",
                    "build-image",
                    config_path,
                    "--terraform-credentials-path",
                    terraform_credentials_path,
                    "--config-type",
                    "terraform",
                ],
            )
            self.assertEqual(0, result.exit_code)
            mock_build.assert_called_once_with()

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

    @patch("builtins.print")
    def test_terraform_check_dependencies(self, mock_print):
        """Test that checking for dependencies prints the correct output when files are missing"""
        runner = CliRunner()
        with runner.isolated_filesystem():
            generate_cmd = GenerateCommand()
            generate_cmd.generate_terraform_config = Mock(side_effect=generate_cmd.generate_terraform_config)
            terraform_cmd = TerraformCommand("config_path", "terraform_credentials_path", config_type="terraform")

            # Test with valid environment
            with patch(
                "observatory.platform.cli.terraform_command.TerraformCommand.is_environment_valid",
                new_callable=PropertyMock,
                return_value=True,
            ):
                terraform_check_dependencies(terraform_cmd, generate_cmd, config_type="terraform", min_line_chars=0)
                self.assertIn(call(f"{TERRAFORM_NAME}: all dependencies found"), mock_print.call_args_list)

            # Test with invalid environment
            with patch(
                "observatory.platform.cli.terraform_command.TerraformCommand.is_environment_valid",
                new_callable=PropertyMock,
                return_value=False,
            ):
                with self.assertRaises(SystemExit):
                    terraform_check_dependencies(terraform_cmd, generate_cmd, config_type="terraform", min_line_chars=0)
                self.assertIn(call(f"{TERRAFORM_NAME}: dependencies missing"), mock_print.call_args_list)

            with patch(
                "observatory.platform.cli.terraform_command.TerraformCommand.is_environment_valid",
                new_callable=PropertyMock,
                return_value=True,
            ):
                # Test when config exists and with valid config
                terraform_cmd.config_exists = True
                with patch(
                    "observatory.platform.cli.terraform_command.TerraformCommand.config", new_callable=MagicMock
                ) as mock_config:
                    mock_print.reset_mock()
                    mock_config.is_valid = True
                    terraform_check_dependencies(terraform_cmd, generate_cmd, config_type="terraform", min_line_chars=0)
                    self.assertIn(call(indent("- file valid", INDENT2)), mock_print.call_args_list)

                # Test when config exists and with invalid config
                terraform_cmd.config_exists = True
                with patch(
                    "observatory.platform.cli.terraform_command.TerraformCommand.config", new_callable=MagicMock
                ) as mock_config:
                    mock_print.reset_mock()
                    mock_config.is_valid = False
                    terraform_check_dependencies(terraform_cmd, generate_cmd, config_type="terraform", min_line_chars=0)
                    self.assertIn(call(indent("- file invalid", INDENT2)), mock_print.call_args_list)

                # Test when config does not exist
                terraform_cmd.config_exists = False
                generate_cmd.generate_terraform_config.reset_mock()
                terraform_check_dependencies(terraform_cmd, generate_cmd, config_type="terraform", min_line_chars=0)
                generate_cmd.generate_terraform_config.assert_called_once_with(
                    "config_path", editable=False, workflows=[]
                )

                # Test when terraform credentials exist
                terraform_cmd.terraform_credentials_exists = True
                mock_print.reset_mock()
                terraform_check_dependencies(terraform_cmd, generate_cmd, config_type="terraform", min_line_chars=0)
                self.assertIn(
                    call(indent(f"- path: {terraform_cmd.terraform_credentials_path}", INDENT2)),
                    mock_print.call_args_list,
                )

                # Test when terraform credentials don't exist
                terraform_cmd.terraform_credentials_exists = False
                mock_print.reset_mock()
                terraform_check_dependencies(terraform_cmd, generate_cmd, config_type="terraform", min_line_chars=0)
                self.assertIn(
                    call(indent("- file not found, create one by running 'terraform login'", INDENT2)),
                    mock_print.call_args_list,
                )

                # Test config type 'terraform' and packer exe path is valid
                with patch(
                    "observatory.platform.cli.terraform_command.TerraformCommand.terraform_builder",
                    new_callable=MagicMock,
                ) as mock_builder:
                    mock_print.reset_mock()
                    mock_builder.packer_exe_path = "path/to/packer"
                    terraform_check_dependencies(terraform_cmd, generate_cmd, config_type="terraform", min_line_chars=0)
                    self.assertIn(
                        call(indent(f"- path: {terraform_cmd.terraform_builder.packer_exe_path}", INDENT2)),
                        mock_print.call_args_list,
                    )

                # Test config type 'terraform' and packer exe path is invalid
                with patch(
                    "observatory.platform.cli.terraform_command.TerraformCommand.terraform_builder",
                    new_callable=MagicMock,
                ) as mock_builder:
                    mock_print.reset_mock()
                    mock_builder.packer_exe_path = None
                    terraform_check_dependencies(terraform_cmd, generate_cmd, config_type="terraform", min_line_chars=0)
                    self.assertIn(
                        call(indent("- not installed, please install https://www.packer.io/docs/install", INDENT2)),
                        mock_print.call_args_list,
                    )

                # Test config type 'terraform-api' and gcloud exe path is valid
                with patch(
                    "observatory.platform.cli.terraform_command.TerraformCommand.terraform_builder",
                    new_callable=MagicMock,
                ) as mock_builder:
                    mock_print.reset_mock()
                    mock_builder.gcloud_exe_path = "path/to/gcloud"
                    terraform_check_dependencies(
                        terraform_cmd, generate_cmd, config_type="terraform-api", min_line_chars=0
                    )
                    self.assertIn(
                        call(indent(f"- path: {terraform_cmd.terraform_builder.gcloud_exe_path}", INDENT2)),
                        mock_print.call_args_list,
                    )

                # Test config type 'terraform-api' and gcloud exe path is invalid
                with patch(
                    "observatory.platform.cli.terraform_command.TerraformCommand.terraform_builder",
                    new_callable=MagicMock,
                ) as mock_builder:
                    mock_print.reset_mock()
                    mock_builder.gcloud_exe_path = None
                    terraform_check_dependencies(
                        terraform_cmd, generate_cmd, config_type="terraform-api", min_line_chars=0
                    )
                    self.assertIn(
                        call(
                            indent("- not installed, please install https://cloud.google.com/sdk/docs/install", INDENT2)
                        ),
                        mock_print.call_args_list,
                    )
