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

# Author: James Diprose, Tuan Chien, Aniek Roelofs

import os
import re
import shutil
import subprocess
import sys
import unittest
from datetime import datetime
from unittest.mock import patch

import click
from click.testing import CliRunner
from observatory.platform.cli.cli import generate
from observatory.platform.cli.generate_command import (
    DefaultWorkflowsProject,
    FernetKeyType,
    FlaskSecretKeyType,
    GenerateCommand,
    InteractiveConfigBuilder,
    write_rendered_template,
)
from observatory.platform.observatory_config import (
    AirflowConnection,
    AirflowVariable,
    AirflowMainVm,
    AirflowWorkerVm,
    Api,
    ApiType,
    ApiTypes,
    Backend,
    BackendType,
    CloudSqlDatabase,
    CloudStorageBucket,
    Environment,
    GoogleCloud,
    Observatory,
    ObservatoryConfig,
    Terraform,
    TerraformAPIConfig,
    TerraformConfig,
    WorkflowsProject,
    WorkflowsProjects,
)
from observatory.platform.utils.config_utils import module_file_path
from observatory.platform.utils.proc_utils import stream_process
from observatory.platform.utils.test_utils import ObservatoryTestCase


class TestGenerateCommand(ObservatoryTestCase):
    @patch("observatory.platform.cli.generate_command.ObservatoryConfig", wraps=ObservatoryConfig)
    @patch("observatory.platform.cli.generate_command.InteractiveConfigBuilder.get_installed_workflows")
    @patch("observatory.platform.cli.generate_command.InteractiveConfigBuilder.set_editable_observatory_platform")
    def test_generate_local_config(self, mock_set_editable, mock_get_workflows, mock_observatory_config):
        cmd = GenerateCommand()
        config_path = "config.yaml"

        with CliRunner().isolated_filesystem():
            mock_get_workflows.return_value = []

            # Test with non-editable observatory platform
            cmd.generate_local_config(config_path, editable=False, workflows=[])
            mock_set_editable.assert_not_called()
            mock_observatory_config.assert_called_once_with()
            self.assertTrue(os.path.exists(config_path))

            # Test with editable observatory platform
            mock_observatory_config.reset_mock()
            cmd.generate_local_config(config_path, editable=True, workflows=[])
            mock_set_editable.assert_called_once()
            mock_observatory_config.assert_called_once_with()
            self.assertTrue(os.path.exists(config_path))

            # Test with available workflows installed by installer script
            mock_observatory_config.reset_mock()
            mock_get_workflows.reset_mock()
            mock_get_workflows.return_value = [WorkflowsProject()]
            cmd.generate_local_config(config_path, editable=True, workflows=[])
            mock_observatory_config.assert_called_once_with(workflows_projects=WorkflowsProjects([WorkflowsProject()]))
            self.assertTrue(os.path.exists(config_path))

    @patch("observatory.platform.cli.generate_command.TerraformConfig", wraps=TerraformConfig)
    @patch("observatory.platform.cli.generate_command.InteractiveConfigBuilder.get_installed_workflows")
    @patch("observatory.platform.cli.generate_command.InteractiveConfigBuilder.set_editable_observatory_platform")
    def test_generate_terraform_config(self, mock_set_editable, mock_get_workflows, mock_terraform_config):
        cmd = GenerateCommand()
        config_path = "config-terraform.yaml"

        with CliRunner().isolated_filesystem():
            mock_get_workflows.return_value = []

            # Test with non-editable observatory platform
            cmd.generate_terraform_config(config_path, editable=False, workflows=[])
            mock_set_editable.assert_not_called()
            mock_terraform_config.assert_called_once_with()
            self.assertTrue(os.path.exists(config_path))

            # Test with editable observatory platform
            mock_terraform_config.reset_mock()
            cmd.generate_terraform_config(config_path, editable=True, workflows=[])
            mock_set_editable.assert_called_once()
            mock_terraform_config.assert_called_once_with()
            self.assertTrue(os.path.exists(config_path))

            # Test with available workflows installed by installer script
            mock_terraform_config.reset_mock()
            mock_get_workflows.reset_mock()
            mock_get_workflows.return_value = [WorkflowsProject()]
            cmd.generate_terraform_config(config_path, editable=True, workflows=[])
            mock_terraform_config.assert_called_once_with(workflows_projects=WorkflowsProjects([WorkflowsProject()]))
            self.assertTrue(os.path.exists(config_path))

    @patch("observatory.platform.cli.generate_command.TerraformAPIConfig", wraps=TerraformConfig)
    def test_generate_terraform_api_config(self, mock_terraform_api_config):
        cmd = GenerateCommand()
        config_path = "config-terraform-api.yaml"

        with CliRunner().isolated_filesystem():
            cmd.generate_terraform_api_config(config_path)
            mock_terraform_api_config.assert_called_once_with()
            self.assertTrue(os.path.exists(config_path))

    @patch("observatory.platform.cli.generate_command.InteractiveConfigBuilder.set_editable_observatory_platform")
    @patch("observatory.platform.cli.generate_command.InteractiveConfigBuilder.build")
    def test_generate_local_config_interactive(self, mock_build, m_set_edit):
        cmd = GenerateCommand()
        cmd.generate_local_config_interactive(
            config_path="path", workflows=["academic-observatory-workflows"], editable=False
        )
        mock_build.assert_called_once_with(
            backend_type=BackendType.local, workflows=["academic-observatory-workflows"], editable=False
        )
        m_set_edit.assert_not_called()
        mock_build.reset_mock()

        cmd.generate_local_config_interactive(
            config_path="path", workflows=["academic-observatory-workflows"], editable=True
        )
        mock_build.assert_called_once_with(
            backend_type=BackendType.local, workflows=["academic-observatory-workflows"], editable=True
        )
        m_set_edit.assert_called_once()

    @patch("observatory.platform.cli.generate_command.InteractiveConfigBuilder.set_editable_observatory_platform")
    @patch("observatory.platform.cli.generate_command.InteractiveConfigBuilder.build")
    def test_generate_terraform_config_interactive(self, mock_build, m_set_edit):
        cmd = GenerateCommand()
        cmd.generate_terraform_config_interactive(
            config_path="path", workflows=["academic-observatory-workflows"], editable=False
        )
        mock_build.assert_called_once_with(
            backend_type=BackendType.terraform, workflows=["academic-observatory-workflows"], editable=False
        )
        m_set_edit.assert_not_called()
        mock_build.reset_mock()

        cmd.generate_terraform_config_interactive(
            config_path="path", workflows=["academic-observatory-workflows"], editable=True
        )
        mock_build.assert_called_once_with(
            backend_type=BackendType.terraform, workflows=["academic-observatory-workflows"], editable=True
        )
        m_set_edit.assert_called_once()

    @patch("observatory.platform.cli.generate_command.InteractiveConfigBuilder.build")
    def test_generate_terraform_api_config_interactive(self, mock_build):
        cmd = GenerateCommand()
        config_path = "config-terraform-api.yaml"
        mock_build.return_value = TerraformAPIConfig()

        with CliRunner().isolated_filesystem():
            cmd.generate_terraform_api_config_interactive(config_path=config_path)
            mock_build.assert_called_once_with(backend_type=BackendType.terraform_api, workflows=None, editable=None)
            self.assertTrue(os.path.exists(config_path))

    @patch("click.confirm")
    def test_generate_workflows_project(self, mock_cli_confirm):
        """Test generate a new workflows project

        :return: None.
        """
        cmd = GenerateCommand()
        runner = CliRunner()
        with runner.isolated_filesystem():
            # Test creating new project
            project_path = os.path.join(os.getcwd(), "my-project")
            package_name = "my_dags"
            cmd.generate_workflows_project(project_path, package_name, "Author Name")

            # Check that all directories and __init__.py files exist
            init_dirs = [
                package_name,
                os.path.join(package_name, "dags"),
                os.path.join(package_name, "database"),
                os.path.join(package_name, "database", "schema"),
                os.path.join(package_name, "workflows"),
                os.path.join(package_name, "workflows", "tests"),
            ]
            for d in init_dirs:
                init_file_path = os.path.join(project_path, d, "__init__.py")
                self.assertTrue(os.path.isfile(init_file_path))
                with open(init_file_path, "a") as f:
                    f.write("test")

            # Check that setup files exist
            self.assertTrue(os.path.isfile(os.path.join(project_path, "setup.cfg")))
            self.assertTrue(os.path.isfile(os.path.join(project_path, "setup.py")))

            # Check that config.py file exists
            self.assertTrue(os.path.isfile(os.path.join(project_path, package_name, "config.py")))

            # Check that all docs files exist
            docs_dirs = ["_build", "_static", "_templates", "workflows"]
            for d in docs_dirs:
                dir_path = os.path.join(project_path, "docs", d)
                self.assertTrue(os.path.isdir(dir_path))

            docs_files = [
                "conf.py",
                "generate_schema_csv.py",
                "index.rst",
                "make.bat",
                "Makefile",
                "requirements.txt",
                "test_generate_schema_csv.py",
            ]
            for f in docs_files:
                file_path = os.path.join(project_path, "docs", f)
                self.assertTrue(os.path.isfile(file_path))

            # Test creating new project when files already exist
            mock_cli_confirm.return_value = "y"
            cmd.generate_workflows_project(project_path, package_name, "Author Name")

            # Check that file hash stayed the same (no new empty init files)
            for d in init_dirs:
                init_file_path = os.path.join(project_path, d, "__init__.py")
                self.assertTrue(os.path.isfile(init_file_path))
                self.assert_file_integrity(init_file_path, "098f6bcd4621d373cade4e832627b4f6", "md5")

            self.assertEqual(3, mock_cli_confirm.call_count)

            # Check that setup files exist
            self.assertTrue(os.path.isfile(os.path.join(project_path, "setup.cfg")))
            self.assertTrue(os.path.isfile(os.path.join(project_path, "setup.py")))

            # Check that config.py file exists
            self.assertTrue(os.path.isfile(os.path.join(project_path, package_name, "config.py")))

    def test_generate_workflow(self):
        """Test generate workflow command and run unit tests that are generated for each of the workflow types.

        :return: None.
        """
        runner = CliRunner()
        with runner.isolated_filesystem():
            cmd = GenerateCommand()

            # Copy actual observatory platform inside isolated filesystem
            observatory_dir = os.path.join(os.getcwd(), "observatory-platform")
            shutil.copytree(module_file_path("observatory.platform"), observatory_dir)

            # Create new workflows project
            project_path = os.path.join(os.getcwd(), "my-project")
            package_name = "unittest_dags"
            result = runner.invoke(generate, ["project", project_path, package_name, "Author Name"], input="n")
            self.assertEqual(0, result.exit_code)

            # Get expected file dirs
            dag_dst_dir = os.path.join(project_path, package_name, "dags")
            workflow_dst_dir = os.path.join(project_path, package_name, "workflows")
            schema_dst_dir = os.path.join(project_path, package_name, "database", "schema")
            test_dst_dir = os.path.join(workflow_dst_dir, "tests")
            doc_dst_dir = os.path.join(project_path, "docs")

            # Test valid workflows
            for workflow_name, workflow_type in [
                ("MyWorkflow", "Workflow"),
                ("MyStream", "StreamTelescope"),
                ("MySnapshot", "SnapshotTelescope"),
                ("MyOrganisation", "OrganisationTelescope"),
            ]:
                cmd.generate_workflow(project_path, package_name, workflow_type, workflow_name)

                # Get expected file paths
                workflow_module = re.sub(r"([A-Z])", r"_\1", workflow_name).lower().strip("_")
                dag_dst_file = os.path.join(dag_dst_dir, f"{workflow_module}.py")
                workflow_dst_file = os.path.join(workflow_dst_dir, f"{workflow_module}.py")
                test_dst_file = os.path.join(test_dst_dir, f"test_{workflow_module}.py")
                index_dst_file = os.path.join(doc_dst_dir, "index.rst")
                doc_dst_file = os.path.join(doc_dst_dir, "workflows", f"{workflow_module}.md")
                schema_dst_file = os.path.join(
                    schema_dst_dir, f"{workflow_module}_{datetime.now().strftime('%Y-%m-%d')}.json"
                )
                identifiers_dst_file = os.path.join(project_path, package_name, "identifiers.py")

                # Check whether all expected files are generated
                for file in [
                    dag_dst_file,
                    workflow_dst_file,
                    test_dst_file,
                    index_dst_file,
                    doc_dst_file,
                    schema_dst_file,
                ]:
                    self.assertTrue(os.path.exists(file))

                # Check if identifiers file exists
                if workflow_type == "OrganisationTelescope":
                    self.assertTrue(os.path.exists(identifiers_dst_file))

                # Check whether the template files do not contain any errors by running the created test files
                proc = subprocess.run(
                    [sys.executable, "-m", "unittest", test_dst_file],
                    env=dict(os.environ, PYTHONPATH=project_path),
                    capture_output=True,
                )
                self.assertEqual("OK", proc.stderr.decode().splitlines()[-1])

            # Test that identifiers file is only created if it does not exist
            cmd.generate_workflow(project_path, package_name, "OrganisationTelescope", "MyOrganisation2")
            self.assert_file_integrity(identifiers_dst_file, "cb7dbb7978aeaf77dfdcad451a065680", "md5")

            # Test invalid workflow type
            with self.assertRaises(Exception):
                cmd.generate_workflow(project_path, package_name, "Invalid", "MyWorkflow")

    @patch("click.confirm")
    def test_write_rendered_template(self, mock_click_confirm):
        """Test writing a rendered template file, only overwrite when file exists if confirmed by user

        :param mock_click_confirm: Mock the click.confirm user confirmation
        :return: None.
        """
        with CliRunner().isolated_filesystem():
            # Create file to test function when file already exists
            file_path = "test.txt"
            with open(file_path, "w") as f:
                f.write("test")
            self.assert_file_integrity(file_path, "098f6bcd4621d373cade4e832627b4f6", "md5")

            mock_click_confirm.return_value = False
            write_rendered_template(file_path, template="some text", file_type="test")
            # Assert that file content stays the same ('test')
            self.assert_file_integrity(file_path, "098f6bcd4621d373cade4e832627b4f6", "md5")

            mock_click_confirm.return_value = True
            write_rendered_template(file_path, template="some text", file_type="test")
            # Assert that file content is now 'some text' instead of 'test'
            self.assert_file_integrity(file_path, "552e21cd4cd9918678e3c1a0df491bc3", "md5")


class TestInteractiveConfigBuilder(unittest.TestCase):
    @patch("observatory.platform.cli.generate_command.InteractiveConfigBuilder.set_editable_observatory_platform")
    @patch("observatory.platform.cli.generate_command.module_file_path")
    @patch("observatory.platform.cli.generate_command.InteractiveConfigBuilder.config_workflows_projects")
    @patch("observatory.platform.cli.generate_command.InteractiveConfigBuilder.config_terraform")
    @patch("observatory.platform.cli.generate_command.InteractiveConfigBuilder.config_observatory")
    @patch("observatory.platform.cli.generate_command.InteractiveConfigBuilder.config_google_cloud")
    @patch("observatory.platform.cli.generate_command.InteractiveConfigBuilder.config_cloud_sql_database")
    @patch("observatory.platform.cli.generate_command.InteractiveConfigBuilder.config_backend")
    @patch("observatory.platform.cli.generate_command.InteractiveConfigBuilder.config_api_type")
    @patch("observatory.platform.cli.generate_command.InteractiveConfigBuilder.config_api")
    @patch("observatory.platform.cli.generate_command.InteractiveConfigBuilder.config_airflow_worker_vm")
    @patch("observatory.platform.cli.generate_command.InteractiveConfigBuilder.config_airflow_variables")
    @patch("observatory.platform.cli.generate_command.InteractiveConfigBuilder.config_airflow_main_vm")
    @patch("observatory.platform.cli.generate_command.InteractiveConfigBuilder.config_airflow_connections")
    def test_build(
        self,
        m_airflow_connections,
        m_airflow_main_vm,
        m_airflow_variables,
        m_airflow_worker_m,
        m_api,
        m_api_type,
        m_backend,
        m_cloud_sql_database,
        m_google_cloud,
        m_observatory,
        m_terraform,
        m_workflows_projects,
        m_module_file_path,
        m_set_editable,
    ):
        def mock_mfp(*arg, **kwargs):
            if arg[0] == "academic_observatory_workflows.dags":
                return "/ao_workflows/path"
            else:
                return "/oaebu_workflows/path"

        m_module_file_path.side_effect = mock_mfp
        methods_all = [m_backend, m_terraform, m_google_cloud]
        methods_terraform_local = [m_observatory, m_airflow_connections, m_airflow_variables, m_workflows_projects]
        methods_terraform = [m_cloud_sql_database, m_airflow_main_vm, m_airflow_worker_m]
        methods_terraform_api = [m_api, m_api_type]
        methods_editable = [m_set_editable]

        def reset_methods():
            for m in (
                methods_all + methods_terraform_local + methods_terraform + methods_terraform_api + methods_editable
            ):
                m.reset_mock()

        # Test with local backend type and no workflows
        config = InteractiveConfigBuilder.build(backend_type=BackendType.local, workflows=[], editable=False)
        self.assertTrue(isinstance(config, ObservatoryConfig))
        self.assertListEqual([], config.workflows_projects.workflows_projects)
        for method in methods_all + methods_terraform_local:
            method.assert_called_once_with(config)

        # Test with local backend type and given workflows
        reset_methods()
        config = InteractiveConfigBuilder.build(
            backend_type=BackendType.local,
            workflows=["academic-observatory-workflows", "oaebu-workflows"],
            editable=False,
        )
        self.assertTrue(isinstance(config, ObservatoryConfig))
        self.assertListEqual(
            [DefaultWorkflowsProject.academic_observatory_workflows(), DefaultWorkflowsProject.oaebu_workflows()],
            config.workflows_projects.workflows_projects,
        )
        for method in methods_all + methods_terraform_local:
            method.assert_called_once_with(config)

        # Test with local backend type and editable
        reset_methods()
        config = InteractiveConfigBuilder.build(backend_type=BackendType.local, workflows=[], editable=True)
        self.assertTrue(isinstance(config, ObservatoryConfig))
        for method in methods_all + methods_terraform_local + methods_editable:
            method.assert_called_once_with(config)

        # Test with terraform backend and no workflows
        reset_methods()
        config = InteractiveConfigBuilder.build(backend_type=BackendType.terraform, workflows=[], editable=False)
        self.assertTrue(isinstance(config, TerraformConfig))
        self.assertListEqual([], config.workflows_projects.workflows_projects)
        for method in methods_all + methods_terraform_local + methods_terraform:
            method.assert_called_once_with(config)

        # Test with terraform backend and given workflows
        reset_methods()
        config = InteractiveConfigBuilder.build(
            backend_type=BackendType.terraform,
            workflows=["academic-observatory-workflows", "oaebu-workflows"],
            editable=False,
        )
        self.assertTrue(isinstance(config, TerraformConfig))
        self.assertListEqual(
            [DefaultWorkflowsProject.academic_observatory_workflows(), DefaultWorkflowsProject.oaebu_workflows()],
            config.workflows_projects.workflows_projects,
        )
        for method in methods_all + methods_terraform_local + methods_terraform:
            method.assert_called_once_with(config)

        # Test with terraform backend type and editable
        reset_methods()
        config = InteractiveConfigBuilder.build(backend_type=BackendType.terraform, workflows=[], editable=True)
        self.assertTrue(isinstance(config, TerraformConfig))
        for method in methods_all + methods_terraform_local + methods_terraform + methods_editable:
            method.assert_called_once_with(config)

        # Test with terraform api backend
        reset_methods()
        config = InteractiveConfigBuilder.build(backend_type=BackendType.terraform_api, workflows=None, editable=None)
        self.assertTrue(isinstance(config, TerraformAPIConfig))
        for method in methods_all + methods_terraform_api:
            method.assert_called_once_with(config)

    @patch("observatory.platform.cli.generate_command.click.prompt")
    def test_config_backend(self, m_prompt):
        m_prompt.return_value = "staging"

        config = ObservatoryConfig()
        expected = Backend(type=BackendType.local, environment=Environment.staging)
        InteractiveConfigBuilder.config_backend(config=config)
        self.assertEqual(config.backend.type, expected.type)
        self.assertEqual(config.backend.environment, expected.environment)

        config = TerraformConfig()
        expected = Backend(type=BackendType.terraform, environment=Environment.staging)
        InteractiveConfigBuilder.config_backend(config=config)
        self.assertEqual(config.backend.type, expected.type)
        self.assertEqual(config.backend.environment, expected.environment)

    @patch("observatory.platform.cli.generate_command.click.prompt")
    @patch("observatory.platform.cli.generate_command.click.confirm")
    def test_config_observatory_filled_keys(self, m_confirm, m_prompt):
        observatory = Observatory(
            airflow_fernet_key="IWt5jFGSw2MD1shTdwzLPTFO16G8iEAU3A6mGo_vJTY=",
            airflow_secret_key=("a" * 16),
            airflow_ui_user_email="email@email",
            airflow_ui_user_password="pass",
            observatory_home="/",
            postgres_password="pass",
            redis_port=111,
            flower_ui_port=53,
            airflow_ui_port=64,
            elastic_port=343,
            kibana_port=143,
            docker_network_name="raefd",
            docker_compose_project_name="proj",
        )

        # Answer to questions
        m_prompt.side_effect = [
            # observatory.package_type,
            observatory.airflow_fernet_key,
            observatory.airflow_secret_key,
            observatory.airflow_ui_user_email,
            observatory.airflow_ui_user_password,
            observatory.observatory_home,
            observatory.postgres_password,
            observatory.redis_port,
            observatory.flower_ui_port,
            observatory.airflow_ui_port,
            observatory.elastic_port,
            observatory.kibana_port,
            observatory.docker_network_name,
            observatory.docker_network_is_external,
            observatory.docker_compose_project_name,
        ]
        m_confirm.return_value = True

        config = ObservatoryConfig()
        InteractiveConfigBuilder.config_observatory(config=config)
        self.assertEqual(config.observatory, observatory)

    @patch("observatory.platform.cli.generate_command.click.prompt")
    @patch("observatory.platform.cli.generate_command.click.confirm")
    def test_config_observatory_empty_keys(self, m_confirm, m_prompt):
        observatory = Observatory(
            airflow_fernet_key="",
            airflow_secret_key="",
            airflow_ui_user_email="email@email",
            airflow_ui_user_password="pass",
            observatory_home="/",
            postgres_password="pass",
            redis_port=111,
            flower_ui_port=53,
            airflow_ui_port=64,
            elastic_port=343,
            kibana_port=143,
            docker_network_name="raefd",
            docker_compose_project_name="proj",
        )

        # Answer to questions
        m_prompt.side_effect = [
            # observatory.package_type,
            observatory.airflow_fernet_key,
            observatory.airflow_secret_key,
            observatory.airflow_ui_user_email,
            observatory.airflow_ui_user_password,
            observatory.observatory_home,
            observatory.postgres_password,
            observatory.redis_port,
            observatory.flower_ui_port,
            observatory.airflow_ui_port,
            observatory.elastic_port,
            observatory.kibana_port,
            observatory.docker_network_name,
            observatory.docker_network_is_external,
            observatory.docker_compose_project_name,
        ]
        m_confirm.return_value = True
        config = ObservatoryConfig()
        InteractiveConfigBuilder.config_observatory(config=config)
        self.assertTrue(len(config.observatory.airflow_fernet_key) > 0)
        self.assertTrue(len(config.observatory.airflow_secret_key) > 0)

        observatory.airflow_fernet_key = config.observatory.airflow_fernet_key
        observatory.airflow_secret_key = config.observatory.airflow_secret_key
        self.assertEqual(config.observatory, observatory)

    @patch("observatory.platform.cli.generate_command.click.prompt")
    @patch("observatory.platform.cli.generate_command.click.confirm")
    def test_config_observatory(self, m_confirm, m_prompt):
        observatory = Observatory(
            airflow_fernet_key="",
            airflow_secret_key="",
            airflow_ui_user_email="email@email",
            airflow_ui_user_password="pass",
            observatory_home="/",
            postgres_password="pass",
            redis_port=111,
            flower_ui_port=53,
            airflow_ui_port=64,
            elastic_port=343,
            kibana_port=143,
            docker_network_name="raefd",
            docker_compose_project_name="proj",
            package_type="pypi",
        )

        # Answer to questions
        m_prompt.side_effect = [
            # "pypi",
            observatory.airflow_fernet_key,
            observatory.airflow_secret_key,
            observatory.airflow_ui_user_email,
            observatory.airflow_ui_user_password,
            observatory.observatory_home,
            observatory.postgres_password,
            observatory.redis_port,
            observatory.flower_ui_port,
            observatory.airflow_ui_port,
            observatory.elastic_port,
            observatory.kibana_port,
            observatory.docker_network_name,
            observatory.docker_network_is_external,
            observatory.docker_compose_project_name,
        ]
        m_confirm.return_value = True

        config = ObservatoryConfig()
        InteractiveConfigBuilder.config_observatory(config=config)
        self.assertTrue(len(config.observatory.airflow_fernet_key) > 0)
        self.assertTrue(len(config.observatory.airflow_secret_key) > 0)

        observatory.airflow_fernet_key = config.observatory.airflow_fernet_key
        observatory.airflow_secret_key = config.observatory.airflow_secret_key
        self.assertEqual(config.observatory, observatory)

    @patch("observatory.platform.cli.generate_command.click.confirm")
    def test_config_google_cloud_local_no_config(self, m_confirm):
        config = ObservatoryConfig()
        m_confirm.return_value = False
        InteractiveConfigBuilder.config_google_cloud(config)
        self.assertEqual(config.google_cloud, None)

    @patch("observatory.platform.cli.generate_command.click.confirm")
    @patch("observatory.platform.cli.generate_command.click.prompt")
    def test_config_google_cloud_local_config(self, m_prompt, m_confirm):
        m_confirm.return_value = True

        google_cloud = GoogleCloud(
            project_id="proj",
            credentials="/tmp",
            region=None,
            zone=None,
            data_location="us",
            buckets=[
                CloudStorageBucket(id="download_bucket", name="download"),
                CloudStorageBucket(id="transform_bucket", name="transform"),
            ],
        )

        # Answer to questions
        m_prompt.side_effect = [
            google_cloud.project_id,
            google_cloud.credentials,
            google_cloud.data_location,
            google_cloud.buckets[0].name,
            google_cloud.buckets[1].name,
        ]

        config = ObservatoryConfig()
        InteractiveConfigBuilder.config_google_cloud(config)

        self.assertEqual(google_cloud, config.google_cloud)

    @patch("observatory.platform.cli.generate_command.click.prompt")
    def test_config_google_cloud_terraform_config(self, m_prompt):
        google_cloud = GoogleCloud(
            project_id="proj", credentials="/tmp", data_location="us", region="us-west2", zone="us-west1-b", buckets=[]
        )

        # Answer to questions
        m_prompt.side_effect = [
            google_cloud.project_id,
            google_cloud.credentials,
            google_cloud.data_location,
            google_cloud.region,
            google_cloud.zone,
        ]

        config = TerraformConfig()
        InteractiveConfigBuilder.config_google_cloud(config)

        self.assertEqual(google_cloud, config.google_cloud)

    @patch("observatory.platform.cli.generate_command.click.confirm")
    def test_config_terraform_local_no_config(self, m_confirm):
        m_confirm.return_value = False

        config = ObservatoryConfig()
        InteractiveConfigBuilder.config_terraform(config)
        self.assertEqual(config.terraform, None)

    @patch("observatory.platform.cli.generate_command.click.confirm")
    @patch("observatory.platform.cli.generate_command.click.prompt")
    def test_config_terraform_local_config(self, m_prompt, m_confirm):
        m_confirm.return_value = True
        m_prompt.side_effect = ["myorg", ""]

        config = ObservatoryConfig()
        terraform = Terraform(organization="myorg")
        InteractiveConfigBuilder.config_terraform(config)
        self.assertEqual(terraform, config.terraform)

        config = ObservatoryConfig()
        terraform = Terraform(organization="")
        InteractiveConfigBuilder.config_terraform(config)
        self.assertEqual(terraform, config.terraform)

    @patch("observatory.platform.cli.generate_command.click.prompt")
    def test_config_terraform_terraform_config(self, m_prompt):
        m_prompt.return_value = "myorg"
        terraform = Terraform(organization="myorg")

        config = TerraformConfig()
        InteractiveConfigBuilder.config_terraform(config)
        self.assertEqual(config.terraform, terraform)

    @patch("observatory.platform.cli.generate_command.click.confirm")
    def test_config_airflow_connections_none(self, m_confirm):
        m_confirm.return_value = False
        config = ObservatoryConfig()
        InteractiveConfigBuilder.config_airflow_connections(config)
        self.assertEqual(None, config.airflow_connections)

    @patch("observatory.platform.cli.generate_command.click.prompt")
    @patch("observatory.platform.cli.generate_command.click.confirm")
    def test_config_airflow_connections_add(self, m_confirm, m_prompt):
        m_confirm.side_effect = [True, True, False]

        expected_conns = [
            AirflowConnection(name="con1", value="val1"),
            AirflowConnection(name="con2", value="val2"),
        ]

        m_prompt.side_effect = [
            expected_conns[0].name,
            expected_conns[0].value,
            expected_conns[1].name,
            expected_conns[1].value,
        ]

        config = ObservatoryConfig()
        InteractiveConfigBuilder.config_airflow_connections(config)
        self.assertEqual(config.airflow_connections.airflow_connections, expected_conns)

    @patch("observatory.platform.cli.generate_command.click.confirm")
    def test_config_airflow_variables_none(self, m_confirm):
        m_confirm.return_value = False
        config = ObservatoryConfig()
        InteractiveConfigBuilder.config_airflow_variables(config)
        self.assertEqual(None, config.airflow_variables)

    @patch("observatory.platform.cli.generate_command.click.prompt")
    @patch("observatory.platform.cli.generate_command.click.confirm")
    def test_config_airflow_variables_add(self, m_confirm, m_prompt):
        m_confirm.side_effect = [True, True, False]

        expected_variables = [
            AirflowVariable(name="var1", value="val1"),
            AirflowVariable(name="var2", value="val2"),
        ]

        m_prompt.side_effect = [
            expected_variables[0].name,
            expected_variables[0].value,
            expected_variables[1].name,
            expected_variables[1].value,
        ]

        config = ObservatoryConfig()
        InteractiveConfigBuilder.config_airflow_variables(config)
        self.assertEqual(config.airflow_variables.airflow_variables, expected_variables)

    @patch("observatory.platform.cli.generate_command.click.confirm")
    def test_config_workflows_projects_none(self, m_confirm):
        m_confirm.return_value = False
        config = ObservatoryConfig()
        InteractiveConfigBuilder.config_workflows_projects(config)
        self.assertEqual(None, config.workflows_projects)

    @patch("observatory.platform.cli.generate_command.click.prompt")
    @patch("observatory.platform.cli.generate_command.click.confirm")
    def test_config_workflows_projects_add(self, m_confirm, m_prompt):
        m_confirm.side_effect = [True, True, False] * 2

        expected_projects = [
            WorkflowsProject(
                package_name="pack1",
                package="/tmp",
                package_type="editable",
                dags_module="something",
            ),
            WorkflowsProject(
                package_name="pack2",
                package="/tmp",
                package_type="editable",
                dags_module="else",
            ),
        ]

        m_prompt.side_effect = [
            expected_projects[0].package_name,
            expected_projects[0].package,
            expected_projects[0].package_type,
            expected_projects[0].dags_module,
            expected_projects[1].package_name,
            expected_projects[1].package,
            expected_projects[1].package_type,
            expected_projects[1].dags_module,
        ] * 2

        # Test when no workflows_projects already are given
        config = ObservatoryConfig()
        InteractiveConfigBuilder.config_workflows_projects(config)
        self.assertListEqual(expected_projects, config.workflows_projects.workflows_projects)

        # Test when workflows_projects are given
        config = ObservatoryConfig(workflows_projects=WorkflowsProjects())
        InteractiveConfigBuilder.config_workflows_projects(config)
        self.assertListEqual([WorkflowsProject()] + expected_projects, config.workflows_projects.workflows_projects)

    @patch("observatory.platform.cli.generate_command.click.prompt")
    def test_config_cloud_sql_database(self, m_prompt):
        setting = CloudSqlDatabase(tier="something", backup_start_time="12:00")

        m_prompt.side_effect = [
            setting.tier,
            setting.backup_start_time,
        ]

        config = TerraformConfig()
        InteractiveConfigBuilder.config_cloud_sql_database(config)

        self.assertEqual(config.cloud_sql_database, setting)

    @patch("observatory.platform.cli.generate_command.click.prompt")
    @patch("observatory.platform.cli.generate_command.click.confirm")
    def test_config_airflow_main_vm(self, m_confirm, m_prompt):
        create = True
        m_confirm.return_value = create

        vm = AirflowMainVm(
            machine_type="n2-standard-2",
            disk_size=1,
            disk_type="pd-ssd",
            create=create,
        )

        m_prompt.side_effect = [
            vm.machine_type,
            vm.disk_size,
            vm.disk_type,
            vm.create,
        ]

        config = TerraformConfig()
        InteractiveConfigBuilder.config_airflow_main_vm(config)
        self.assertEqual(config.airflow_main_vm, vm)

    @patch("observatory.platform.cli.generate_command.click.prompt")
    @patch("observatory.platform.cli.generate_command.click.confirm")
    def test_config_airflow_worker_vm(self, m_confirm, m_prompt):
        create = False
        m_confirm.return_value = create

        vm = AirflowWorkerVm(
            machine_type="n2-standard-2",
            disk_size=1,
            disk_type="pd-ssd",
            create=create,
        )

        m_prompt.side_effect = [
            vm.machine_type,
            vm.disk_size,
            vm.disk_type,
            vm.create,
        ]

        config = TerraformConfig()
        InteractiveConfigBuilder.config_airflow_worker_vm(config)
        self.assertEqual(config.airflow_worker_vm, vm)

    @patch("observatory.platform.cli.generate_command.click.prompt")
    def test_config_api(self, m_prompt):

        api = Api()

        m_prompt.side_effect = [
            api.name,
            api.package,
            api.domain_name,
            api.subdomain,
            api.image_tag,
            api.api_key,
            api.session_secret_key,
        ]

        config = TerraformAPIConfig()
        InteractiveConfigBuilder.config_api(config)
        self.assertEqual(config.api, api)

    @patch("observatory.platform.cli.generate_command.click.prompt")
    def test_config_api_type(self, m_prompt):
        # Test with observatory_api type
        api_type = ApiType(type=ApiTypes.observatory_api)
        m_prompt.return_value = api_type.type.value
        config = TerraformAPIConfig()

        InteractiveConfigBuilder.config_api_type(config)
        self.assertEqual(config.api_type, api_type)

        # Test with data_api type
        m_prompt.reset_mock()
        api_type = ApiType()
        m_prompt.side_effect = [api_type.type, api_type.elasticsearch_host, api_type.elasticsearch_api_key]
        config = TerraformAPIConfig()

        InteractiveConfigBuilder.config_api_type(config)
        self.assertEqual(config.api_type, api_type)


class TestFernetKeyParamType(unittest.TestCase):
    def test_fernet_key_convert_fail(self):
        ctype = FernetKeyType()
        self.assertTrue(hasattr(ctype, "name"))
        self.assertRaises(click.exceptions.BadParameter, ctype.convert, "badkey")

    def test_fernet_key_convert_succeed(self):
        ctype = FernetKeyType()
        self.assertTrue(hasattr(ctype, "name"))
        key = "2a-Wxx5CZdb7wm_T6OailRtUilT7gajYTmPxoUvhVfM="
        result = ctype.convert(key)
        self.assertEqual(result, key)


class TestSecretKeyParamType(unittest.TestCase):
    def test_secret_key_convert_fail(self):
        ctype = FlaskSecretKeyType()
        self.assertTrue(hasattr(ctype, "name"))
        self.assertRaises(click.exceptions.BadParameter, ctype.convert, "badkey")

    def test_secret_key_convert_succeed(self):
        ctype = FlaskSecretKeyType()
        self.assertTrue(hasattr(ctype, "name"))
        key = "a" * 16
        result = ctype.convert(key)
        self.assertEqual(result, key)
