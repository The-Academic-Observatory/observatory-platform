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
    Api,
    Backend,
    BackendType,
    CloudSqlDatabase,
    CloudStorageBucket,
    ElasticSearch,
    Environment,
    GoogleCloud,
    Observatory,
    ObservatoryConfig,
    Terraform,
    TerraformConfig,
    VirtualMachine,
    WorkflowsProject,
)
from observatory.platform.utils.config_utils import module_file_path
from observatory.platform.utils.proc_utils import stream_process
from observatory.platform.utils.test_utils import ObservatoryTestCase


class TestGenerateCommand(ObservatoryTestCase):
    def test_generate_local_config(self):
        cmd = GenerateCommand()
        config_path = "config.yaml"

        with CliRunner().isolated_filesystem():
            cmd.generate_local_config(config_path, editable=False, workflows=[], oapi=False)
            self.assertTrue(os.path.exists(config_path))

        with CliRunner().isolated_filesystem():
            cmd.generate_local_config(config_path, editable=True, workflows=[], oapi=False)
            self.assertTrue(os.path.exists(config_path))

        with CliRunner().isolated_filesystem():
            cmd.generate_local_config(config_path, editable=False, workflows=[], oapi=True)
            self.assertTrue(os.path.exists(config_path))

        with CliRunner().isolated_filesystem():
            cmd.generate_local_config(config_path, editable=True, workflows=[], oapi=True)
            self.assertTrue(os.path.exists(config_path))

    def test_generate_terraform_config(self):
        cmd = GenerateCommand()
        config_path = "config-terraform.yaml"

        with CliRunner().isolated_filesystem():
            cmd.generate_terraform_config(config_path, editable=False, workflows=[], oapi=False)
            self.assertTrue(os.path.exists(config_path))

        with CliRunner().isolated_filesystem():
            cmd.generate_terraform_config(config_path, editable=True, workflows=[], oapi=False)
            self.assertTrue(os.path.exists(config_path))

        with CliRunner().isolated_filesystem():
            cmd.generate_terraform_config(config_path, editable=False, workflows=[], oapi=True)
            self.assertTrue(os.path.exists(config_path))

        with CliRunner().isolated_filesystem():
            cmd.generate_terraform_config(config_path, editable=True, workflows=[], oapi=True)
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
                print(proc.stderr.decode())
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
    @patch("observatory.platform.cli.generate_command.InteractiveConfigBuilder.build")
    def test_generate_local_config_interactive(self, mock_build, m_set_edit):
        cmd = GenerateCommand()
        cmd.generate_local_config_interactive(
            config_path="path", workflows=["academic-observatory-workflows"], oapi=False, editable=False
        )
        self.assertEqual(mock_build.call_args.kwargs["backend_type"], BackendType.local)
        self.assertEqual(mock_build.call_args.kwargs["workflows"], ["academic-observatory-workflows"])
        self.assertEqual(m_set_edit.call_count, 0)

        cmd.generate_local_config_interactive(
            config_path="path", workflows=["academic-observatory-workflows"], oapi=False, editable=True
        )
        self.assertEqual(mock_build.call_args.kwargs["backend_type"], BackendType.local)
        self.assertEqual(mock_build.call_args.kwargs["workflows"], ["academic-observatory-workflows"])
        self.assertEqual(m_set_edit.call_count, 1)

    @patch("observatory.platform.cli.generate_command.InteractiveConfigBuilder.set_editable_observatory_platform")
    @patch("observatory.platform.cli.generate_command.InteractiveConfigBuilder.build")
    def test_generate_terraform_config_interactive(self, mock_build, m_set_edit):
        cmd = GenerateCommand()
        cmd.generate_terraform_config_interactive(
            config_path="path", workflows=["academic-observatory-workflows"], oapi=False, editable=False
        )
        self.assertEqual(mock_build.call_args.kwargs["backend_type"], BackendType.terraform)
        self.assertEqual(mock_build.call_args.kwargs["workflows"], ["academic-observatory-workflows"])
        self.assertEqual(m_set_edit.call_count, 0)

        cmd.generate_terraform_config_interactive(
            config_path="path", workflows=["academic-observatory-workflows"], oapi=False, editable=True
        )
        self.assertEqual(mock_build.call_args.kwargs["backend_type"], BackendType.terraform)
        self.assertEqual(mock_build.call_args.kwargs["workflows"], ["academic-observatory-workflows"])
        self.assertEqual(m_set_edit.call_count, 1)

    @patch("observatory.platform.cli.generate_command.module_file_path")
    @patch("observatory.platform.cli.generate_command.InteractiveConfigBuilder.config_api")
    @patch("observatory.platform.cli.generate_command.InteractiveConfigBuilder.config_elasticsearch")
    @patch("observatory.platform.cli.generate_command.InteractiveConfigBuilder.config_airflow_worker_vm")
    @patch("observatory.platform.cli.generate_command.InteractiveConfigBuilder.config_airflow_main_vm")
    @patch("observatory.platform.cli.generate_command.InteractiveConfigBuilder.config_cloud_sql_database")
    @patch("observatory.platform.cli.generate_command.InteractiveConfigBuilder.config_workflows_projects")
    @patch("observatory.platform.cli.generate_command.InteractiveConfigBuilder.config_airflow_variables")
    @patch("observatory.platform.cli.generate_command.InteractiveConfigBuilder.config_airflow_connections")
    @patch("observatory.platform.cli.generate_command.InteractiveConfigBuilder.config_google_cloud")
    @patch("observatory.platform.cli.generate_command.InteractiveConfigBuilder.config_terraform")
    @patch("observatory.platform.cli.generate_command.InteractiveConfigBuilder.config_observatory")
    @patch("observatory.platform.cli.generate_command.InteractiveConfigBuilder.config_backend")
    def test_build(
        self,
        m_backend,
        m_observatory,
        m_terraform,
        m_google_cloud,
        m_airflow_connections,
        m_airflow_variables,
        m_workflows_projects,
        m_cloud_sql_database,
        m_airflow_main_vm,
        m_airflow_worker_m,
        m_elasticsearch,
        m_api,
        m_mfp,
    ):
        def mock_mfp(*arg, **kwargs):
            if arg[0] == "academic_observatory_workflows.dags":
                return "/ao_workflows/path"
            else:
                return "oaebu_workflows/path"

        m_mfp.side_effect = mock_mfp
        workflows = []
        local_nodags = InteractiveConfigBuilder.build(
            backend_type=BackendType.local, workflows=workflows, oapi=False, editable=False
        )
        self.assertTrue(isinstance(local_nodags, ObservatoryConfig))
        self.assertEqual(len(local_nodags.workflows_projects), 0)

        workflows = ["academic-observatory-workflows", "oaebu-workflows"]
        local_dags = InteractiveConfigBuilder.build(
            backend_type=BackendType.local, workflows=workflows, oapi=False, editable=False
        )
        self.assertTrue(isinstance(local_dags, ObservatoryConfig))
        self.assertEqual(len(local_dags.workflows_projects), 2)
        self.assertEqual(local_dags.workflows_projects[0], DefaultWorkflowsProject.academic_observatory_workflows())
        self.assertEqual(local_dags.workflows_projects[1], DefaultWorkflowsProject.oaebu_workflows())
        print(local_dags.workflows_projects)

        terraform_nodags = InteractiveConfigBuilder.build(
            backend_type=BackendType.terraform, workflows=[], oapi=False, editable=False
        )
        self.assertTrue(isinstance(terraform_nodags, TerraformConfig))
        self.assertTrue(isinstance(terraform_nodags, ObservatoryConfig))
        self.assertEqual(len(terraform_nodags.workflows_projects), 0)

        terraform_dags = InteractiveConfigBuilder.build(
            backend_type=BackendType.terraform,
            workflows=["academic-observatory-workflows", "oaebu-workflows"],
            oapi=False,
            editable=False,
        )
        self.assertTrue(isinstance(terraform_dags, TerraformConfig))
        self.assertEqual(len(terraform_dags.workflows_projects), 2)
        self.assertEqual(terraform_dags.workflows_projects[0], DefaultWorkflowsProject.academic_observatory_workflows())
        self.assertEqual(terraform_dags.workflows_projects[1], DefaultWorkflowsProject.oaebu_workflows())

        self.assertTrue(m_backend.called)
        self.assertTrue(m_observatory.called)
        self.assertTrue(m_terraform.called)
        self.assertTrue(m_google_cloud.called)
        self.assertTrue(m_airflow_connections.called)
        self.assertTrue(m_airflow_variables.called)
        self.assertTrue(m_workflows_projects.called)
        self.assertTrue(m_cloud_sql_database.called)
        self.assertTrue(m_airflow_main_vm.called)
        self.assertTrue(m_airflow_worker_m.called)
        self.assertTrue(m_elasticsearch.called)
        self.assertTrue(m_api.called)

    @patch("observatory.platform.cli.generate_command.click.prompt")
    def test_config_backend(self, m_prompt):
        m_prompt.return_value = "staging"

        config = ObservatoryConfig()
        expected = Backend(type=BackendType.local, environment=Environment.staging)
        InteractiveConfigBuilder.config_backend(config=config, backend_type=expected.type)
        self.assertEqual(config.backend.type, expected.type)
        self.assertEqual(config.backend.environment, expected.environment)

        config = TerraformConfig()
        expected = Backend(type=BackendType.terraform, environment=Environment.staging)
        InteractiveConfigBuilder.config_backend(config=config, backend_type=expected.type)
        self.assertEqual(config.backend.type, expected.type)
        self.assertEqual(config.backend.environment, expected.environment)

    @patch("observatory.platform.cli.generate_command.click.prompt")
    def test_config_observatory_filled_keys(self, m_prompt):
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
            api_port=123,
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
            observatory.api_port,
            observatory.docker_network_name,
            observatory.docker_network_is_external,
            observatory.docker_compose_project_name,
            "y",
            observatory.api_package,
            observatory.api_package_type,
        ]

        config = ObservatoryConfig()
        InteractiveConfigBuilder.config_observatory(config=config, oapi=False, editable=False)
        self.assertEqual(config.observatory, observatory)

    @patch("observatory.platform.cli.generate_command.click.prompt")
    def test_config_observatory_empty_keys(self, m_prompt):
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
            api_port=123,
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
            observatory.api_port,
            observatory.docker_network_name,
            observatory.docker_network_is_external,
            observatory.docker_compose_project_name,
            "y",
            observatory.api_package,
            observatory.api_package_type,
        ]

        config = ObservatoryConfig()
        InteractiveConfigBuilder.config_observatory(config=config, oapi=False, editable=False)
        self.assertTrue(len(config.observatory.airflow_fernet_key) > 0)
        self.assertTrue(len(config.observatory.airflow_secret_key) > 0)

        observatory.airflow_fernet_key = config.observatory.airflow_fernet_key
        observatory.airflow_secret_key = config.observatory.airflow_secret_key
        self.assertEqual(config.observatory, observatory)

    @patch("observatory.platform.cli.generate_command.click.prompt")
    def test_config_observatory_editable(self, m_prompt):
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
            api_port=123,
            docker_network_name="raefd",
            docker_compose_project_name="proj",
            package=module_file_path("observatory.platform", nav_back_steps=-3),
            package_type="editable",
        )

        # Answer to questions
        m_prompt.side_effect = [
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
            observatory.api_port,
            observatory.docker_network_name,
            observatory.docker_network_is_external,
            observatory.docker_compose_project_name,
            "y",
            observatory.api_package,
            observatory.api_package_type,
        ]

        config = ObservatoryConfig()
        InteractiveConfigBuilder.config_observatory(config=config, oapi=False, editable=True)
        observatory.airflow_fernet_key = config.observatory.airflow_fernet_key
        observatory.airflow_secret_key = config.observatory.airflow_secret_key
        self.assertTrue(config.observatory.package_type, "editable")
        self.assertEqual(config.observatory, observatory)

    @patch("observatory.platform.cli.generate_command.click.prompt")
    def test_config_observatory_oapi(self, m_prompt):
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
            api_port=123,
            docker_network_name="raefd",
            docker_compose_project_name="proj",
            package_type="pypi",
            api_package_type="pypi",
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
            observatory.api_port,
            observatory.docker_network_name,
            observatory.docker_network_is_external,
            observatory.docker_compose_project_name,
            "y",
        ]

        config = ObservatoryConfig()
        InteractiveConfigBuilder.config_observatory(config=config, oapi=True, editable=False)
        self.assertTrue(len(config.observatory.airflow_fernet_key) > 0)
        self.assertTrue(len(config.observatory.airflow_secret_key) > 0)

        observatory.airflow_fernet_key = config.observatory.airflow_fernet_key
        observatory.airflow_secret_key = config.observatory.airflow_secret_key
        self.assertEqual(config.observatory, observatory)

    @patch("observatory.platform.cli.generate_command.click.prompt")
    def test_config_observatory_oapi_editable(self, m_prompt):
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
            api_port=123,
            docker_network_name="raefd",
            docker_compose_project_name="proj",
            package_type="editable",
            api_package_type="editable",
            package=module_file_path("observatory.platform", nav_back_steps=-3),
            api_package=module_file_path("observatory.api", nav_back_steps=-3),
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
            observatory.api_port,
            observatory.docker_network_name,
            observatory.docker_network_is_external,
            observatory.docker_compose_project_name,
            "y",
        ]

        config = ObservatoryConfig()
        InteractiveConfigBuilder.config_observatory(config=config, oapi=True, editable=True)
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

        self.assertEqual(config.google_cloud, google_cloud)

    @patch("observatory.platform.cli.generate_command.click.prompt")
    def test_config_google_cloud_terraform_config(self, m_prompt):
        project_id = "proj"
        credentials = "/tmp"
        data_location = "us"
        region = "us-west2"
        zone = "us-west1-b"

        google_cloud = GoogleCloud(
            project_id="proj",
            credentials="/tmp",
            data_location="us",
            region="us-west2",
            zone="us-west1-b",
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

        self.assertEqual(config.google_cloud, google_cloud)

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
        terraform = Terraform(organization="myorg")

        config = ObservatoryConfig()
        InteractiveConfigBuilder.config_terraform(config)
        self.assertEqual(config.terraform, terraform)

        config = ObservatoryConfig()
        InteractiveConfigBuilder.config_terraform(config)
        self.assertEqual(config.terraform, None)

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
        self.assertEqual(len(config.airflow_connections), 0)

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
        self.assertEqual(config.airflow_connections, expected_conns)

    @patch("observatory.platform.cli.generate_command.click.confirm")
    def test_config_airflow_variables_none(self, m_confirm):
        m_confirm.return_value = False
        config = ObservatoryConfig()
        InteractiveConfigBuilder.config_airflow_variables(config)
        self.assertEqual(len(config.airflow_variables), 0)

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
        self.assertEqual(config.airflow_variables, expected_variables)

    @patch("observatory.platform.cli.generate_command.click.confirm")
    def test_config_workflows_projects_none(self, m_confirm):
        m_confirm.return_value = False
        config = ObservatoryConfig()
        expected_dags = list()
        InteractiveConfigBuilder.config_workflows_projects(config)
        self.assertEqual(config.workflows_projects, expected_dags)

    @patch("observatory.platform.cli.generate_command.click.prompt")
    @patch("observatory.platform.cli.generate_command.click.confirm")
    def test_config_workflows_projects_add(self, m_confirm, m_prompt):
        m_confirm.side_effect = [True, True, False]

        config = ObservatoryConfig()
        expected_dags = [
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
            expected_dags[0].package_name,
            expected_dags[0].package,
            expected_dags[0].package_type,
            expected_dags[0].dags_module,
            expected_dags[1].package_name,
            expected_dags[1].package,
            expected_dags[1].package_type,
            expected_dags[1].dags_module,
        ]

        InteractiveConfigBuilder.config_workflows_projects(config)
        self.assertEqual(config.workflows_projects, expected_dags)

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

        vm = VirtualMachine(
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

        vm = VirtualMachine(
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
    def test_config_elasticsearch(self, m_prompt):
        es = ElasticSearch(
            host="https://host:port",
            api_key="mykey",
        )

        m_prompt.side_effect = [
            es.host,
            es.api_key,
        ]

        config = TerraformConfig()
        InteractiveConfigBuilder.config_elasticsearch(config)
        self.assertEqual(config.elasticsearch, es)

    @patch("observatory.platform.cli.generate_command.click.prompt")
    def test_config_api(self, m_prompt):
        api = Api(
            domain_name="api.something",
            subdomain="project_id",
            api_image="us-docker.pkg.dev/gcp-project-id/observatory-platform/observatory-api:latest",
        )

        m_prompt.side_effect = [
            api.domain_name,
            api.subdomain,
            api.api_image,
            api.er_image,
        ]

        config = TerraformConfig()
        InteractiveConfigBuilder.config_api(config)
        self.assertEqual(config.api, api)


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
