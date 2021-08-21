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

# Author: James Diprose, Tuan Chien

import os
import unittest
from unittest.mock import Mock, patch

import click
from click.testing import CliRunner
from observatory.platform.cli.cli import generate
from observatory.platform.cli.generate_command import (
    GenerateCommand,
    InteractiveConfigBuilder,
)
from observatory.platform.observatory_config import (
    AirflowConnection,
    AirflowVariable,
    Api,
    Backend,
    BackendType,
    CloudSqlDatabase,
    CloudStorageBucket,
    DagsProject,
    ElasticSearch,
    Environment,
    GoogleCloud,
    Observatory,
    ObservatoryConfig,
    Terraform,
    TerraformConfig,
    VirtualMachine,
    generate_secret_key,
)
from observatory.platform.utils.config_utils import module_file_path


class TestGenerateCommand(unittest.TestCase):
    def test_generate_fernet_key(self):
        cmd = GenerateCommand()

        # Test that keys are random
        num_keys = 100
        random_keys = [cmd.generate_fernet_key() for _ in range(num_keys)]
        self.assertEqual(len(set(random_keys)), num_keys)

        # Test that keys have length of 44
        expected_key_len = 44
        [self.assertEqual(expected_key_len, len(key)) for key in random_keys]

    def test_generate_local_config(self):
        cmd = GenerateCommand()
        config_path = "config.yaml"

        with CliRunner().isolated_filesystem():
            cmd.generate_local_config(config_path)
            self.assertTrue(os.path.exists(config_path))

    def test_generate_terraform_config(self):
        cmd = GenerateCommand()
        config_path = "config-terraform.yaml"

        with CliRunner().isolated_filesystem():
            cmd.generate_terraform_config(config_path)
            self.assertTrue(os.path.exists(config_path))

    @patch("observatory.platform.cli.generate_command.open")
    def test_generate_telescope_telescope(self, mock_open):
        # Cannot do filesystem isolation since we are writing explicit paths.
        runner = CliRunner()
        result = runner.invoke(generate, ["telescope", "Telescope", "MyTestTelescope"])
        self.assertEqual(result.exit_code, 0)
        call_args = mock_open.call_args_list
        dagfile = os.path.basename(call_args[0][0][0])
        telescopefile = os.path.basename(call_args[1][0][0])
        testfile = os.path.basename(call_args[2][0][0])
        docfile = os.path.basename(call_args[3][0][0])
        docindexfile = os.path.basename(call_args[4][0][0])
        self.assertEqual(dagfile, "mytesttelescope.py")
        self.assertEqual(telescopefile, "mytesttelescope.py")
        self.assertEqual(testfile, "test_mytesttelescope.py")
        self.assertEqual(docfile, "mytesttelescope.md")
        self.assertEqual(docindexfile, "index.rst")

        result = runner.invoke(generate, ["telescope", "StreamTelescope", "MyTestTelescope"])
        self.assertEqual(result.exit_code, 0)
        dagfile = os.path.basename(call_args[5][0][0])
        telescopefile = os.path.basename(call_args[6][0][0])
        testfile = os.path.basename(call_args[7][0][0])
        docfile = os.path.basename(call_args[8][0][0])
        docindexfile = os.path.basename(call_args[9][0][0])
        self.assertEqual(dagfile, "mytesttelescope.py")
        self.assertEqual(telescopefile, "mytesttelescope.py")
        self.assertEqual(testfile, "test_mytesttelescope.py")
        self.assertEqual(docfile, "mytesttelescope.md")
        self.assertEqual(docindexfile, "index.rst")

        result = runner.invoke(generate, ["telescope", "SnapshotTelescope", "MyTestTelescope"])
        self.assertEqual(result.exit_code, 0)
        dagfile = os.path.basename(call_args[10][0][0])
        telescopefile = os.path.basename(call_args[11][0][0])
        testfile = os.path.basename(call_args[12][0][0])
        docfile = os.path.basename(call_args[13][0][0])
        docindexfile = os.path.basename(call_args[14][0][0])
        self.assertEqual(dagfile, "mytesttelescope.py")
        self.assertEqual(telescopefile, "mytesttelescope.py")
        self.assertEqual(testfile, "test_mytesttelescope.py")
        self.assertEqual(docfile, "mytesttelescope.md")
        self.assertEqual(docindexfile, "index.rst")

        result = runner.invoke(generate, ["telescope", "unknown", "MyTestTelscope"])
        self.assertEqual(result.exit_code, 1)


class TestInteractiveConfigBuilder(unittest.TestCase):
    @patch("observatory.platform.cli.generate_command.InteractiveConfigBuilder.build")
    def test_generate_local_config_interactive(self, mock_build):
        cmd = GenerateCommand()
        cmd.generate_local_config_interactive(config_path="path", install_odags=True)
        self.assertEqual(mock_build.call_args.kwargs["backend_type"], BackendType.local)
        self.assertEqual(mock_build.call_args.kwargs["install_odags"], True)

    @patch("observatory.platform.cli.generate_command.InteractiveConfigBuilder.build")
    def test_generate_terraform_config_interactive(self, mock_build):
        cmd = GenerateCommand()
        cmd.generate_terraform_config_interactive(config_path="path", install_odags=True)
        self.assertEqual(mock_build.call_args.kwargs["backend_type"], BackendType.terraform)
        self.assertEqual(mock_build.call_args.kwargs["install_odags"], True)

    def test_observatory_dagsproject(self):
        dp = GenerateCommand.observatory_dagsproject()
        self.assertEqual(len(dp), 1)
        self.assertEqual(dp[0].package_name, "observatory-dags")
        self.assertEqual(dp[0].path, module_file_path("observatory.dags", nav_back_steps=-3))
        self.assertEqual(dp[0].dags_module, "observatory.dags.dags")

    @patch("observatory.platform.cli.generate_command.InteractiveConfigBuilder.config_backend")
    @patch("observatory.platform.cli.generate_command.InteractiveConfigBuilder.config_observatory")
    @patch("observatory.platform.cli.generate_command.InteractiveConfigBuilder.config_terraform")
    @patch("observatory.platform.cli.generate_command.InteractiveConfigBuilder.config_google_cloud")
    @patch("observatory.platform.cli.generate_command.InteractiveConfigBuilder.config_airflow_connections")
    @patch("observatory.platform.cli.generate_command.InteractiveConfigBuilder.config_airflow_variables")
    @patch("observatory.platform.cli.generate_command.InteractiveConfigBuilder.config_dags_projects")
    @patch("observatory.platform.cli.generate_command.InteractiveConfigBuilder.config_cloud_sql_database")
    @patch("observatory.platform.cli.generate_command.InteractiveConfigBuilder.config_airflow_main_vm")
    @patch("observatory.platform.cli.generate_command.InteractiveConfigBuilder.config_airflow_worker_vm")
    @patch("observatory.platform.cli.generate_command.InteractiveConfigBuilder.config_elasticsearch")
    @patch("observatory.platform.cli.generate_command.InteractiveConfigBuilder.config_api")
    def test_build(
        self,
        m_backend,
        m_observatory,
        m_terraform,
        m_google_cloud,
        m_airflow_connections,
        m_airflow_variables,
        m_dags_projects,
        m_cloud_sql_database,
        m_airflow_main_vm,
        m_airflow_worker_m,
        m_elasticsearch,
        m_api,
    ):
        local_nodags = InteractiveConfigBuilder.build(backend_type=BackendType.local, install_odags=False)
        self.assertTrue(isinstance(local_nodags, ObservatoryConfig))
        self.assertEqual(len(local_nodags.dags_projects), 0)

        local_dags = InteractiveConfigBuilder.build(backend_type=BackendType.local, install_odags=True)
        self.assertTrue(isinstance(local_dags, ObservatoryConfig))
        self.assertEqual(len(local_dags.dags_projects), 1)
        self.assertEqual(local_dags.dags_projects[0], GenerateCommand.observatory_dagsproject()[0])

        terraform_nodags = InteractiveConfigBuilder.build(backend_type=BackendType.terraform, install_odags=False)
        self.assertTrue(isinstance(terraform_nodags, TerraformConfig))
        self.assertTrue(isinstance(terraform_nodags, ObservatoryConfig))
        self.assertEqual(len(terraform_nodags.dags_projects), 0)

        terraform_dags = InteractiveConfigBuilder.build(backend_type=BackendType.terraform, install_odags=True)
        self.assertTrue(isinstance(terraform_dags, TerraformConfig))
        self.assertEqual(len(terraform_dags.dags_projects), 1)
        self.assertEqual(terraform_dags.dags_projects[0], GenerateCommand.observatory_dagsproject()[0])

        self.assertTrue(m_backend.called)
        self.assertTrue(m_observatory.called)
        self.assertTrue(m_terraform.called)
        self.assertTrue(m_google_cloud.called)
        self.assertTrue(m_airflow_connections.called)
        self.assertTrue(m_airflow_variables.called)
        self.assertTrue(m_dags_projects.called)
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
            airflow_secret_key=("a" * 16,),
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
            observatory.docker_compose_project_name,
        ]

        config = ObservatoryConfig()
        InteractiveConfigBuilder.config_observatory(config)
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
            docker_network_name="raefd",
            docker_compose_project_name="proj",
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
            observatory.docker_network_name,
            observatory.docker_compose_project_name,
        ]

        config = ObservatoryConfig()
        InteractiveConfigBuilder.config_observatory(config)
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
    def test_config_dags_projects_none(self, m_confirm):
        m_confirm.return_value = False

        config = ObservatoryConfig()
        expected_dags = list()
        InteractiveConfigBuilder.config_dags_projects(config)
        self.assertEqual(config.dags_projects, expected_dags)

    @patch("observatory.platform.cli.generate_command.click.prompt")
    @patch("observatory.platform.cli.generate_command.click.confirm")
    def test_config_dags_projects_add(self, m_confirm, m_prompt):
        m_confirm.side_effect = [True, True, False]

        config = ObservatoryConfig()
        expected_dags = [
            DagsProject(
                package_name="pack1",
                path="/tmp",
                dags_module="something",
            ),
            DagsProject(
                package_name="pack2",
                path="/tmp",
                dags_module="else",
            ),
        ]

        m_prompt.side_effect = [
            expected_dags[0].package_name,
            expected_dags[0].path,
            expected_dags[0].dags_module,
            expected_dags[1].package_name,
            expected_dags[1].path,
            expected_dags[1].dags_module,
        ]

        InteractiveConfigBuilder.config_dags_projects(config)
        self.assertEqual(config.dags_projects, expected_dags)

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
        )

        m_prompt.side_effect = [
            api.domain_name,
            api.subdomain,
        ]

        config = TerraformConfig()
        InteractiveConfigBuilder.config_api(config)
        self.assertEqual(config.api, api)
