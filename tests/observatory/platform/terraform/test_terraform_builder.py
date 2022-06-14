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
import subprocess
import unittest
from unittest.mock import Mock, patch

from click.testing import CliRunner

from observatory.platform.observatory_config import (
    TerraformConfig,
    Backend,
    BackendType,
    Environment,
    Observatory,
    Terraform,
    GoogleCloud,
    CloudSqlDatabase,
    VirtualMachine,
    AirflowVariable,
    AirflowConnection,
    ElasticSearch,
    Api,
)
from observatory.platform.terraform.terraform_builder import TerraformBuilder
from observatory.platform.utils.proc_utils import stream_process
from observatory.platform.utils.test_utils import module_file_path


class Popen(Mock):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @property
    def returncode(self):
        return 0


class TestTerraformBuilder(unittest.TestCase):
    def setUp(self) -> None:
        self.is_env_local = True
        self.observatory_platform_path = module_file_path("observatory.platform", nav_back_steps=-3)
        self.observatory_api_path = module_file_path("observatory.api", nav_back_steps=-3)

    def get_terraform_config(self, t: str) -> TerraformConfig:
        credentials_path = os.path.abspath("creds.json")

        return TerraformConfig(
            backend=Backend(type=BackendType.terraform, environment=Environment.develop),
            observatory=Observatory(
                package=self.observatory_platform_path,
                package_type="editable",
                airflow_fernet_key="ez2TjBjFXmWhLyVZoZHQRTvBcX2xY7L4A7Wjwgr6SJU=",
                airflow_secret_key="a" * 16,
                airflow_ui_user_password="password",
                airflow_ui_user_email="password",
                postgres_password="my-password",
                observatory_home=t,
                api_package=self.observatory_api_path,
                api_package_type="editable",
            ),
            terraform=Terraform(organization="hello world"),
            google_cloud=GoogleCloud(
                project_id="my-project",
                credentials=credentials_path,
                region="us-west1",
                zone="us-west1-c",
                data_location="us",
            ),
            cloud_sql_database=CloudSqlDatabase(tier="db-custom-2-7680", backup_start_time="23:00"),
            airflow_main_vm=VirtualMachine(machine_type="n2-standard-2", disk_size=1, disk_type="pd-ssd", create=True),
            airflow_worker_vm=VirtualMachine(
                machine_type="n2-standard-2",
                disk_size=1,
                disk_type="pd-standard",
                create=False,
            ),
            airflow_variables=[AirflowVariable(name="my-variable-name", value="my-variable-value")],
            airflow_connections=[AirflowConnection(name="my-connection", value="http://:my-token-key@")],
            elasticsearch=ElasticSearch(host="https://address.region.gcp.cloud.es.io:port", api_key="API_KEY"),
            api=Api(
                domain_name="api.custom.domain",
                subdomain="project_id",
                api_image="us-docker.pkg.dev/gcp-project-id/observatory-platform/observatory-api:latest",
            ),
        )

    def test_is_environment_valid(self):
        with CliRunner().isolated_filesystem() as t:
            config_path = os.path.join(t, "config.yaml")

            # Environment should be valid because there is a config.yaml
            # Assumes that Docker is setup on the system where the tests are run
            cfg = self.get_terraform_config(t)
            cmd = TerraformBuilder(config=cfg)
            self.assertTrue(cmd.is_environment_valid)

    @unittest.skip
    def test_packer_exe_path(self):
        """Test that the path to the Packer executable is found"""

        with CliRunner().isolated_filesystem() as t:
            cfg = self.get_terraform_config(t)
            cmd = TerraformBuilder(config=cfg)
            result = cmd.packer_exe_path
            self.assertIsNotNone(result)
            self.assertTrue(result.endswith("packer"))

    def test_build_terraform(self):
        """Test building of the terraform files"""

        with CliRunner().isolated_filesystem() as t:
            cfg = self.get_terraform_config(t)
            cmd = TerraformBuilder(config=cfg)
            cmd.build_terraform()

            # Test that the expected Terraform files have been written
            secret_files = [os.path.join("secret", n) for n in ["main.tf", "outputs.tf", "variables.tf"]]
            vm_files = [os.path.join("vm", n) for n in ["main.tf", "outputs.tf", "variables.tf"]]
            root_files = [
                "build.sh",
                "main.tf",
                "observatory-image.json",
                "outputs.tf",
                "startup-main.tpl",
                "startup-worker.tpl",
                "variables.tf",
                "versions.tf",
            ]
            all_files = secret_files + vm_files + root_files

            for file_name in all_files:
                path = os.path.join(cmd.build_path, "terraform", file_name)
                self.assertTrue(os.path.isfile(path))

            # Test that expected packages exists
            packages = ["observatory-api", "observatory-platform"]
            for package in packages:
                path = os.path.join(cmd.build_path, "packages", package)
                self.assertTrue(os.path.exists(path))

            # Test that the expected Docker files have been written
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
                path = os.path.join(cmd.build_path, "docker", file_name)
                self.assertTrue(os.path.isfile(path))
                self.assertTrue(os.stat(path).st_size > 0)

    @patch("subprocess.Popen")
    @patch("observatory.platform.terraform.terraform_builder.stream_process")
    def test_build_image(self, mock_stream_process, mock_subprocess):
        """Test building of the observatory platform"""

        # Check that the environment variables are set properly for the default config
        with CliRunner().isolated_filesystem() as t:
            mock_subprocess.return_value = Popen()
            mock_stream_process.return_value = ("", "")

            # Save default config file
            cfg = self.get_terraform_config(t)
            cmd = TerraformBuilder(config=cfg)

            # Build the image
            output, error, return_code = cmd.build_image()

            # Assert that the image built
            expected_return_code = 0
            self.assertEqual(expected_return_code, return_code)

    @patch("subprocess.Popen")
    @patch("observatory.platform.terraform.terraform_builder.stream_process")
    def test_gcloud_activate_service_account(self, mock_stream_process, mock_subprocess):
        """Test activating the gcloud service account"""

        # Check that the environment variables are set properly for the default config
        with CliRunner().isolated_filesystem() as t:
            mock_subprocess.return_value = Popen()
            mock_stream_process.return_value = ("", "")

            # Make observatory files
            cfg = self.get_terraform_config(t)
            cmd = TerraformBuilder(config=cfg)

            # Activate the service account
            output, error, return_code = cmd.gcloud_activate_service_account()

            # Assert that account was activated
            expected_return_code = 0
            self.assertEqual(expected_return_code, return_code)

    @patch("subprocess.Popen")
    @patch("observatory.platform.terraform.terraform_builder.stream_process")
    def test_gcloud_builds_submit(self, mock_stream_process, mock_subprocess):
        """Test gcloud builds submit command"""

        # Check that the environment variables are set properly for the default config
        with CliRunner().isolated_filesystem() as t:
            mock_subprocess.return_value = Popen()
            mock_stream_process.return_value = ("", "")

            cfg = self.get_terraform_config(t)
            cmd = TerraformBuilder(config=cfg)

            # Build the image
            output, error, return_code = cmd.gcloud_builds_submit()

            # Assert that the image built
            expected_return_code = 0
            self.assertEqual(expected_return_code, return_code)

    def test_build_api_image(self):
        """Test building API image using Docker"""

        # Check that the environment variables are set properly for the default config
        with CliRunner().isolated_filesystem() as t:
            cfg = self.get_terraform_config(t)
            cmd = TerraformBuilder(config=cfg)

            args = ["docker", "build", "."]
            print("Executing subprocess:")

            proc: Popen = subprocess.Popen(
                args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd=cmd.api_package_path
            )
            output, error = stream_process(proc, True)

            # Assert that the image built
            expected_return_code = 0
            self.assertEqual(expected_return_code, proc.returncode)
