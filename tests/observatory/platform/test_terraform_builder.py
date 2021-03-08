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

import os
import unittest
from unittest.mock import Mock
from unittest.mock import patch

from click.testing import CliRunner

from observatory.platform.observatory_config import save_yaml
from observatory.platform.terraform_builder import TerraformBuilder

class Popen(Mock):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @property
    def returncode(self):
        return 0


class TestTerraformBuilder(unittest.TestCase):

    def setUp(self) -> None:
        self.is_env_local = True

    def set_dirs(self):
        # Make directories
        self.config_path = os.path.abspath('config.yaml')
        self.build_path = os.path.join(os.path.abspath('build'), 'terraform')
        self.dags_path = os.path.abspath('dags')
        self.data_path = os.path.abspath('data')
        self.logs_path = os.path.abspath('logs')
        self.postgres_path = os.path.abspath('postgres')

        os.makedirs(self.build_path, exist_ok=True)
        os.makedirs(self.dags_path, exist_ok=True)
        os.makedirs(self.data_path, exist_ok=True)
        os.makedirs(self.logs_path, exist_ok=True)
        os.makedirs(self.postgres_path, exist_ok=True)

    def make_terraform_builder(self):
        return TerraformBuilder(self.config_path, build_path=self.build_path)

    def save_terraform_config(self, file_path: str):
        credentials_path = os.path.abspath('creds.json')
        open(credentials_path, 'a').close()

        dict_ = {
            'backend': {
                'type': 'terraform',
                'environment': 'develop'
            },
            'airflow': {
                'fernet_key': 'random-fernet-key',
                'secret_key': 'random-secret-key',
                'ui_user_password': 'password',
                'ui_user_email': 'password'
            },
            'terraform': {
                'organization': 'hello world'
            },
            'google_cloud': {
                'project_id': 'my-project',
                'credentials': credentials_path,
                'region': 'us-west1',
                'zone': 'us-west1-c',
                'data_location': 'us'
            },
            'cloud_sql_database': {
                'tier': 'db-custom-2-7680',
                'backup_start_time': '23:00',
                'postgres_password': 'my-password'
            },
            'airflow_main_vm': {
                'machine_type': 'n2-standard-2',
                'disk_size': 1,
                'disk_type': 'pd-ssd',
                'create': True
            },
            'airflow_worker_vm': {
                'machine_type': 'n2-standard-2',
                'disk_size': 1,
                'disk_type': 'pd-standard',
                'create': False
            },
            'airflow_variables': {
                'my-variable-name': 'my-variable-value'
            },
            'airflow_connections': {
                'my-connection': 'http://:my-token-key@'
            },
            'elasticsearch': {
                'host': 'https://address.region.gcp.cloud.es.io:port',
                'api_key': 'API_KEY'
            },
            'api': {
                'domain_name': 'api.custom.domain'
            }
        }

        save_yaml(file_path, dict_)

    def test_is_environment_valid(self):
        with CliRunner().isolated_filesystem():
            self.set_dirs()

            # Environment should be invalid because there is no config.yaml
            cmd = self.make_terraform_builder()
            self.assertFalse(cmd.is_environment_valid)

            # Environment should be valid because there is a config.yaml
            # Assumes that Docker is setup on the system where the tests are run
            self.save_terraform_config(self.config_path)
            cmd = self.make_terraform_builder()
            self.assertTrue(cmd.is_environment_valid)

    @unittest.skip
    def test_packer_exe_path(self):
        """ Test that the path to the Packer executable is found """

        with CliRunner().isolated_filesystem():
            self.set_dirs()
            cmd = self.make_terraform_builder()
            result = cmd.packer_exe_path
            self.assertIsNotNone(result)
            self.assertTrue(result.endswith('packer'))

    def test_build_terraform(self):
        """ Test building of the terraform files """

        with CliRunner().isolated_filesystem():
            self.set_dirs()

            # Save default config file
            self.save_terraform_config(self.config_path)

            # Make observatory files
            cmd = self.make_terraform_builder()
            cmd.build_terraform()

            # Test that the expected Terraform files have been written
            secret_files = [os.path.join('secret', n) for n in ['main.tf', 'outputs.tf', 'variables.tf']]
            vm_files = [os.path.join('vm', n) for n in ['main.tf', 'outputs.tf', 'variables.tf']]
            root_files = ['build.sh', 'main.tf', 'observatory-image.json', 'outputs.tf', 'startup-main.tpl',
                          'startup-worker.tpl', 'variables.tf', 'versions.tf']
            all_files = secret_files + vm_files + root_files

            for file_name in all_files:
                path = os.path.join(self.build_path, 'terraform', file_name)
                self.assertTrue(os.path.isfile(path))

            # Test that the expected Docker files have been written
            build_file_names = ['docker-compose.observatory.yml', 'Dockerfile.observatory', 'elasticsearch.yml',
                                'entrypoint-airflow.sh', 'entrypoint-root.sh', 'requirements.txt']
            for file_name in build_file_names:
                path = os.path.join(self.build_path, 'docker', file_name)
                self.assertTrue(os.path.isfile(path))
                self.assertTrue(os.stat(path).st_size > 0)

    @patch('subprocess.Popen')
    @patch('observatory.platform.terraform_builder.stream_process')
    def test_build_image(self, mock_stream_process, mock_subprocess):
        """ Test building of the observatory platform """

        # Check that the environment variables are set properly for the default config
        with CliRunner().isolated_filesystem():
            mock_subprocess.return_value = Popen()
            mock_stream_process.return_value = ('', '')

            self.set_dirs()

            # Save default config file
            self.save_terraform_config(self.config_path)

            # Make observatory files
            cmd = self.make_terraform_builder()

            # Build the image
            output, error, return_code = cmd.build_image()

            # Assert that the image built
            expected_return_code = 0
            self.assertEqual(expected_return_code, return_code)
