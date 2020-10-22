# Copyright 2019 Curtin University. All Rights Reserved.
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
import pathlib
import random
import string
import unittest
from typing import List, Dict

import yaml
from click.testing import CliRunner

from observatory.platform.observatory_config import (make_schema, BackendType, ObservatoryConfig, TerraformConfig,
                                                     ObservatoryConfigValidator)


def save_yaml(file_path: str, dict_: Dict):
    with open(file_path, 'w') as yaml_file:
        yaml.dump(dict_, yaml_file, default_flow_style=False)


class TestObservatoryConfigValidator(unittest.TestCase):
    def setUp(self) -> None:
        self.schema = dict()
        self.schema['google_cloud'] = {
            'required': True,
            'type': 'dict',
            'schema': {
                'credentials': {
                    'required': True,
                    'type': 'string',
                    'google_application_credentials': True
                }
            }
        }

    def test_validate_google_application_credentials(self):
        """ Check if an error occurs for pointing to a file that does not exist when the
        'google_application_credentials' tag is present in the schema. """

        with CliRunner().isolated_filesystem():
            # Make google application credentials
            credentials_file_path = os.path.join(pathlib.Path().absolute(), 'google_application_credentials.json')
            with open(credentials_file_path, 'w') as f:
                f.write('')
            validator = ObservatoryConfigValidator(self.schema)

            # google_application_credentials tag and existing file
            validator.validate({
                'google_cloud': {
                    'credentials': credentials_file_path
                }
            })
            self.assertEqual(len(validator.errors), 0)

            # google_application_credentials tag and non-existing file
            validator.validate({
                'google_cloud': {
                    'credentials': 'missing_file.json'
                }
            })
            self.assertEqual(len(validator.errors), 1)


class TestObservatoryConfig(unittest.TestCase):

    def test_load(self):
        # Test that a minimal configuration works
        dict_ = {
            'backend': {
                'type': 'local',
                'environment': 'develop'
            },
            'airflow': {
                'fernet_key': 'random-fernet-key'
            }
        }

        file_path = 'config-valid-minimal.yaml'
        with CliRunner().isolated_filesystem():
            save_yaml(file_path, dict_)

            config = ObservatoryConfig.load(file_path)
            self.assertIsInstance(config, ObservatoryConfig)
            self.assertTrue(config.is_valid)

        file_path = 'config-valid-typical.yaml'
        with CliRunner().isolated_filesystem():
            credentials_path = os.path.abspath('creds.json')
            open(credentials_path, 'a').close()

            # Test that a typical configuration works
            dict_ = {
                'backend': {
                    'type': 'local',
                    'environment': 'develop'
                },
                'google_cloud': {
                    'project_id': 'my-project-id',
                    'credentials': credentials_path,
                    'data_location': 'us',
                    'buckets': {
                        'download_bucket': 'my-download-bucket-1234',
                        'transform_bucket': 'my-transform-bucket-1234'
                    }
                },
                'airflow': {
                    'fernet_key': 'random-fernet-key'
                },
                'airflow_variables': {
                    'my-variable-name': 'my-variable-value'
                },
                'airflow_connections': {
                    'my-connection': 'http://:my-token-key@'
                },
                'dags_projects': [
                    {
                        'package_name': 'observatory-dags',
                        'path': '/path/to/dags/project',
                        'dags_module': 'observatory.dags.dags'
                    },
                    {
                        'package_name': 'observatory-dags',
                        'path': '/path/to/dags/project',
                        'dags_module': 'observatory.dags.dags'
                    }
                ]
            }

            save_yaml(file_path, dict_)

            config = ObservatoryConfig.load(file_path)
            self.assertIsInstance(config, ObservatoryConfig)
            self.assertTrue(config.is_valid)

        # Test that an invalid minimal config works
        dict_ = {
            'backend': {
                'type': 'terraform',
                'environment': 'my-env'
            },
            'airflow': {
                'fernet_key': False
            }
        }

        file_path = 'config-invalid-minimal.yaml'
        with CliRunner().isolated_filesystem():
            save_yaml(file_path, dict_)

            config = ObservatoryConfig.load(file_path)
            self.assertIsInstance(config, ObservatoryConfig)
            self.assertFalse(config.is_valid)

        # Test that an invalid typical config works
        dict_ = {
            'backend': {
                'type': 'terraform',
                'environment': 'my-env'
            },
            'google_cloud': {
                'project_id': 'my-project-id',
                'credentials': '/path/to/creds.json',
                'data_location': 1,
                'buckets': {
                    'download_bucket': 'my-download-bucket-1234',
                    'transform_bucket': 'my-transform-bucket-1234'
                }
            },
            'airflow': {
                'fernet_key': 1
            },
            'airflow_variables': {
                'my-variable-name': 1
            },
            'airflow_connections': {
                'my-connection': 'token-key'
            },
            'dags_projects': [
                {
                    'package_name': 'observatory-dags',
                    'path': '/path/to/dags/project',
                    'dags_module': False
                },
                {
                    'package_name': 'observatory-dags',
                    'dags_module': 'observatory.dags.dags'
                }
            ]
        }

        file_path = 'config-invalid-typical.yaml'
        with CliRunner().isolated_filesystem():
            save_yaml(file_path, dict_)

            config = ObservatoryConfig.load(file_path)
            self.assertIsInstance(config, ObservatoryConfig)
            self.assertFalse(config.is_valid)


class TestTerraformConfig(unittest.TestCase):

    def test_load(self):
        # Test that a minimal configuration works

        file_path = 'config-valid-typical.yaml'
        with CliRunner().isolated_filesystem():
            credentials_path = os.path.abspath('creds.json')
            open(credentials_path, 'a').close()

            dict_ = {
                'backend': {
                    'type': 'terraform',
                    'environment': 'develop'
                },
                'airflow': {
                    'fernet_key': 'random-fernet-key',
                    'ui_user_password': 'password',
                    'ui_user_email': 'password'
                },
                'terraform': {
                    'organization': 'hello world',
                    'workspace_prefix': 'my-workspaces-'
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
                }
            }

            save_yaml(file_path, dict_)

            config = TerraformConfig.load(file_path)
            self.assertIsInstance(config, TerraformConfig)
            self.assertTrue(config.is_valid)

        file_path = 'config-valid-typical.yaml'
        with CliRunner().isolated_filesystem():
            credentials_path = os.path.abspath('creds.json')
            open(credentials_path, 'a').close()

            # Test that a typical configuration is loaded
            dict_ = {
                'backend': {
                    'type': 'terraform',
                    'environment': 'develop'
                },
                'airflow': {
                    'fernet_key': 'random-fernet-key',
                    'ui_user_password': 'password',
                    'ui_user_email': 'password'
                },
                'terraform': {
                    'organization': 'hello world',
                    'workspace_prefix': 'my-workspaces-'
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
                'dags_projects': [
                    {
                        'package_name': 'observatory-dags',
                        'path': '/path/to/dags/project',
                        'dags_module': 'observatory.dags.dags'
                    },
                    {
                        'package_name': 'observatory-dags',
                        'path': '/path/to/dags/project',
                        'dags_module': 'observatory.dags.dags'
                    }
                ]
            }

            save_yaml(file_path, dict_)

            config = TerraformConfig.load(file_path)
            self.assertIsInstance(config, TerraformConfig)
            self.assertTrue(config.is_valid)

        # Test that an invalid minimal config works
        dict_ = {
            'backend': {
                'type': 'local',
                'environment': 'develop'
            },
            'airflow': {
                'fernet_key': 'random-fernet-key',
                'ui_user_password': 'password',
                'ui_user_email': 'password'
            },
            'terraform': {
                'organization': 'hello world',
                'workspace_prefix': 'my-workspaces-'
            },
            'google_cloud': {
                'project_id': 'my-project',
                'credentials': '/path/to/creds.json',
                'region': 'us-west',
                'zone': 'us-west1',
                'data_location': 'us',
                'buckets': {
                    'download_bucket': 'my-download-bucket-1234',
                    'transform_bucket': 'my-transform-bucket-1234'
                }
            },
            'cloud_sql_database': {
                'tier': 'db-custom-2-7680',
                'backup_start_time': '2300'
            },
            'airflow_main_vm': {
                'machine_type': 'n2-standard-2',
                'disk_size': 0,
                'disk_type': 'disk',
                'create': True
            },
            'airflow_worker_vm': {
                'machine_type': 'n2-standard-2',
                'disk_size': 0,
                'disk_type': 'disk',
                'create': False
            }
        }

        file_path = 'config-invalid-minimal.yaml'
        with CliRunner().isolated_filesystem():
            save_yaml(file_path, dict_)

            config = TerraformConfig.load(file_path)
            self.assertIsInstance(config, TerraformConfig)
            self.assertFalse(config.is_valid)

        # Test that an invalid typical config is loaded
        dict_ = {
            'backend': {
                'type': 'terraform',
                'environment': 'develop'
            },
            'airflow': {
                'fernet_key': 'random-fernet-key',
                'ui_user_password': 'password',
                'ui_user_email': 'password'
            },
            'terraform': {
                'organization': 'hello world',
                'workspace_prefix': 'my-workspaces-'
            },
            'google_cloud': {
                'project_id': 'my-project',
                'credentials': '/path/to/creds.json',
                'region': 'us-west1',
                'zone': 'us-west1-c',
                'data_location': 'us',
                'buckets': {
                    'download_bucket': 'my-download-bucket-1234',
                    'transform_bucket': 'my-transform-bucket-1234'
                }
            },
            'cloud_sql_database': {
                'tier': 'db-custom-2-7680',
                'backup_start_time': '23:00'
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
                'my-variable-name': 1
            },
            'airflow_connections': {
                'my-connection': 'my-token'
            },
            'dags_projects': [
                {
                    'package_name': 'observatory-dags',
                    'path': '/path/to/dags/project',
                    'dags_module': 'observatory.dags.dags'
                }
            ]
        }

        file_path = 'config-invalid-typical.yaml'
        with CliRunner().isolated_filesystem():
            save_yaml(file_path, dict_)

            config = TerraformConfig.load(file_path)
            self.assertIsInstance(config, TerraformConfig)
            self.assertFalse(config.is_valid)


class TestSchema(unittest.TestCase):

    def assert_sub_schema_valid(self, valid_docs: List[Dict], invalid_docs: List[Dict], schema, sub_schema_key,
                                expected_errors):
        validator = ObservatoryConfigValidator()
        sub_schema = dict()
        sub_schema[sub_schema_key] = schema[sub_schema_key]

        # Assert that docs expected to be valid are valid
        for doc in valid_docs:
            is_valid = validator.validate(doc, sub_schema)
            self.assertTrue(is_valid)

        # Assert that docs that are expected to be invalid are invalid
        for doc, error in zip(invalid_docs, expected_errors):
            is_valid = validator.validate(doc, sub_schema)
            self.assertFalse(is_valid)
            self.assertDictEqual(validator.errors, error)

    def assert_schema_keys(self, schema: Dict, contains: List, not_contains: List):
        # Assert that keys are in schema
        for key in contains:
            self.assertTrue(key in schema)

        # Assert that keys aren't in schema
        for key in not_contains:
            self.assertTrue(key not in schema)

    def test_local_schema_keys(self):
        # Test that local schema keys exist and that terraform only keys don't exist
        schema = make_schema(BackendType.local)
        contains = ['backend', 'terraform', 'google_cloud', 'airflow', 'airflow_variables',
                    'airflow_connections', 'dags_projects']
        not_contains = ['cloud_sql_database', 'airflow_main_vm', 'airflow_worker_vm']
        self.assert_schema_keys(schema, contains, not_contains)

    def test_terraform_schema_keys(self):
        # Test that terraform schema keys exist
        schema = make_schema(BackendType.terraform)
        contains = ['backend', 'terraform', 'google_cloud', 'airflow', 'airflow_variables',
                    'airflow_connections', 'dags_projects', 'cloud_sql_database', 'airflow_main_vm',
                    'airflow_worker_vm']
        not_contains = []
        self.assert_schema_keys(schema, contains, not_contains)

    def test_local_schema_backend(self):
        schema = make_schema(BackendType.local)
        schema_key = 'backend'

        valid_docs = [
            {
                'backend': {
                    'type': 'local',
                    'environment': 'develop'
                }
            },
            {
                'backend': {
                    'type': 'local',
                    'environment': 'staging'
                }
            },
            {
                'backend': {
                    'type': 'local',
                    'environment': 'production'
                }
            }
        ]
        invalid_docs = [
            {
                'backend': {
                    'type': 'terraform',
                    'environment': 'hello'
                }
            }
        ]
        expected_errors = [
            {'backend': [{'environment': ['unallowed value hello'], 'type': ['unallowed value terraform']}]}]
        self.assert_sub_schema_valid(valid_docs, invalid_docs, schema, schema_key, expected_errors)

    def test_local_schema_terraform(self):
        schema = make_schema(BackendType.local)
        schema_key = 'terraform'

        valid_docs = [
            {},
            {
                'terraform': {
                    'organization': 'hello world',
                    'workspace_prefix': 'my-workspaces-'
                }
            }
        ]
        invalid_docs = [
            {
                'terraform': {
                    'organization': 0,
                    'workspace_prefix': 1
                }
            },
            {
                'terraform': {
                    'organization': dict(),
                    'workspace_prefix': list()
                }
            },
            {
                'terraform': {
                    'organization': 'hello world'
                }
            },
            {
                'terraform': {
                    'workspace_prefix': 'my-workspaces-'
                }
            }
        ]
        expected_errors = [{'terraform': [
            {'organization': ['must be of string type'], 'workspace_prefix': ['must be of string type']}]},
            {'terraform': [{'organization': ['must be of string type'],
                            'workspace_prefix': ['must be of string type']}]},
            {'terraform': [{'workspace_prefix': ['required field']}]},
            {'terraform': [{'organization': ['required field']}]}]
        self.assert_sub_schema_valid(valid_docs, invalid_docs, schema, schema_key, expected_errors)

    def test_local_schema_google_cloud(self):
        schema = make_schema(BackendType.local)
        schema_key = 'google_cloud'

        with CliRunner().isolated_filesystem():
            credentials_path = os.path.abspath('creds.json')
            open(credentials_path, 'a').close()

            valid_docs = [
                {},
                {
                    'google_cloud': {
                        'project_id': 'my-project',
                        'credentials': credentials_path,
                        'region': 'us-west1',
                        'zone': 'us-west1-c',
                        'data_location': 'us',
                        'buckets': {
                            'download_bucket': 'my-download-bucket-1234',
                            'transform_bucket': 'my-transform-bucket-1234'
                        }
                    }
                }
            ]
            invalid_docs = [
                {
                    'google_cloud': {
                        'project_id': 1,
                        'credentials': '/path/to/creds.json',
                        'region': 'us-west',
                        'zone': 'us-west1',
                        'data_location': list(),
                        'buckets': {
                            1: 2,
                            'download_bucket': list()
                        }
                    }
                }
            ]

            expected_errors = [{'google_cloud': [{'buckets': [
                {1: ['must be of string type', 'must be of string type'],
                 'download_bucket': ['must be of string type']}],
                'credentials': [
                    'the file /path/to/creds.json does not exist. See https://cloud.google.com/docs/authentication/getting-started for instructions on how to create a service account and save the JSON key to your workstation.'],
                'data_location': ['must be of string type'],
                'project_id': ['must be of string type'],
                'region': ["value does not match regex '^\\w+\\-\\w+\\d+$'"],
                'zone': ["value does not match regex '^\\w+\\-\\w+\\d+\\-[a-z]{1}$'"]}]}]
            self.assert_sub_schema_valid(valid_docs, invalid_docs, schema, schema_key, expected_errors)

    def test_local_schema_airflow(self):
        schema = make_schema(BackendType.local)
        schema_key = 'airflow'

        valid_docs = [
            {
                'airflow': {
                    'fernet_key': 'random-fernet-key'
                }
            },
            {
                'airflow': {
                    'fernet_key': 'password',
                    'ui_user_password': 'password',
                    'ui_user_email': 'password'
                }
            }
        ]
        invalid_docs = [
            {},
            {
                'airflow': {
                    'ui_user_password': 'password',
                    'ui_user_email': 'password'
                }
            }
        ]

        expected_errors = [{'airflow': ['required field']},
                           {'airflow': [{'fernet_key': ['required field']}]}]
        self.assert_sub_schema_valid(valid_docs, invalid_docs, schema, schema_key, expected_errors)

    def test_local_schema_airflow_variables(self):
        schema = make_schema(BackendType.local)
        schema_key = 'airflow_variables'

        valid_docs = [
            {},
            {
                'airflow_variables': {
                    'key1': 'value',
                    'key2': 'value'
                }
            }
        ]
        invalid_docs = [
            {
                'airflow_variables': {
                    'key1': 1,
                    1: 'value'
                }
            }
        ]

        expected_errors = [{'airflow_variables': [{1: ['must be of string type'], 'key1': ['must be of string type']}]}]
        self.assert_sub_schema_valid(valid_docs, invalid_docs, schema, schema_key, expected_errors)

    def test_local_schema_dags_projects(self):
        schema = make_schema(BackendType.local)
        schema_key = 'dags_projects'

        valid_docs = [
            {},
            {
                'dags_projects': [
                    {
                        'package_name': 'observatory-dags',
                        'path': '/path/to/dags/project',
                        'dags_module': 'observatory.dags.dags'
                    }
                ]
            }
        ]
        invalid_docs = [
            {
                'dags_projects': [
                    {
                        'package_name': 'observatory-dags',
                        'path': '/path/to/dags/project'
                    }
                ]
            }
        ]

        expected_errors = [{'dags_projects': [{0: [{'dags_module': ['required field']}]}]}]
        self.assert_sub_schema_valid(valid_docs, invalid_docs, schema, schema_key, expected_errors)

    def test_local_schema_airflow_connections(self):
        schema = make_schema(BackendType.local)
        schema_key = 'airflow_connections'

        valid_docs = [
            {},
            {
                'airflow_connections': {
                    'terraform': 'mysql://:terraform-token@',
                    'slack': 'https://:T00000000%2FB00000000%2FXXXXXXXXXXXXXXXXXXXXXXXX@https%3A%2F%2Fhooks.slack.com%2Fservices',
                    'crossref': 'http://myname:mypassword@myhost.com',
                    'mag_releases_table': 'http://myname:mypassword@myhost.com',
                    'mag_snapshots_container': 'http://myname:mypassword@myhost.com'
                }
            }
        ]
        invalid_docs = [
            {
                'airflow_connections': {
                    'key1': 1,
                    1: 'value',
                    'terraform': 'not-a-connection-string'
                }
            }
        ]

        expected_errors = [{'airflow_connections': [
            {1: ['must be of string type', "value does not match regex '\\S*:\\/\\/\\S*:\\S*@\\S*$'"],
             'key1': ['must be of string type'],
             'terraform': ["value does not match regex '\\S*:\\/\\/\\S*:\\S*@\\S*$'"]}]}]
        self.assert_sub_schema_valid(valid_docs, invalid_docs, schema, schema_key, expected_errors)

    def test_terraform_schema_backend(self):
        schema = make_schema(BackendType.terraform)
        schema_key = 'backend'

        valid_docs = [
            {
                'backend': {
                    'type': 'terraform',
                    'environment': 'develop'
                }
            }
        ]
        invalid_docs = [
            {
                'backend': {
                    'type': 'local',
                    'environment': 'develop'
                }
            }
        ]
        expected_errors = [
            {'backend': [{'type': ['unallowed value local']}]}]
        self.assert_sub_schema_valid(valid_docs, invalid_docs, schema, schema_key, expected_errors)

    def test_terraform_schema_terraform(self):
        # Test that terraform is required
        schema = make_schema(BackendType.terraform)
        schema_key = 'terraform'

        valid_docs = [
            {
                'terraform': {
                    'organization': 'hello world',
                    'workspace_prefix': 'my-workspaces-'
                }
            }
        ]
        invalid_docs = [{}]
        expected_errors = [{'terraform': ['required field']}]
        self.assert_sub_schema_valid(valid_docs, invalid_docs, schema, schema_key, expected_errors)

    def test_terraform_schema_google_cloud(self):
        # Test that all google cloud fields are required
        schema = make_schema(BackendType.terraform)
        schema_key = 'google_cloud'

        with CliRunner().isolated_filesystem():
            credentials_path = os.path.abspath('creds.json')
            open(credentials_path, 'a').close()

            valid_docs = [
                {
                    'google_cloud': {
                        'project_id': 'my-project',
                        'credentials': credentials_path,
                        'region': 'us-west1',
                        'zone': 'us-west1-c',
                        'data_location': 'us'
                    }
                }
            ]
            invalid_docs = [
                {},
                {
                    'google_cloud': {}
                }
            ]

            expected_errors = [{'google_cloud': ['required field']},
                               {'google_cloud': [{'credentials': ['required field'],
                                                  'data_location': ['required field'], 'project_id': ['required field'],
                                                  'region': ['required field'], 'zone': ['required field']}]}]
            self.assert_sub_schema_valid(valid_docs, invalid_docs, schema, schema_key, expected_errors)

    def test_terraform_schema_airflow(self):
        # Test that airflow ui password and email required
        schema = make_schema(BackendType.terraform)
        schema_key = 'airflow'

        valid_docs = [
            {
                'airflow': {
                    'fernet_key': 'password',
                    'ui_user_password': 'password',
                    'ui_user_email': 'password'
                }
            }
        ]
        invalid_docs = [
            {},
            {
                'airflow': {}
            }
        ]

        expected_errors = [{'airflow': ['required field']},
                           {'airflow': [{'fernet_key': ['required field'], 'ui_user_email': ['required field'],
                                         'ui_user_password': ['required field']}]}]
        self.assert_sub_schema_valid(valid_docs, invalid_docs, schema, schema_key, expected_errors)

    def test_terraform_schema_database(self):
        # Test database schema
        schema = make_schema(BackendType.terraform)
        schema_key = 'cloud_sql_database'

        valid_docs = [
            {
                'cloud_sql_database': {
                    'tier': 'db-custom-2-7680',
                    'backup_start_time': '23:00',
                    'postgres_password': 'my-password'
                }
            }
        ]
        invalid_docs = [
            {},
            {
                'cloud_sql_database': {}
            },
            {
                'cloud_sql_database': {
                    'tier': 1,
                    'backup_start_time': '2300',
                    'postgres_password': 'my-password'
                }
            }
        ]

        expected_errors = [{'cloud_sql_database': ['required field']},
                           {'cloud_sql_database': [
                               {'backup_start_time': ['required field'], 'tier': ['required field'],
                                'postgres_password': ['required field']}]},
                           {'cloud_sql_database': [
                               {'backup_start_time': ["value does not match regex '^\\d{2}:\\d{2}$'"],
                                'tier': ['must be of string type']}]}]
        self.assert_sub_schema_valid(valid_docs, invalid_docs, schema, schema_key, expected_errors)

    def assert_vm_schema(self, schema_key: str):
        schema = make_schema(BackendType.terraform)
        valid_docs = [
            {
                schema_key: {
                    'machine_type': 'n2-standard-2',
                    'disk_size': 1,
                    'disk_type': 'pd-standard',
                    'create': False
                }
            },
            {
                schema_key: {
                    'machine_type': 'n2-standard-2',
                    'disk_size': 100,
                    'disk_type': 'pd-ssd',
                    'create': True
                }
            }
        ]
        invalid_docs = [
            {},
            {
                schema_key: {}
            },
            {
                schema_key: {
                    'machine_type': 1,
                    'disk_size': 0,
                    'disk_type': 'develop',
                    'create': 'True'
                }
            }
        ]

        expected_errors = [{schema_key: ['required field']},
                           {schema_key: [{'create': ['required field'], 'disk_size': ['required field'],
                                          'disk_type': ['required field'], 'machine_type': ['required field']}]},
                           {schema_key: [{'create': ['must be of boolean type'], 'disk_size': ['min value is 1'],
                                          'disk_type': ['unallowed value develop'],
                                          'machine_type': ['must be of string type']}]}]
        self.assert_sub_schema_valid(valid_docs, invalid_docs, schema, schema_key, expected_errors)

    def test_terraform_schema_vms(self):
        # Test VM schema
        self.assert_vm_schema('airflow_main_vm')
        self.assert_vm_schema('airflow_worker_vm')


def tmp_config_file(dict_: dict) -> str:
    """
    Dumps dict into a yaml file that is saved in a randomly named file. Used to as config file to create
    ObservatoryConfig instance.
    :param dict_: config dict
    :return: path of temporary file
    """
    content = yaml.safe_dump(dict_).replace("'!", "!").replace("':", ":")
    file_name = ''.join(random.choices(string.ascii_lowercase, k=10))
    with open(file_name, 'w') as f:
        f.write(content)
    return file_name

#
# class TestObservatoryConfigValidator(unittest.TestCase):
#     def setUp(self) -> None:
#         self.schema = {
#             'google_application_credentials': {
#                 'required': False,
#                 'type': 'string',
#                 'google_application_credentials': True
#             },
#             'crossref': {
#                 'required': False,
#                 'type': 'string',
#                 'isuri': True,
#             },
#             'variable': {
#                 'required': False,
#                 'type': 'string'
#             }
#         }
#
#     def test_validate_google_application_credentials(self):
#         """ Check if an error occurs for pointing to a file that does not exist when the
#         'google_application_credentials' tag is present in the schema. """
#         with CliRunner().isolated_filesystem():
#             # Make google application credentials
#             credentials_file_path = os.path.join(pathlib.Path().absolute(), 'google_application_credentials.json')
#             with open(credentials_file_path, 'w') as f:
#                 f.write('')
#             validator = ObservatoryConfigValidator(self.schema)
#
#             # google_application_credentials tag and existing file
#             validator.validate({
#                 'google_application_credentials': credentials_file_path
#             })
#             self.assertEqual(len(validator.errors), 0)
#
#             # google_application_credentials tag and non-existing file
#             validator.validate({
#                 'google_application_credentials': 'missing_file.json'
#             })
#             self.assertEqual(len(validator.errors), 1)
#
#             # no google_application_credentials tag and non-existing file
#             validator.validate({
#                 'variable': 'google_application_credentials.json'
#             })
#             self.assertEqual(len(validator.errors), 0)
#
#             # no google_application_credentials tag and existing file
#             validator.validate({
#                 'variable': 'google_application_credentials.json'
#             })
#             self.assertEqual(len(validator.errors), 0)
#
#     def test_validate_isuri(self):
#         """ Check if an error occurs for an invalid uri when the 'isuri' tag is present in the schema. """
#         validator = ObservatoryConfigValidator(self.schema)
#         # isuri tag and valid uri
#         validator.validate({
#             'crossref': 'mysql://myname:mypassword@myhost.com'
#         })
#         self.assertEqual(len(validator.errors), 0)
#
#         # isuri tag and invalid uri
#         validator.validate({
#             'crossref': 'mysql://mypassword@myhost.com'
#         })
#         self.assertEqual(len(validator.errors), 1)
#
#         # no uri tag and invalid uri
#         validator.validate({
#             'variable': 'mysql://mypassword@myhost.com'
#         })
#         self.assertEqual(len(validator.errors), 0)
#
#         # no uri tag and valid uri
#         validator.validate({
#             'variable': 'mysql://myname:mypassword@myhost.com'
#         })
#         self.assertEqual(len(validator.errors), 0)
#
#
# class TestLocalObservatoryConfig(unittest.TestCase):
#     @classmethod
#     def setUpClass(cls) -> None:
#         cls.config_dict_complete_valid = {
#             'backend': 'local',
#             'fernet_key': 'nUKEUmwh5Fs8pRSaYo-v4jSB5-zcf5_0TvG4uulhzsE=',
#             'google_application_credentials': '/path/to/google_application_credentials.json',
#             'airflow_variables': cls.create_variables_dict(AirflowVar, valid=True),
#             'airflow_connections': cls.create_variables_dict(AirflowConn, valid=True)
#         }
#         cls.config_dict_incomplete_valid = {
#             'backend': 'local',
#             'fernet_key': 'nUKEUmwh5Fs8pRSaYo-v4jSB5-zcf5_0TvG4uulhzsE=',
#             'google_application_credentials': '/path/to/google_application_credentials.json',
#             'airflow_variables': cls.create_variables_dict(AirflowVar, valid=True)
#         }
#         cls.config_dict_complete_invalid = {
#             'backend': 'local',
#             'fernet_key': 'nUKEUmwh5Fs8pRSaYo-v4jSB5-zcf5_0TvG4uulhzsE=',
#             'google_application_credentials': '/path/to/google_application_credentials.json',
#             'airflow_variables': cls.create_variables_dict(AirflowVar, valid=False),
#             'airflow_connections': cls.create_variables_dict(AirflowConn, valid=False)
#         }
#         cls.config_dict_incomplete_invalid = {
#             'backend': 'local',
#             'fernet_key': 'nUKEUmwh5Fs8pRSaYo-v4jSB5-zcf5_0TvG4uulhzsE=',
#             'google_application_credentials': '/path/to/google_application_credentials.json'
#         }
#
#     def test_load(self):
#         """ Test if config is loaded from a yaml file correctly """
#         with CliRunner().isolated_filesystem():
#             self.set_google_application_credentials()
#             config_path = tmp_config_file(self.config_dict_complete_valid)
#
#             # Test that loaded config matches expected config
#             expected_config = self.config_dict_complete_valid
#             actual_config = ObservatoryConfig(config_path).load()
#             self.assertEqual(expected_config, actual_config)
#
#     def test_get_schema(self):
#         """ Test that the created schema is in line with the values in AirflowVar/AirflowConn class """
#         schema = ObservatoryConfig.get_schema('local')
#         self.assertIsInstance(schema, dict)
#         self.assertEqual(schema['backend'], {
#             'required': True,
#             'type': 'string',
#             'check_with': customise_pointer
#         })
#         self.assertEqual(schema['fernet_key'], {
#             'required': True,
#             'type': 'string',
#             'check_with': customise_pointer
#         })
#         self.assertEqual(schema['google_application_credentials'], {
#             'type': 'string',
#             'google_application_credentials': True,
#             'required': True,
#             'check_with': customise_pointer
#         })
#         self.assertIn('airflow_connections', schema)
#         self.assertIsInstance(schema['airflow_connections'], dict)
#         self.assertIn('airflow_variables', schema)
#         self.assertIsInstance(schema['airflow_variables'], dict)
#         for obj in itertools.chain(AirflowConn, AirflowVar):
#             if obj.value['schema']:
#                 if type(obj) == AirflowConn:
#                     # if connection should check for valid uri
#                     obj.value['schema']['isuri'] = True
#                     self.assertEqual(schema['airflow_connections']['schema'][obj.get()], obj.value['schema'])
#                 else:
#                     self.assertEqual(schema['airflow_variables']['schema'][obj.get()], obj.value['schema'])
#
#     def test_is_valid(self):
#         """ Test if validation works as expected when using valid/invalid config files."""
#         with CliRunner().isolated_filesystem():
#             self.set_google_application_credentials()
#
#             # All properties specified and valid
#             config_path = tmp_config_file(self.config_dict_complete_valid)
#             valid_config = ObservatoryConfig(config_path)
#             self.assertTrue(valid_config.is_valid)
#
#             # Some properties missing, but they are not required so still valid
#             config_path = tmp_config_file(self.config_dict_incomplete_valid)
#             self.assertTrue(ObservatoryConfig(config_path).is_valid)
#
#             # All properties specified, but some that are required are None so invalid
#             config_path = tmp_config_file(self.config_dict_complete_invalid)
#             self.assertFalse(ObservatoryConfig(config_path).is_valid)
#
#             # Some properties missing, and required properties are None so invalid
#             config_path = tmp_config_file(self.config_dict_incomplete_invalid)
#             self.assertFalse(ObservatoryConfig(config_path).is_valid)
#
#             # All properties specified but google_application_credentials doesn't exist
#             os.remove(valid_config.local.google_application_credentials)
#             self.assertFalse(valid_config.is_valid)
#
#     def test_iterate_over_local_schema(self):
#         """ Test method that iterates over schema and is used to print a default config """
#         with CliRunner().isolated_filesystem():
#             # Make google application credentials
#             credentials_file_path = os.path.join(pathlib.Path().absolute(), 'google_application_credentials.json')
#             with open(credentials_file_path, 'w') as f:
#                 f.write('')
#
#             schema = ObservatoryConfig.get_schema('local')
#             print_dict = iterate_over_local_schema(schema, {}, {}, test_config=False)
#             # create dict that can be read in by config class and should be valid
#             valid_default_dict = {}
#             for key, value in print_dict.items():
#                 # check that variable only starts with '#' if it's not required
#                 if key.startswith('#'):
#                     trimmed_key = key[1:]
#                     self.assertFalse(schema[trimmed_key]['required'])
#                 else:
#                     trimmed_key = key
#                     self.assertTrue(schema[key]['required'])
#
#                 if isinstance(value, dict):
#                     valid_default_dict[trimmed_key] = {}
#                     for key2, value2 in value.items():
#                         # check that nested variables only start with '#' if they are not required
#                         if key2.startswith('#'):
#                             trimmed_key2 = key2[1:]
#                             self.assertFalse(schema[trimmed_key]['schema'][trimmed_key2]['required'])
#                         else:
#                             trimmed_key2 = key2
#                             self.assertTrue(schema[trimmed_key]['schema'][key2]['required'])
#                         # strip <-- from nested value
#                         valid_default_dict[trimmed_key][trimmed_key2] = value2.strip(' <--')
#                 elif key == 'google_application_credentials':
#                     # set value to existing path
#                     valid_default_dict[trimmed_key] = credentials_file_path
#                 else:
#                     # strip <-- from value
#                     valid_default_dict[trimmed_key] = value.strip(' <--')
#             config_path = tmp_config_file(valid_default_dict)
#             default_config = ObservatoryConfig(config_path)
#             self.assertTrue(default_config.is_valid)
#
#             # create dict with test_config and check if it gives valid config instance
#             test_dict = iterate_over_local_schema(schema, {}, {}, test_config=True)
#             test_dict['google_application_credentials'] = credentials_file_path
#             config_path = tmp_config_file(test_dict)
#             test_config = ObservatoryConfig(config_path)
#             self.assertTrue(test_config.is_valid)
#
#     def test_save_default(self):
#         """ Test if default config file is saved """
#         with CliRunner().isolated_filesystem():
#             # Set google application credentials
#             self.set_google_application_credentials()
#
#             # Create example config and save
#             example_config_path = 'example_config_path.yaml'
#             ObservatoryConfig.save_default('local', example_config_path)
#             # Check file exists
#             self.assertTrue(os.path.isfile(example_config_path))
#
#             # Create test config and save
#             test_config_path = 'test_config_path.yaml'
#             ObservatoryConfig.save_default('local', test_config_path, test=True)
#             # Check file exists
#             self.assertTrue(os.path.isfile(test_config_path))
#
#     def set_google_application_credentials(self) -> str:
#         """ For valid configs, set google application credentials to path of existing file. """
#         # Make google application credentials
#         credentials_file_path = os.path.join(pathlib.Path().absolute(), 'google_application_credentials.json')
#         with open(credentials_file_path, 'w') as f:
#             f.write('')
#         self.config_dict_complete_valid['google_application_credentials'] = credentials_file_path
#         self.config_dict_incomplete_valid['google_application_credentials'] = credentials_file_path
#
#         return credentials_file_path
#
#     @staticmethod
#     def parse_airflow_variables(airflow_class: Union[AirflowVar, AirflowConn]) -> list:
#         """
#         Parse airflow variables, create a dict for each variable with info on name, default value and whether
#         variable is
#         required or not.
#         :param airflow_class: Either AirflowVar or AirflowConn Enum class.
#         :return: List of dictionaries with variable info
#         """
#         variables = []
#         for variable in airflow_class:
#             variable_info = {}
#             if variable.value['schema']:
#                 variable_info['name'] = variable.value['name']
#             else:
#                 # skip variables that don't have a schema defined, these are hardcoded in e.g. docker file
#                 continue
#             if variable.value['default']:
#                 variable_info['default'] = variable.value['default']
#             else:
#                 variable_info['default'] = None
#             required = variable.value['schema']['required']
#             if required:
#                 variable_info['required'] = True
#             else:
#                 variable_info['required'] = False
#             variables.append(variable_info)
#         return variables
#
#     @staticmethod
#     def create_variables_dict(airflow_class: Union[AirflowVar, AirflowConn], valid: bool) -> dict:
#         """
#         Create dictionary of airflow variables. Contains name as key. Value is dependent on whether the dictionary
#         should
#         contain 'valid' values or not. If valid the value is either 'random-string' or the default value. If not
#         valid, the
#         value is None.
#         :param airflow_class: Either AirflowVar or AirflowConn Enum class.
#         :param valid: Determines whether dict should contain valid or invalid values for the variables.
#         :return: dictionary with airflow variables
#         """
#         config_dict = {}
#         variables = TestLocalObservatoryConfig.parse_airflow_variables(airflow_class)
#         for variable in variables:
#             if valid:
#                 config_dict[variable['name']] = variable['default'].strip(' <--')
#             else:
#                 config_dict[variable['name']] = None
#         return config_dict
#
#
# class TestTerraformObservatoryConfig(unittest.TestCase):
#     @classmethod
#     def setUpClass(cls) -> None:
#         cls.config_dict_complete_valid = {
#             'backend': {
#                 'terraform': {
#                     'organization': 'your-organization',
#                     'workspaces_prefix': 'observatory-'
#                 }
#             },
#             'project_id': 'your-project-id',
#             'google_application_credentials': '/path/to/service/account/key.json',
#             'region': 'us-west1',
#             'zone': 'us-west1-c',
#             'data_location': 'us',
#             'environment': 'develop',
#             'database': {
#                 'tier': 'db-custom-4-15360',
#                 'backup_start_time': '23:00'
#             },
#             'airflow_main': {
#                 'machine_type': 'n2-standard-2',
#                 'disk_size': 20,
#                 'disk_type': 'pd-ssd'
#             },
#             'airflow_worker': {
#                 'machine_type': 'n1-standard-2',
#                 'disk_size': 20,
#                 'disk_type': 'pd-standard'
#             },
#             'airflow_worker_create': False,
#             'airflow_secrets': {
#                 'fernet_key': 'eUO4rUub_3XiyY8ltOjPNl2e9SXD4WdOe6tXVyH7N2E=',
#                 'postgres_password': 'random_password$#@=?%^Q^$',
#                 'redis_password': 'random_password$#@=?%^Q^$',
#                 'airflow_ui_user_password': 'random_password',
#                 'airflow_ui_user_email': 'your.email@somehost.com'
#             },
#             'airflow_connections': {
#                 'terraform': 'mysql://:terraform-token@',
#                 'slack': 'https://:T00000000%2FB00000000%2FXXXXXXXXXXXXXXXXXXXXXXXX@https%3A%2F%2Fhooks.slack.com'
#                          '%2Fservices',
#                 'crossref': 'mysql://myname:mypassword@myhost.com',
#                 'mag_releases_table': 'mysql://myname:mypassword@myhost.com',
#                 'mag_snapshots_container': 'mysql://myname:mypassword@myhost.com'
#             },
#             'airflow_variables': {
#                 'example_name': 'example_value'
#             }
#         }
#         cls.config_dict_incomplete_valid = copy.deepcopy(cls.config_dict_complete_valid)
#         del cls.config_dict_incomplete_valid['airflow_connections']['crossref']
#         del cls.config_dict_incomplete_valid['airflow_connections']['mag_releases_table']
#         del cls.config_dict_incomplete_valid['airflow_variables']
#
#         cls.config_dict_complete_invalid = copy.deepcopy(cls.config_dict_complete_valid)
#         cls.config_dict_complete_invalid['airflow_connections']['terraform'] = None
#         cls.config_dict_complete_invalid['region'] = None
#
#         cls.config_dict_incomplete_invalid = copy.deepcopy(cls.config_dict_complete_valid)
#         del cls.config_dict_incomplete_invalid['airflow_secrets']
#         del cls.config_dict_incomplete_invalid['zone']
#
#     def test_load(self):
#         """ Test if config is loaded from a yaml file correctly """
#         with CliRunner().isolated_filesystem():
#             self.set_google_application_credentials()
#             config_path = tmp_config_file(self.config_dict_complete_valid)
#
#             # Test that loaded config matches expected config
#             expected_config = self.config_dict_complete_valid
#             actual_config = ObservatoryConfig(config_path).load()
#             self.assertEqual(expected_config, actual_config)
#
#     def test_get_schema(self):
#         """ Test that the created schema is in line with the values in variables.tf file """
#         schema = ObservatoryConfig.get_schema('terraform', terraform_variables_tf_path())
#         self.assertIsInstance(schema, dict)
#         self.assertEqual(schema['backend'], {
#             'required': True,
#             'type': 'dict',
#             'schema': {
#                 'terraform': {
#                     'required': True,
#                     'type': 'dict',
#                     'schema': {
#                         'organization': {
#                             'required': True,
#                             'type': 'string',
#                             'meta': {
#                                 'default': 'your-organization <--'
#                             }
#                         },
#                         'workspaces_prefix': {
#                             'required': True,
#                             'type': 'string',
#                             'meta': {
#                                 'default': 'observatory- <--'
#                             }
#                         }
#                     },
#                 }
#             }
#         })
#         for var, value in schema.items():
#             # every variable should have at least these fields
#             self.assertIn('required', value)
#             self.assertIn('type', value)
#             # backend has slightly different structure (two times nested)
#             if var == 'backend':
#                 continue
#             self.assertIn('meta', value)
#             self.assertIn('description', value['meta'])
#             # nested variables
#             try:
#                 for key2, value2 in value['schema'].items():
#                     self.assertIn('required', value2)
#                     self.assertIn('type', value2)
#                     self.assertIn('default', value2['meta'])
#             # unnested variables
#             except KeyError:
#                 self.assertIn('default', value['meta'])
#
#             if var == 'airflow_connections':
#                 for key2, value2 in value['schema'].items():
#                     # connections should have isuri field
#                     self.assertEqual(value2['isuri'], True)
#                     # if the connection isn't required there should be a real default value
#                     if value2['required'] is False:
#                         self.assertIn('default', value2)
#             # boolean should be lowercase
#             if value['type'] == 'boolean':
#                 self.assertTrue(value['meta']['default'] in ['false', 'true'])
#
#     def test_is_valid(self):
#         """ Check that instances using the dicts defined above are valid/invalid as expected."""
#         with CliRunner().isolated_filesystem():
#             self.set_google_application_credentials()
#
#             # All properties specified and valid
#             config_path = tmp_config_file(self.config_dict_complete_valid)
#             valid_config = ObservatoryConfig(config_path)
#             self.assertTrue(valid_config.is_valid)
#
#             # Some properties missing, but they are not required so still valid
#             config_path = tmp_config_file(self.config_dict_incomplete_valid)
#             self.assertTrue(ObservatoryConfig(config_path).is_valid)
#
#             # All properties specified, but some that are required are None so invalid
#             config_path = tmp_config_file(self.config_dict_complete_invalid)
#             self.assertFalse(ObservatoryConfig(config_path).is_valid)
#
#             # Some properties missing, and required properties are None so invalid
#             config_path = tmp_config_file(self.config_dict_incomplete_invalid)
#             self.assertFalse(ObservatoryConfig(config_path).is_valid)
#
#             # All properties specified but google_application_credentials doesn't exist
#             os.remove(valid_config.terraform.google_application_credentials)
#             self.assertFalse(valid_config.is_valid)
#
#     def test_iterate_over_terraform_schema(self):
#         """ Check that iterate_over_terraform_schema returns a valid schema. Fields such as 'required' and
#         'sensitive' are checked. The resulting schema is used to create an ObservatoryConfig instance and a check is
#         done to see if this instance is valid. Tags ('!sensitive', '#') are removed from the keys as well as ' <--'
#         from the values. """
#         with CliRunner().isolated_filesystem():
#             # Make google application credentials
#             credentials_file_path = os.path.join(pathlib.Path().absolute(), 'google_application_credentials.json')
#             with open(credentials_file_path, 'w') as f:
#                 f.write('')
#
#             schema = ObservatoryConfig.get_schema('terraform', terraform_variables_tf_path())
#             default_dict = iterate_over_terraform_schema(schema, {}, {}, test_config=False)
#             # create dict that can be read in by config class and should be valid
#             valid_default_dict = {}
#             for key, value in default_dict.items():
#                 # check that variable only starts with '#' if it's not required and with '!sensitive' for sensitive
#                 # variables
#                 required = True
#                 sensitive = False
#                 if key.startswith('#'):
#                     trimmed_key = key[1:]
#                     required = False
#                 else:
#                     trimmed_key = key
#                 if trimmed_key.startswith('!sensitive '):
#                     trimmed_key = trimmed_key[11:]
#                     sensitive = True
#
#                 if sensitive:
#                     self.assertTrue(
#                         trimmed_key in ['google_application_credentials', 'airflow_secrets', 'airflow_connections'])
#                 else:
#                     self.assertFalse(
#                         trimmed_key in ['google_application_credentials', 'airflow_secrets', 'airflow_connections'])
#                 if required:
#                     self.assertTrue(schema[trimmed_key]['required'])
#                 else:
#                     self.assertFalse(schema[trimmed_key]['required'])
#
#                 # backend has slightly different structure
#                 if key == 'backend':
#                     valid_default_dict['backend'] = {
#                         'terraform': {
#                             'organization': 'your-organization',
#                             'workspaces_prefix': 'observatory-'
#                         }
#                     }
#                     continue
#                 # nested variables
#                 if isinstance(value, dict):
#                     valid_default_dict[trimmed_key] = {}
#                     for key2, value2 in value.items():
#                         required = True
#                         if key2.startswith('#'):
#                             trimmed_key2 = key2[1:]
#                             required = False
#                         else:
#                             trimmed_key2 = key2
#                         # not all variables have a schema (e.g. 'airflow_variables')
#                         try:
#                             schema2 = schema[trimmed_key]['schema']
#                             # check that nested variables only start with '#' if they are not required
#                             if required:
#                                 self.assertTrue(schema2[key2]['required'])
#                             else:
#                                 self.assertFalse(schema2[trimmed_key2]['required'])
#                         except KeyError:
#                             value2 = value2.strip(' <--')
#                             valid_default_dict[trimmed_key][trimmed_key2] = value2
#                             continue
#
#                         # strip <-- from nested value and transform to right format based on schema type
#                         if schema2[trimmed_key2]['type'] == 'string':
#                             value2 = value2.strip(' <--')
#                         elif schema2[trimmed_key2]['type'] == 'boolean':
#                             value2 = eval(value2.strip(' <--').capitalize())
#                         elif schema2[trimmed_key2]['type'] == 'number':
#                             value2 = int(str(value2).strip(' <--'))
#                         valid_default_dict[trimmed_key][trimmed_key2] = value2
#
#                 elif trimmed_key == 'google_application_credentials':
#                     # set value to existing path
#                     valid_default_dict[trimmed_key] = credentials_file_path
#                 else:
#                     # strip <-- from value and transform to right format based on schema type
#                     if schema[trimmed_key]['type'] == 'string':
#                         value = value.strip(' <--')
#                     elif schema[trimmed_key]['type'] == 'boolean':
#                         value = eval(value.strip(' <--').capitalize())
#                     elif schema[trimmed_key]['type'] == 'number':
#                         value = int(str(value).strip(' <--'))
#                     valid_default_dict[trimmed_key] = value
#
#             config_path = tmp_config_file(valid_default_dict)
#             default_config = ObservatoryConfig(config_path)
#             # check if config is valid using the values from the schema
#             self.assertTrue(default_config.is_valid)
#
#             # create dict with test_config and check if it gives valid config instance
#             test_dict = iterate_over_terraform_schema(schema, {}, {}, test_config=True)
#             test_dict['google_application_credentials'] = credentials_file_path
#             config_path = tmp_config_file(test_dict)
#             test_config = ObservatoryConfig(config_path)
#             self.assertTrue(test_config.is_valid)
#
#     def test_save_default(self):
#         """ Test if default config file is saved """
#         with CliRunner().isolated_filesystem():
#             # Set google application credentials
#             self.set_google_application_credentials()
#
#             # Create example config and save
#             example_config_path = 'example_config_path.yaml'
#             ObservatoryConfig.save_default('terraform', example_config_path, terraform_variables_tf_path())
#             # Check file exists
#             self.assertTrue(os.path.isfile(example_config_path))
#
#             # Create test config and save
#             test_config_path = 'test_config_path.yaml'
#             ObservatoryConfig.save_default('terraform', test_config_path, terraform_variables_tf_path(), test=True)
#             # Check file exists
#             self.assertTrue(os.path.isfile(test_config_path))
#
#     def test_yaml_constructor(self):
#         """ Checks whether variables with a !sensitive tag are added to the config's sensitive_variables dict as
#         expected."""
#         with CliRunner().isolated_filesystem():
#             # Set google application credentials
#             self.set_google_application_credentials()
#
#             # Create default config and save, this should contain variables with '!sensitive' tag
#             default_config_path = 'default_config_path.yaml'
#             ObservatoryConfig.save_default('terraform', default_config_path, terraform_variables_tf_path())
#             # create config instance from file
#             default_config = ObservatoryConfig(default_config_path)
#
#             # retrieve sensitive variables from file
#             with open(default_config_path, 'r') as f:
#                 config_lines = f.readlines()
#             sensitive_variables = []
#             for line in config_lines:
#                 if line.startswith('!sensitive '):
#                     line = line.split(':')[0]
#                     sensitive_variables.append(line[11:])
#             # compare sensitive variables from config and sensitive variables from file
#             self.assertEqual(default_config.sensitive_variables, sensitive_variables)
#
#     def set_google_application_credentials(self) -> str:
#         """ For valid configs, set google application credentials to path of existing file. """
#         # Make google application credentials
#         credentials_file_path = os.path.join(pathlib.Path().absolute(), 'google_application_credentials.json')
#         with open(credentials_file_path, 'w') as f:
#             f.write('')
#         self.config_dict_complete_valid['google_application_credentials'] = credentials_file_path
#         self.config_dict_incomplete_valid['google_application_credentials'] = credentials_file_path
#
#         return credentials_file_path
