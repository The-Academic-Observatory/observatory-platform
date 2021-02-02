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
                                                     ObservatoryConfigValidator, save_yaml)


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
                'fernet_key': 'random-fernet-key',
                'secret_key': 'random-secret-key'
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
                    'fernet_key': 'random-fernet-key',
                    'secret_key': 'random-secret-key'
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
                'fernet_key': 1,
                'secret_key': 1
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
                'secret_key': 'random-secret-key',
                'ui_user_password': 'password',
                'ui_user_email': 'password'
            },
            'terraform': {
                'organization': 'hello world'
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
                'secret_key': 'random-secret-key',
                'ui_user_password': 'password',
                'ui_user_email': 'password'
            },
            'terraform': {
                'organization': 'hello world'
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
                    'organization': 'hello world'
                }
            }
        ]
        invalid_docs = [
            {
                'terraform': {
                    'organization': 0
                }
            },
            {
                'terraform': {
                    'organization': dict()
                }
            },
            {
                'terraform': {

                }
            }
        ]
        expected_errors = [{'terraform': [
            {'organization': ['must be of string type']}]},
            {'terraform': [{'organization': ['must be of string type']}]},
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
                    'fernet_key': 'random-fernet-key',
                    'secret_key': 'random-secret-key'
                }
            },
            {
                'airflow': {
                    'fernet_key': 'password',
                    'secret_key': 'password',
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
                           {'airflow': [{'fernet_key': ['required field'], 'secret_key': ['required field']}]}]
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
                    'organization': 'hello world'
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
                    'secret_key': 'password',
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
                           {'airflow': [{'fernet_key': ['required field'], 'secret_key': ['required field'],
                                         'ui_user_email': ['required field'], 'ui_user_password': ['required field']}]}]
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
