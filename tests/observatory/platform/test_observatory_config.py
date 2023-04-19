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

import datetime
import os
import pathlib
import random
import string
import unittest
from typing import Dict, List

import pendulum
import yaml
from click.testing import CliRunner

from observatory.platform.observatory_config import (
    Backend,
    BackendType,
    CloudSqlDatabase,
    Environment,
    GoogleCloud,
    Observatory,
    ObservatoryConfig,
    ObservatoryConfigValidator,
    Terraform,
    TerraformConfig,
    VirtualMachine,
    WorkflowsProject,
    is_base64,
    is_fernet_key,
    is_secret_key,
    make_schema,
    save_yaml,
    Workflow,
    workflows_to_json_string,
    json_string_to_workflows,
)


class TestObservatoryConfigValidator(unittest.TestCase):
    def setUp(self) -> None:
        self.schema = dict()
        self.schema["google_cloud"] = {
            "required": True,
            "type": "dict",
            "schema": {"credentials": {"required": True, "type": "string", "google_application_credentials": True}},
        }

    def test_workflows_to_json_string(self):
        workflows = [
            Workflow(
                dag_id="my_dag",
                name="My DAG",
                class_name="observatory.platform.workflows.vm_workflow.VmCreateWorkflow",
                kwargs=dict(dt=pendulum.datetime(2021, 1, 1)),
            )
        ]
        json_string = workflows_to_json_string(workflows)
        self.assertEqual(
            '[{"dag_id": "my_dag", "name": "My DAG", "class_name": "observatory.platform.workflows.vm_workflow.VmCreateWorkflow", "cloud_workspace": null, "kwargs": {"dt": "2021-01-01T00:00:00+00:00"}}]',
            json_string,
        )

    def test_json_string_to_workflows(self):
        json_string = '[{"dag_id": "my_dag", "name": "My DAG", "class_name": "observatory.platform.workflows.vm_workflow.VmCreateWorkflow", "cloud_workspace": null, "kwargs": {"dt": "2021-01-01T00:00:00+00:00"}}]'
        actual_workflows = json_string_to_workflows(json_string)
        self.assertEqual(
            [
                Workflow(
                    dag_id="my_dag",
                    name="My DAG",
                    class_name="observatory.platform.workflows.vm_workflow.VmCreateWorkflow",
                    kwargs=dict(dt=pendulum.datetime(2021, 1, 1)),
                )
            ],
            actual_workflows,
        )

    def test_validate_google_application_credentials(self):
        """Check if an error occurs for pointing to a file that does not exist when the
        'google_application_credentials' tag is present in the schema."""

        with CliRunner().isolated_filesystem():
            # Make google application credentials
            credentials_file_path = os.path.join(pathlib.Path().absolute(), "google_application_credentials.json")
            with open(credentials_file_path, "w") as f:
                f.write("")
            validator = ObservatoryConfigValidator(self.schema)

            # google_application_credentials tag and existing file
            validator.validate({"google_cloud": {"credentials": credentials_file_path}})
            self.assertEqual(len(validator.errors), 0)

            # google_application_credentials tag and non-existing file
            validator.validate({"google_cloud": {"credentials": "missing_file.json"}})
            self.assertEqual(len(validator.errors), 1)


class TestObservatoryConfig(unittest.TestCase):
    def test_load(self):
        # Test that a minimal configuration works
        dict_ = {
            "backend": {"type": "local", "environment": "develop"},
            "observatory": {
                "package": "observatory-platform",
                "package_type": "pypi",
                "airflow_fernet_key": "IWt5jFGSw2MD1shTdwzLPTFO16G8iEAU3A6mGo_vJTY=",
                "airflow_secret_key": "a" * 16,
            },
        }

        file_path = "config-valid-minimal.yaml"
        with CliRunner().isolated_filesystem():
            save_yaml(file_path, dict_)

            config = ObservatoryConfig.load(file_path)
            self.assertIsInstance(config, ObservatoryConfig)
            self.assertTrue(config.is_valid)

        file_path = "config-valid-typical.yaml"
        with CliRunner().isolated_filesystem():
            credentials_path = os.path.abspath("creds.json")
            open(credentials_path, "a").close()

            # Test that a typical configuration works
            dict_ = {
                "backend": {"type": "local", "environment": "develop"},
                "google_cloud": {
                    "project_id": "my-project-id",
                    "credentials": credentials_path,
                    "data_location": "us",
                },
                "observatory": {
                    "package": "observatory-platform",
                    "package_type": "pypi",
                    "airflow_fernet_key": "IWt5jFGSw2MD1shTdwzLPTFO16G8iEAU3A6mGo_vJTY=",
                    "airflow_secret_key": "a" * 16,
                },
                "workflows_projects": [
                    {
                        "package_name": "academic-observatory-workflows",
                        "package": "/path/to/academic-observatory-workflows",
                        "package_type": "editable",
                        "dags_module": "academic_observatory_workflows.dags",
                    },
                    {
                        "package_name": "oaebu-workflows",
                        "package": "/path/to/oaebu-workflows/dist/oaebu-workflows.tar.gz",
                        "package_type": "sdist",
                        "dags_module": "oaebu_workflows.dags",
                    },
                ],
                "cloud_workspaces": [
                    {
                        "workspace": {
                            "project_id": "my-project-id",
                            "download_bucket": "my-download-bucket",
                            "transform_bucket": "my-transform-bucket",
                            "data_location": "us",
                        }
                    },
                ],
                "workflows": [
                    {
                        "dag_id": "my_dag",
                        "name": "My DAG",
                        "cloud_workspace": {
                            "project_id": "my-project-id",
                            "download_bucket": "my-download-bucket",
                            "transform_bucket": "my-transform-bucket",
                            "data_location": "us",
                        },
                        "class_name": "path.to.my_workflow.Workflow",
                        "kwargs": {
                            "hello": "world",
                            "hello_date": datetime.date(2021, 1, 1),
                            "hello_datetime": datetime.datetime(2021, 1, 1),
                        },
                        # datetime.date gets converted into 2021-01-01 in yaml, which can be read as a date
                        # same for datetime.datetime
                    },
                ],
            }

            save_yaml(file_path, dict_)

            config = ObservatoryConfig.load(file_path)
            self.assertIsInstance(config, ObservatoryConfig)
            self.assertTrue(config.is_valid)

            # Test that date value are parsed into pendulums
            workflow: Workflow = config.workflows[0]
            hello_date = workflow.kwargs["hello_date"]
            self.assertIsInstance(hello_date, pendulum.DateTime)
            self.assertEqual(pendulum.datetime(2021, 1, 1), hello_date)

            hello_datetime = workflow.kwargs["hello_datetime"]
            self.assertIsInstance(hello_datetime, pendulum.DateTime)
            self.assertEqual(pendulum.datetime(2021, 1, 1), hello_datetime)

        # Test that an invalid minimal config works
        dict_ = {"backend": {"type": "terraform", "environment": "my-env"}, "airflow": {"fernet_key": False}}

        file_path = "config-invalid-minimal.yaml"
        with CliRunner().isolated_filesystem():
            save_yaml(file_path, dict_)

            config = ObservatoryConfig.load(file_path)
            self.assertIsInstance(config, ObservatoryConfig)
            self.assertFalse(config.is_valid)

        # Test that an invalid typical config is loaded by invalid
        dict_ = {
            "backend": {"type": "terraform", "environment": "my-env"},
            "google_cloud": {
                "project_id": "my-project-id",
                "credentials": "/path/to/creds.json",
                "data_location": 1,
            },
            "observatory": {"airflow_fernet_key": "bad", "airflow_secret_key": "bad"},
            "workflows_projects": [
                {
                    "package_name": "academic-observatory-workflows",
                    "package_type": "editable",
                    "dags_module": "academic_observatory_workflows.dags",
                },
                {
                    "package_name": "oaebu-workflows",
                    "package": "/path/to/oaebu-workflows/dist/oaebu-workflows.tar.gz",
                    "package_type": "sdist",
                    "dags_module": False,
                },
            ],
            "cloud_workspaces": [
                {
                    "workspace": {
                        "download_bucket": "my-download-bucket",
                        "transform_bucket": "my-transform-bucket",
                        "data_location": "us",
                    }
                },
            ],
            "workflows": [
                {
                    "name": "My DAG",
                    "cloud_workspace": {
                        "project_id": "my-project-id",
                        "download_bucket": "my-download-bucket",
                        "transform_bucket": "my-transform-bucket",
                        "data_location": "us",
                    },
                    "class_name": "path.to.my_workflow.Workflow",
                    "kwargs": {"hello": "world"},
                },
            ],
        }

        file_path = "config-invalid-typical.yaml"
        with CliRunner().isolated_filesystem():
            save_yaml(file_path, dict_)

            config = ObservatoryConfig.load(file_path)
            self.assertIsInstance(config, ObservatoryConfig)
            self.assertFalse(config.is_valid)
            self.assertEqual(12, len(config.errors))


class TestTerraformConfig(unittest.TestCase):
    def test_load(self):
        # Test that a minimal configuration works

        file_path = "config-valid-typical.yaml"
        with CliRunner().isolated_filesystem():
            credentials_path = os.path.abspath("creds.json")
            open(credentials_path, "a").close()

            dict_ = {
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
                "terraform": {"organization": "hello world"},
                "google_cloud": {
                    "project_id": "my-project",
                    "credentials": credentials_path,
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
                "cloud_workspaces": [
                    {
                        "workspace": {
                            "project_id": "my-project-id",
                            "download_bucket": "my-download-bucket",
                            "transform_bucket": "my-transform-bucket",
                            "data_location": "us",
                        }
                    },
                ],
                "workflows": [
                    {
                        "dag_id": "my_dag",
                        "name": "My DAG",
                        "cloud_workspace": {
                            "project_id": "my-project-id",
                            "download_bucket": "my-download-bucket",
                            "transform_bucket": "my-transform-bucket",
                            "data_location": "us",
                        },
                        "class_name": "path.to.my_workflow.Workflow",
                        "kwargs": {"hello": "world"},
                    },
                ],
            }

            save_yaml(file_path, dict_)

            config = TerraformConfig.load(file_path)
            self.assertIsInstance(config, TerraformConfig)
            self.assertTrue(config.is_valid)

        file_path = "config-valid-typical.yaml"
        with CliRunner().isolated_filesystem():
            credentials_path = os.path.abspath("creds.json")
            open(credentials_path, "a").close()

            # Test that a typical configuration is loaded
            dict_ = {
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
                "terraform": {"organization": "hello world"},
                "google_cloud": {
                    "project_id": "my-project",
                    "credentials": credentials_path,
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
                "workflows_projects": [
                    {
                        "package_name": "academic-observatory-workflows",
                        "package": "/path/to/academic-observatory-workflows",
                        "package_type": "editable",
                        "dags_module": "academic_observatory_workflows.dags",
                    },
                    {
                        "package_name": "oaebu-workflows",
                        "package": "/path/to/oaebu-workflows/dist/oaebu-workflows.tar.gz",
                        "package_type": "sdist",
                        "dags_module": "oaebu_workflows.dags",
                    },
                ],
            }

            save_yaml(file_path, dict_)

            config = TerraformConfig.load(file_path)
            self.assertIsInstance(config, TerraformConfig)
            self.assertTrue(config.is_valid)

        # Test that an invalid minimal config is loaded and invalid
        dict_ = {
            "backend": {"type": "local", "environment": "develop"},
            "airflow": {
                "package": "observatory-platform",
                "package_type": "pypi",
                "fernet_key": "random-fernet-key",
                "secret_key": "random-secret-key",
                "ui_user_password": "password",
                "ui_user_email": "password",
            },
            "terraform": {"organization": "hello world"},
            "google_cloud": {
                "project_id": "my-project",
                "credentials": "/path/to/creds.json",
                "region": "us-west",
                "zone": "us-west1",
                "data_location": "us",
                "buckets": {
                    "download_bucket": "my-download-bucket-1234",
                    "transform_bucket": "my-transform-bucket-1234",
                },
            },
            "cloud_sql_database": {"tier": "db-custom-2-7680", "backup_start_time": "2300"},
            "airflow_main_vm": {"machine_type": "n2-standard-2", "disk_size": 0, "disk_type": "disk", "create": True},
            "airflow_worker_vm": {
                "machine_type": "n2-standard-2",
                "disk_size": 0,
                "disk_type": "disk",
                "create": False,
            },
        }

        file_path = "config-invalid-minimal.yaml"
        with CliRunner().isolated_filesystem():
            save_yaml(file_path, dict_)

            config = TerraformConfig.load(file_path)
            self.assertIsInstance(config, TerraformConfig)
            self.assertFalse(config.is_valid)

        # Test that an invalid typical config is loaded and invalid
        dict_ = {
            "backend": {"type": "terraform", "environment": "develop"},
            "airflow": {
                "package": "observatory-platform",
                "package_type": "pypi",
                "fernet_key": "random-fernet-key",
                "secret_key": "random-secret-key",
                "ui_user_password": "password",
                "ui_user_email": "password",
            },
            "terraform": {"organization": "hello world"},
            "google_cloud": {
                "project_id": "my-project",
                "credentials": "/path/to/creds.json",
                "region": "us-west1",
                "zone": "us-west1-c",
                "data_location": "us",
                "buckets": {
                    "download_bucket": "my-download-bucket-1234",
                    "transform_bucket": "my-transform-bucket-1234",
                },
            },
            "cloud_sql_database": {"tier": "db-custom-2-7680", "backup_start_time": "23:00"},
            "airflow_main_vm": {"machine_type": "n2-standard-2", "disk_size": 1, "disk_type": "pd-ssd", "create": True},
            "airflow_worker_vm": {
                "machine_type": "n2-standard-2",
                "disk_size": 1,
                "disk_type": "pd-standard",
                "create": False,
            },
            "airflow_variables": {"my-variable-name": 1},
            "airflow_connections": {"my-connection": "my-token"},
            "workflows_projects": [
                {
                    "package_name": "academic-observatory-workflows",
                    "package_type": "editable",
                    "dags_module": "academic_observatory_workflows.dags",
                },
                {
                    "package_name": "oaebu-workflows",
                    "package": "/path/to/oaebu-workflows/dist/oaebu-workflows.tar.gz",
                    "package_type": "sdist",
                    "dags_module": False,
                },
            ],
            "cloud_workspaces": [
                {
                    "workspace": {
                        "download_bucket": "my-download-bucket",
                        "transform_bucket": "my-transform-bucket",
                        "data_location": "us",
                    }
                },
            ],
            "workflows": [
                {
                    "name": "My DAG",
                    "cloud_workspace": {
                        "project_id": "my-project-id",
                        "download_bucket": "my-download-bucket",
                        "transform_bucket": "my-transform-bucket",
                        "data_location": "us",
                    },
                    "class_name": "path.to.my_workflow.Workflow",
                    "kwargs": {"hello": "world"},
                },
            ],
        }

        file_path = "config-invalid-typical.yaml"
        with CliRunner().isolated_filesystem():
            save_yaml(file_path, dict_)

            config = TerraformConfig.load(file_path)
            self.assertIsInstance(config, TerraformConfig)
            self.assertFalse(config.is_valid)
            self.assertEqual(10, len(config.errors))


class TestSchema(unittest.TestCase):
    def assert_sub_schema_valid(
        self, valid_docs: List[Dict], invalid_docs: List[Dict], schema, sub_schema_key, expected_errors
    ):
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
        contains = [
            "backend",
            "terraform",
            "google_cloud",
            "observatory",
            "workflows_projects",
            "cloud_workspaces",
            "workflows",
        ]
        not_contains = ["cloud_sql_database", "airflow_main_vm", "airflow_worker_vm"]
        self.assert_schema_keys(schema, contains, not_contains)

    def test_terraform_schema_keys(self):
        # Test that terraform schema keys exist
        schema = make_schema(BackendType.terraform)
        contains = [
            "backend",
            "terraform",
            "google_cloud",
            "observatory",
            "cloud_sql_database",
            "airflow_main_vm",
            "airflow_worker_vm",
            "workflows_projects",
            "cloud_workspaces",
            "workflows",
        ]
        not_contains = []
        self.assert_schema_keys(schema, contains, not_contains)

    def test_local_schema_backend(self):
        schema = make_schema(BackendType.local)
        schema_key = "backend"

        valid_docs = [
            {"backend": {"type": "local", "environment": "develop"}},
            {"backend": {"type": "local", "environment": "staging"}},
            {"backend": {"type": "local", "environment": "production"}},
        ]
        invalid_docs = [{"backend": {"type": "terraform", "environment": "hello"}}]
        expected_errors = [
            {"backend": [{"environment": ["unallowed value hello"], "type": ["unallowed value terraform"]}]}
        ]
        self.assert_sub_schema_valid(valid_docs, invalid_docs, schema, schema_key, expected_errors)

    def test_local_schema_terraform(self):
        schema = make_schema(BackendType.local)
        schema_key = "terraform"

        valid_docs = [{}, {"terraform": {"organization": "hello world"}}]
        invalid_docs = [{"terraform": {"organization": 0}}, {"terraform": {"organization": dict()}}, {"terraform": {}}]
        expected_errors = [
            {"terraform": [{"organization": ["must be of string type"]}]},
            {"terraform": [{"organization": ["must be of string type"]}]},
            {"terraform": [{"organization": ["required field"]}]},
        ]
        self.assert_sub_schema_valid(valid_docs, invalid_docs, schema, schema_key, expected_errors)

    def test_local_schema_google_cloud(self):
        schema = make_schema(BackendType.local)
        schema_key = "google_cloud"

        with CliRunner().isolated_filesystem():
            credentials_path = os.path.abspath("creds.json")
            open(credentials_path, "a").close()

            valid_docs = [
                {},
                {
                    "google_cloud": {
                        "project_id": "my-project",
                        "credentials": credentials_path,
                        "region": "us-west1",
                        "zone": "us-west1-c",
                        "data_location": "us",
                    }
                },
            ]
            invalid_docs = [
                {
                    "google_cloud": {
                        "project_id": 1,
                        "credentials": "/path/to/creds.json",
                        "region": "us-west",
                        "zone": "us-west1",
                        "data_location": list(),
                    }
                }
            ]

            expected_errors = [
                {
                    "google_cloud": [
                        {
                            "credentials": [
                                "the file /path/to/creds.json does not exist. See https://cloud.google.com/docs/authentication/getting-started for instructions on how to create a service account and save the JSON key to your workstation."
                            ],
                            "project_id": ["must be of string type"],
                            "data_location": ["must be of string type"],
                            "region": ["value does not match regex '^\\w+\\-\\w+\\d+$'"],
                            "zone": ["value does not match regex '^\\w+\\-\\w+\\d+\\-[a-z]{1}$'"],
                        }
                    ]
                }
            ]
            self.assert_sub_schema_valid(valid_docs, invalid_docs, schema, schema_key, expected_errors)

    def test_local_schema_observatory(self):
        schema = make_schema(BackendType.local)
        schema_key = "observatory"

        valid_docs = [
            {
                "observatory": {
                    "package": "observatory-platform",
                    "package_type": "pypi",
                    "airflow_fernet_key": "IWt5jFGSw2MD1shTdwzLPTFO16G8iEAU3A6mGo_vJTY=",
                    "airflow_secret_key": "a" * 16,
                }
            },
            {
                "observatory": {
                    "package": "/path/to/observatory-platform",
                    "package_type": "editable",
                    "airflow_fernet_key": "IWt5jFGSw2MD1shTdwzLPTFO16G8iEAU3A6mGo_vJTY=",
                    "airflow_secret_key": "a" * 16,
                    "airflow_ui_user_password": "password",
                    "airflow_ui_user_email": "password",
                }
            },
        ]
        invalid_docs = [
            {},
            {"observatory": {"airflow_ui_user_password": "password", "airflow_ui_user_email": "password"}},
        ]

        expected_errors = [
            {"observatory": ["required field"]},
            {
                "observatory": [
                    {
                        "package": ["required field"],
                        "package_type": ["required field"],
                        "airflow_fernet_key": ["required field"],
                        "airflow_secret_key": ["required field"],
                    }
                ]
            },
        ]
        self.assert_sub_schema_valid(valid_docs, invalid_docs, schema, schema_key, expected_errors)

    def test_local_schema_workflows_projects(self):
        schema = make_schema(BackendType.local)
        schema_key = "workflows_projects"

        valid_docs = [
            {},
            {
                "workflows_projects": [
                    {
                        "package_name": "academic-observatory-workflows",
                        "package": "/path/to/academic-observatory-workflows",
                        "package_type": "editable",
                        "dags_module": "academic_observatory_workflows.dags",
                    },
                    {
                        "package_name": "oaebu-workflows",
                        "package": "/path/to/oaebu-workflows/dist/oaebu-workflows.tar.gz",
                        "package_type": "sdist",
                        "dags_module": "oaebu_workflows.dags",
                    },
                ],
            },
        ]
        invalid_docs = [
            {
                "workflows_projects": [
                    {
                        "package_name": "academic-observatory-workflows",
                        "package": "/path/to/academic-observatory-workflows",
                    }
                ]
            }
        ]

        expected_errors = [
            {"workflows_projects": [{0: [{"package_type": ["required field"], "dags_module": ["required field"]}]}]}
        ]
        self.assert_sub_schema_valid(valid_docs, invalid_docs, schema, schema_key, expected_errors)

    def test_terraform_schema_backend(self):
        schema = make_schema(BackendType.terraform)
        schema_key = "backend"

        valid_docs = [{"backend": {"type": "terraform", "environment": "develop"}}]
        invalid_docs = [{"backend": {"type": "local", "environment": "develop"}}]
        expected_errors = [{"backend": [{"type": ["unallowed value local"]}]}]
        self.assert_sub_schema_valid(valid_docs, invalid_docs, schema, schema_key, expected_errors)

    def test_terraform_schema_terraform(self):
        # Test that terraform is required
        schema = make_schema(BackendType.terraform)
        schema_key = "terraform"

        valid_docs = [{"terraform": {"organization": "hello world"}}]
        invalid_docs = [{}]
        expected_errors = [{"terraform": ["required field"]}]
        self.assert_sub_schema_valid(valid_docs, invalid_docs, schema, schema_key, expected_errors)

    def test_terraform_schema_google_cloud(self):
        # Test that all google cloud fields are required
        schema = make_schema(BackendType.terraform)
        schema_key = "google_cloud"

        with CliRunner().isolated_filesystem():
            credentials_path = os.path.abspath("creds.json")
            open(credentials_path, "a").close()

            valid_docs = [
                {
                    "google_cloud": {
                        "project_id": "my-project",
                        "credentials": credentials_path,
                        "region": "us-west1",
                        "zone": "us-west1-c",
                        "data_location": "us",
                    }
                }
            ]
            invalid_docs = [{}, {"google_cloud": {}}]

            expected_errors = [
                {"google_cloud": ["required field"]},
                {
                    "google_cloud": [
                        {
                            "credentials": ["required field"],
                            "data_location": ["required field"],
                            "project_id": ["required field"],
                            "region": ["required field"],
                            "zone": ["required field"],
                        }
                    ]
                },
            ]
            self.assert_sub_schema_valid(valid_docs, invalid_docs, schema, schema_key, expected_errors)

    def test_terraform_schema_observatory(self):
        # Test that airflow ui password and email required
        schema = make_schema(BackendType.terraform)
        schema_key = "observatory"

        valid_docs = [
            {
                "observatory": {
                    "package": "/path/to/observatory-platform/observatory-platform.tar.gz",
                    "package_type": "sdist",
                    "airflow_fernet_key": "IWt5jFGSw2MD1shTdwzLPTFO16G8iEAU3A6mGo_vJTY=",
                    "airflow_secret_key": "a" * 16,
                    "airflow_ui_user_password": "password",
                    "airflow_ui_user_email": "password",
                    "postgres_password": "password",
                }
            }
        ]
        invalid_docs = [{}, {"observatory": {}}]

        expected_errors = [
            {"observatory": ["required field"]},
            {
                "observatory": [
                    {
                        "package": ["required field"],
                        "package_type": ["required field"],
                        "airflow_fernet_key": ["required field"],
                        "airflow_secret_key": ["required field"],
                        "airflow_ui_user_email": ["required field"],
                        "airflow_ui_user_password": ["required field"],
                        "postgres_password": ["required field"],
                    }
                ]
            },
        ]
        self.assert_sub_schema_valid(valid_docs, invalid_docs, schema, schema_key, expected_errors)

    def test_terraform_schema_database(self):
        # Test database schema
        schema = make_schema(BackendType.terraform)
        schema_key = "cloud_sql_database"

        valid_docs = [{"cloud_sql_database": {"tier": "db-custom-2-7680", "backup_start_time": "23:00"}}]
        invalid_docs = [
            {},
            {"cloud_sql_database": {}},
            {"cloud_sql_database": {"tier": 1, "backup_start_time": "2300"}},
        ]

        expected_errors = [
            {"cloud_sql_database": ["required field"]},
            {"cloud_sql_database": [{"backup_start_time": ["required field"], "tier": ["required field"]}]},
            {
                "cloud_sql_database": [
                    {
                        "backup_start_time": ["value does not match regex '^\\d{2}:\\d{2}$'"],
                        "tier": ["must be of string type"],
                    }
                ]
            },
        ]
        self.assert_sub_schema_valid(valid_docs, invalid_docs, schema, schema_key, expected_errors)

    def assert_vm_schema(self, schema_key: str):
        schema = make_schema(BackendType.terraform)
        valid_docs = [
            {
                schema_key: {
                    "machine_type": "n2-standard-2",
                    "disk_size": 1,
                    "disk_type": "pd-standard",
                    "create": False,
                }
            },
            {schema_key: {"machine_type": "n2-standard-2", "disk_size": 100, "disk_type": "pd-ssd", "create": True}},
        ]
        invalid_docs = [
            {},
            {schema_key: {}},
            {schema_key: {"machine_type": 1, "disk_size": 0, "disk_type": "develop", "create": "True"}},
        ]

        expected_errors = [
            {schema_key: ["required field"]},
            {
                schema_key: [
                    {
                        "create": ["required field"],
                        "disk_size": ["required field"],
                        "disk_type": ["required field"],
                        "machine_type": ["required field"],
                    }
                ]
            },
            {
                schema_key: [
                    {
                        "create": ["must be of boolean type"],
                        "disk_size": ["min value is 1"],
                        "disk_type": ["unallowed value develop"],
                        "machine_type": ["must be of string type"],
                    }
                ]
            },
        ]
        self.assert_sub_schema_valid(valid_docs, invalid_docs, schema, schema_key, expected_errors)

    def test_terraform_schema_vms(self):
        # Test VM schema
        self.assert_vm_schema("airflow_main_vm")
        self.assert_vm_schema("airflow_worker_vm")


def tmp_config_file(dict_: dict) -> str:
    """
    Dumps dict into a yaml file that is saved in a randomly named file. Used to as config file to create
    ObservatoryConfig instance.
    :param dict_: config dict
    :return: path of temporary file
    """
    content = yaml.safe_dump(dict_).replace("'!", "!").replace("':", ":")
    file_name = "".join(random.choices(string.ascii_lowercase, k=10))
    with open(file_name, "w") as f:
        f.write(content)
    return file_name


class TestObservatoryConfigGeneration(unittest.TestCase):
    def test_get_requirement_string(self):
        with CliRunner().isolated_filesystem():
            config = ObservatoryConfig()
            requirement = config.get_requirement_string("backend")
            self.assertEqual(requirement, "Required")

            requirement = config.get_requirement_string("google_cloud")
            self.assertEqual(requirement, "Optional")

    def test_save_observatory_config(self):
        config = ObservatoryConfig(
            terraform=Terraform(organization="myorg"),
            backend=Backend(type=BackendType.local, environment=Environment.staging),
            observatory=Observatory(
                package="observatory-platform",
                package_type="editable",
                observatory_home="home",
                postgres_password="pass",
                redis_port=111,
                airflow_ui_user_password="pass",
                airflow_ui_user_email="email@email",
                flower_ui_port=10,
                airflow_ui_port=23,
                docker_network_name="name",
                docker_compose_project_name="proj",
                docker_network_is_external=True,
                api_package="api",
                api_package_type="sdist",
                api_port=123,
            ),
            google_cloud=GoogleCloud(
                project_id="myproject",
                credentials="config.yaml",
                data_location="us",
            ),
            workflows_projects=[
                WorkflowsProject(package_name="myname", package="path", package_type="editable", dags_module="module")
            ],
        )

        with CliRunner().isolated_filesystem():
            file = "config.yaml"
            config.save(path=file)
            self.assertTrue(os.path.exists(file))

            loaded = ObservatoryConfig.load(file)

            self.assertEqual(loaded.backend, config.backend)
            self.assertEqual(loaded.observatory, config.observatory)
            self.assertEqual(loaded.terraform, config.terraform)
            self.assertEqual(loaded.google_cloud, config.google_cloud)
            self.assertEqual(loaded.workflows_projects, config.workflows_projects)

    def test_save_terraform_config(self):
        config = TerraformConfig(
            backend=Backend(type=BackendType.terraform, environment=Environment.staging),
            observatory=Observatory(package="observatory-platform", package_type="editable"),
            google_cloud=GoogleCloud(
                project_id="myproject",
                credentials="config.yaml",
                data_location="us",
                region="us-west1",
                zone="us-west1-a",
            ),
            terraform=Terraform(organization="myorg"),
            cloud_sql_database=CloudSqlDatabase(tier="test", backup_start_time="12:00"),
            airflow_main_vm=VirtualMachine(machine_type="aa", disk_size=1, disk_type="pd-standard", create=False),
            airflow_worker_vm=VirtualMachine(machine_type="bb", disk_size=1, disk_type="pd-ssd", create=True),
        )

        file = "config.yaml"

        with CliRunner().isolated_filesystem():
            config.save(path=file)
            self.assertTrue(os.path.exists(file))
            loaded = TerraformConfig.load(file)

            self.assertEqual(loaded.backend, config.backend)
            self.assertEqual(loaded.terraform, config.terraform)
            self.assertEqual(loaded.google_cloud, config.google_cloud)
            self.assertEqual(loaded.observatory, config.observatory)
            self.assertEqual(loaded.cloud_sql_database, config.cloud_sql_database)
            self.assertEqual(loaded.airflow_main_vm, config.airflow_main_vm)
            self.assertEqual(loaded.airflow_worker_vm, config.airflow_worker_vm)

    def test_save_observatory_config_defaults(self):
        config = ObservatoryConfig(
            backend=Backend(type=BackendType.local, environment=Environment.staging),
        )

        with CliRunner().isolated_filesystem():
            file = "config.yaml"
            config.save(path=file)
            self.assertTrue(os.path.exists(file))

            loaded = ObservatoryConfig.load(file)
            self.assertEqual(loaded.backend, config.backend)
            self.assertEqual(loaded.terraform, Terraform(organization=None))
            self.assertEqual(loaded.google_cloud.project_id, None)
            self.assertEqual(loaded.observatory, config.observatory)

    def test_save_terraform_config_defaults(self):
        config = TerraformConfig(
            backend=Backend(type=BackendType.terraform, environment=Environment.staging),
            observatory=Observatory(),
            google_cloud=GoogleCloud(
                project_id="myproject",
                credentials="config.yaml",
                data_location="us",
                region="us-west1",
                zone="us-west1-a",
            ),
            terraform=Terraform(organization="myorg"),
        )

        file = "config.yaml"

        with CliRunner().isolated_filesystem():
            config.save(path=file)
            self.assertTrue(os.path.exists(file))
            loaded = TerraformConfig.load(file)

            self.assertEqual(loaded.backend, config.backend)
            self.assertEqual(loaded.terraform, config.terraform)
            self.assertEqual(loaded.google_cloud, config.google_cloud)
            self.assertEqual(loaded.observatory, config.observatory)

            self.assertEqual(
                loaded.cloud_sql_database,
                CloudSqlDatabase(
                    tier="db-custom-2-7680",
                    backup_start_time="23:00",
                ),
            )

            self.assertEqual(
                loaded.airflow_main_vm,
                VirtualMachine(
                    machine_type="n2-standard-2",
                    disk_size=50,
                    disk_type="pd-ssd",
                    create=True,
                ),
            )

            self.assertEqual(
                loaded.airflow_worker_vm,
                VirtualMachine(
                    machine_type="n1-standard-8",
                    disk_size=3000,
                    disk_type="pd-standard",
                    create=False,
                ),
            )


class TestKeyCheckers(unittest.TestCase):
    def test_is_base64(self):
        text = b"bWFrZSB0aGlzIHZhbGlk"
        self.assertTrue(is_base64(text))

        text = b"This is invalid base64"
        self.assertFalse(is_base64(text))

    def test_is_secret_key(self):
        text = "invalid length"
        valid, message = is_secret_key(text)
        self.assertFalse(valid)
        self.assertEqual(message, "Secret key should be length >=16, but is length 14.")

        text = "a" * 16
        valid, message = is_secret_key(text)
        self.assertTrue(valid)
        self.assertEqual(message, None)

    def test_is_fernet_key(self):
        text = "invalid key"
        valid, message = is_fernet_key(text)
        self.assertFalse(valid)
        self.assertEqual(message, f"Key {text} could not be urlsafe b64decoded.")

        text = "IWt5jFGSw2MD1shTdwzLPTFO16G8iEAU3A6mGo_vJTY="
        valid, message = is_fernet_key(text)
        self.assertTrue(valid)
        self.assertEqual(message, None)

        text = "[]}{!*/~inv" * 4
        valid, message = is_fernet_key(text)
        self.assertFalse(valid)
        self.assertEqual(message, "Decoded Fernet key should be length 32, but is length 12.")
