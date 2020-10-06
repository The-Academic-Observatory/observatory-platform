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

import copy
import itertools
import os
import pathlib
import random
import string
import unittest
from typing import Union
from unittest.mock import patch

import pendulum
import yaml
from click.testing import CliRunner

import observatory_platform.dags
import observatory_platform.database.telescopes.schema
import observatory_platform.database.telescopes.sql
import observatory_platform.database.workflows.sql
import terraform
from observatory_platform.utils.config_utils import (AirflowConn,
                                                     AirflowVar,
                                                     ObservatoryConfig,
                                                     ObservatoryConfigValidator,
                                                     SubFolder,
                                                     customise_pointer,
                                                     dags_path,
                                                     find_schema,
                                                     get_default_airflow,
                                                     iterate_over_local_schema,
                                                     iterate_over_terraform_schema,
                                                     observatory_home,
                                                     observatory_package_path,
                                                     schema_path,
                                                     telescope_path,
                                                     telescope_templates_path,
                                                     terraform_variables_tf_path,
                                                     workflow_templates_path)
from tests.observatory_platform.config import test_fixtures_path


class TestConfigUtils(unittest.TestCase):

    @patch('observatory_platform.utils.config_utils.pathlib.Path.home')
    def test_observatory_home(self, home_mock):
        runner = CliRunner()
        with runner.isolated_filesystem():
            # Create home path and mock getting home path
            home_path = 'user-home'
            os.makedirs(home_path, exist_ok=True)
            home_mock.return_value = home_path

            with runner.isolated_filesystem():
                # Test that observatory home works
                path = observatory_home()
                self.assertTrue(os.path.exists(path))
                self.assertEqual(f'{home_path}/.observatory', path)

                # Test that subdirectories are created
                path = observatory_home('subfolder')
                self.assertTrue(os.path.exists(path))
                self.assertEqual(f'{home_path}/.observatory/subfolder', path)

    def test_observatory_package_path(self):
        expected_path = pathlib.Path(observatory_platform.__file__).resolve()
        expected_path = str(pathlib.Path(*expected_path.parts[:-2]).resolve())
        actual_path = observatory_package_path()
        self.assertEqual(expected_path, actual_path)
        self.assertTrue(os.path.exists(actual_path))

    def test_dags_path(self):
        expected_path = pathlib.Path(observatory_platform.dags.__file__).resolve()
        expected_path = str(pathlib.Path(*expected_path.parts[:-1]).resolve())
        actual_path = dags_path()
        self.assertEqual(expected_path, actual_path)
        self.assertTrue(os.path.exists(actual_path))

    def test_terraform_variables_tf_path(self):
        expected_path = pathlib.Path(terraform.__file__).resolve()
        expected_path = os.path.join(pathlib.Path(*expected_path.parts[:-1]), 'variables.tf')
        actual_path = terraform_variables_tf_path()
        self.assertEqual(expected_path, actual_path)
        self.assertTrue(os.path.exists(actual_path))

    def test_schema_path(self):
        actual_path = schema_path('telescopes')
        expected_path = pathlib.Path(observatory_platform.database.telescopes.schema.__file__).resolve()
        expected_path = str(pathlib.Path(*expected_path.parts[:-1]).resolve())
        self.assertEqual(expected_path, actual_path)
        self.assertTrue(os.path.exists(actual_path))

    def test_workflow_templates_path(self):
        expected_path = pathlib.Path(observatory_platform.database.workflows.sql.__file__).resolve()
        expected_path = str(pathlib.Path(*expected_path.parts[:-1]).resolve())
        actual_path = workflow_templates_path()
        self.assertEqual(expected_path, actual_path)
        self.assertTrue(os.path.exists(actual_path))

    def test_telescope_templates_path(self):
        expected_path = pathlib.Path(observatory_platform.database.telescopes.sql.__file__).resolve()
        expected_path = str(pathlib.Path(*expected_path.parts[:-1]).resolve())
        actual_path = telescope_templates_path()
        self.assertEqual(expected_path, actual_path)
        self.assertTrue(os.path.exists(actual_path))

    def test_find_schema(self):
        schemas_path = os.path.join(test_fixtures_path(), 'telescopes')

        # Tests that don't use a prefix
        table_name = 'grid'

        # 2020-09-21
        release_date = pendulum.datetime(year=2015, month=9, day=21)
        result = find_schema(schemas_path, table_name, release_date)
        self.assertIsNone(result)

        # 2020-09-22
        expected_schema = 'grid_2015-09-22.json'
        release_date = pendulum.datetime(year=2015, month=9, day=22)
        result = find_schema(schemas_path, table_name, release_date)
        self.assertIsNotNone(result)
        self.assertTrue(result.endswith(expected_schema))

        # 2015-09-23
        release_date = pendulum.datetime(year=2015, month=9, day=23)
        result = find_schema(schemas_path, table_name, release_date)
        self.assertIsNotNone(result)
        self.assertTrue(result.endswith(expected_schema))

        # 2020-04-28
        expected_schema = 'grid_2016-04-28.json'
        release_date = pendulum.datetime(year=2016, month=4, day=28)
        result = find_schema(schemas_path, table_name, release_date)
        self.assertIsNotNone(result)
        self.assertTrue(result.endswith(expected_schema))

        # 2016-04-29
        release_date = pendulum.datetime(year=2016, month=4, day=29)
        result = find_schema(schemas_path, table_name, release_date)
        self.assertIsNotNone(result)
        self.assertTrue(result.endswith(expected_schema))

        # Tests that use a prefix
        table_name = 'Papers'
        prefix = 'Mag'

        # 2020-05-20
        release_date = pendulum.datetime(year=2020, month=5, day=20)
        result = find_schema(schemas_path, table_name, release_date, prefix=prefix)
        self.assertIsNone(result)

        # 2020-05-21
        expected_schema = 'MagPapers2020-05-21.json'
        release_date = pendulum.datetime(year=2020, month=5, day=21)
        result = find_schema(schemas_path, table_name, release_date, prefix=prefix)
        self.assertIsNotNone(result)
        self.assertTrue(result.endswith(expected_schema))

        # 2020-05-22
        release_date = pendulum.datetime(year=2020, month=5, day=22)
        result = find_schema(schemas_path, table_name, release_date, prefix=prefix)
        self.assertIsNotNone(result)
        self.assertTrue(result.endswith(expected_schema))

        # 2020-06-05
        expected_schema = 'MagPapers2020-06-05.json'
        release_date = pendulum.datetime(year=2020, month=6, day=5)
        result = find_schema(schemas_path, table_name, release_date, prefix=prefix)
        self.assertIsNotNone(result)
        self.assertTrue(result.endswith(expected_schema))

        # 2020-06-06
        release_date = pendulum.datetime(year=2020, month=6, day=6)
        result = find_schema(schemas_path, table_name, release_date, prefix=prefix)
        self.assertIsNotNone(result)
        self.assertTrue(result.endswith(expected_schema))

    @patch('observatory_platform.utils.config_utils.airflow.models.Variable.get')
    def test_telescope_path(self, mock_variable_get):
        runner = CliRunner()
        with runner.isolated_filesystem():
            # Mock getting home path
            data_path = 'data'
            mock_variable_get.return_value = data_path

            # The name of the telescope to create and expected root folder
            telescope_name = 'grid'
            root_path = os.path.join(data_path, 'telescopes')

            # Create subdir
            path_downloaded = telescope_path(SubFolder.downloaded, telescope_name)
            expected = os.path.join(root_path, SubFolder.downloaded.value, telescope_name)
            self.assertEqual(expected, path_downloaded)
            self.assertTrue(os.path.exists(path_downloaded))

            # Create subdir
            path_extracted = telescope_path(SubFolder.extracted, telescope_name)
            expected = os.path.join(root_path, SubFolder.extracted.value, telescope_name)
            self.assertEqual(expected, path_extracted)
            self.assertTrue(os.path.exists(path_extracted))

            # Create subdir
            path_transformed = telescope_path(SubFolder.transformed, telescope_name)
            expected = os.path.join(root_path, SubFolder.transformed.value, telescope_name)
            self.assertEqual(expected, path_transformed)
            self.assertTrue(os.path.exists(path_transformed))

    def test_airflow_var(self):
        """ Test objects in both AirflowVar and AirflowConn classes"""
        for obj in itertools.chain(AirflowVar, AirflowConn):
            var = obj.value
            self.assertIsInstance(var, dict)
            self.assertIn('name', var)

            self.assertIn('default', var)
            # if not None, should be string
            if var['default']:
                self.assertIsInstance(var['default'], str)

            self.assertIn('schema', var)
            self.assertIsInstance(var['schema'], dict)
            # schema can be empty, if it's not empty it should contain at least 'type' and 'required' keys.
            if var['schema']:
                self.assertIn('type', var['schema'])
                self.assertIsInstance(var['schema']['type'], str)
                self.assertIn('required', var['schema'])
                self.assertIsInstance(var['schema']['required'], bool)
                # if the schema isn't empty, there should be a default value
                self.assertIsInstance(var['default'], str)

    def test_get_default_airflow(self):
        """ Test function that returns default value for each airflow var/conn"""
        for obj in AirflowVar:
            default_value = get_default_airflow(obj.value['name'], AirflowVar)
            if obj.value['schema']:
                self.assertEqual(default_value, obj.value['default'])
            else:
                self.assertEqual(default_value, None)

        for obj in AirflowConn:
            default_value = get_default_airflow(obj.value['name'], AirflowConn)
            if obj.value['schema']:
                self.assertEqual(default_value, obj.value['default'])
            else:
                self.assertEqual(default_value, None)


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


class TestObservatoryConfigValidator(unittest.TestCase):
    def setUp(self) -> None:
        self.schema = {
            'google_application_credentials': {
                'required': False,
                'type': 'string',
                'google_application_credentials': True
            },
            'crossref': {
                'required': False,
                'type': 'string',
                'isuri': True,
            },
            'variable': {
                'required': False,
                'type': 'string'
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
                'google_application_credentials': credentials_file_path
            })
            self.assertEqual(len(validator.errors), 0)

            # google_application_credentials tag and non-existing file
            validator.validate({
                'google_application_credentials': 'missing_file.json'
            })
            self.assertEqual(len(validator.errors), 1)

            # no google_application_credentials tag and non-existing file
            validator.validate({
                'variable': 'google_application_credentials.json'
            })
            self.assertEqual(len(validator.errors), 0)

            # no google_application_credentials tag and existing file
            validator.validate({
                'variable': 'google_application_credentials.json'
            })
            self.assertEqual(len(validator.errors), 0)

    def test_validate_isuri(self):
        """ Check if an error occurs for an invalid uri when the 'isuri' tag is present in the schema. """
        validator = ObservatoryConfigValidator(self.schema)
        # isuri tag and valid uri
        validator.validate({
            'crossref': 'mysql://myname:mypassword@myhost.com'
        })
        self.assertEqual(len(validator.errors), 0)

        # isuri tag and invalid uri
        validator.validate({
            'crossref': 'mysql://mypassword@myhost.com'
        })
        self.assertEqual(len(validator.errors), 1)

        # no uri tag and invalid uri
        validator.validate({
            'variable': 'mysql://mypassword@myhost.com'
        })
        self.assertEqual(len(validator.errors), 0)

        # no uri tag and valid uri
        validator.validate({
            'variable': 'mysql://myname:mypassword@myhost.com'
        })
        self.assertEqual(len(validator.errors), 0)


class TestLocalObservatoryConfig(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.config_dict_complete_valid = {
            'backend': 'local',
            'fernet_key': 'nUKEUmwh5Fs8pRSaYo-v4jSB5-zcf5_0TvG4uulhzsE=',
            'google_application_credentials': '/path/to/google_application_credentials.json',
            'airflow_variables': cls.create_variables_dict(AirflowVar, valid=True),
            'airflow_connections': cls.create_variables_dict(AirflowConn, valid=True)
        }
        cls.config_dict_incomplete_valid = {
            'backend': 'local',
            'fernet_key': 'nUKEUmwh5Fs8pRSaYo-v4jSB5-zcf5_0TvG4uulhzsE=',
            'google_application_credentials': '/path/to/google_application_credentials.json',
            'airflow_variables': cls.create_variables_dict(AirflowVar, valid=True)
        }
        cls.config_dict_complete_invalid = {
            'backend': 'local',
            'fernet_key': 'nUKEUmwh5Fs8pRSaYo-v4jSB5-zcf5_0TvG4uulhzsE=',
            'google_application_credentials': '/path/to/google_application_credentials.json',
            'airflow_variables': cls.create_variables_dict(AirflowVar, valid=False),
            'airflow_connections': cls.create_variables_dict(AirflowConn, valid=False)
        }
        cls.config_dict_incomplete_invalid = {
            'backend': 'local',
            'fernet_key': 'nUKEUmwh5Fs8pRSaYo-v4jSB5-zcf5_0TvG4uulhzsE=',
            'google_application_credentials': '/path/to/google_application_credentials.json'
        }

    def test_load(self):
        """ Test if config is loaded from a yaml file correctly """
        with CliRunner().isolated_filesystem():
            self.set_google_application_credentials()
            config_path = tmp_config_file(self.config_dict_complete_valid)

            # Test that loaded config matches expected config
            expected_config = self.config_dict_complete_valid
            actual_config = ObservatoryConfig(config_path).load()
            self.assertEqual(expected_config, actual_config)

    def test_get_schema(self):
        """ Test that the created schema is in line with the values in AirflowVar/AirflowConn class """
        schema = ObservatoryConfig.get_schema('local')
        self.assertIsInstance(schema, dict)
        self.assertEqual(schema['backend'], {
            'required': True,
            'type': 'string',
            'check_with': customise_pointer
        })
        self.assertEqual(schema['fernet_key'], {
            'required': True,
            'type': 'string',
            'check_with': customise_pointer
        })
        self.assertEqual(schema['google_application_credentials'], {
            'type': 'string',
            'google_application_credentials': True,
            'required': True,
            'check_with': customise_pointer
        })
        self.assertIn('airflow_connections', schema)
        self.assertIsInstance(schema['airflow_connections'], dict)
        self.assertIn('airflow_variables', schema)
        self.assertIsInstance(schema['airflow_variables'], dict)
        for obj in itertools.chain(AirflowConn, AirflowVar):
            if obj.value['schema']:
                if type(obj) == AirflowConn:
                    # if connection should check for valid uri
                    obj.value['schema']['isuri'] = True
                    self.assertEqual(schema['airflow_connections']['schema'][obj.get()], obj.value['schema'])
                else:
                    self.assertEqual(schema['airflow_variables']['schema'][obj.get()], obj.value['schema'])

    def test_is_valid(self):
        """ Test if validation works as expected when using valid/invalid config files."""
        with CliRunner().isolated_filesystem():
            self.set_google_application_credentials()

            # All properties specified and valid
            config_path = tmp_config_file(self.config_dict_complete_valid)
            valid_config = ObservatoryConfig(config_path)
            self.assertTrue(valid_config.is_valid)

            # Some properties missing, but they are not required so still valid
            config_path = tmp_config_file(self.config_dict_incomplete_valid)
            self.assertTrue(ObservatoryConfig(config_path).is_valid)

            # All properties specified, but some that are required are None so invalid
            config_path = tmp_config_file(self.config_dict_complete_invalid)
            self.assertFalse(ObservatoryConfig(config_path).is_valid)

            # Some properties missing, and required properties are None so invalid
            config_path = tmp_config_file(self.config_dict_incomplete_invalid)
            self.assertFalse(ObservatoryConfig(config_path).is_valid)

            # All properties specified but google_application_credentials doesn't exist
            os.remove(valid_config.local.google_application_credentials)
            self.assertFalse(valid_config.is_valid)

    def test_iterate_over_local_schema(self):
        """ Test method that iterates over schema and is used to print a default config """
        with CliRunner().isolated_filesystem():
            # Make google application credentials
            credentials_file_path = os.path.join(pathlib.Path().absolute(), 'google_application_credentials.json')
            with open(credentials_file_path, 'w') as f:
                f.write('')

            schema = ObservatoryConfig.get_schema('local')
            print_dict = iterate_over_local_schema(schema, {}, {}, test_config=False)
            # create dict that can be read in by config class and should be valid
            valid_default_dict = {}
            for key, value in print_dict.items():
                # check that variable only starts with '#' if it's not required
                if key.startswith('#'):
                    trimmed_key = key[1:]
                    self.assertFalse(schema[trimmed_key]['required'])
                else:
                    trimmed_key = key
                    self.assertTrue(schema[key]['required'])

                if isinstance(value, dict):
                    valid_default_dict[trimmed_key] = {}
                    for key2, value2 in value.items():
                        # check that nested variables only start with '#' if they are not required
                        if key2.startswith('#'):
                            trimmed_key2 = key2[1:]
                            self.assertFalse(schema[trimmed_key]['schema'][trimmed_key2]['required'])
                        else:
                            trimmed_key2 = key2
                            self.assertTrue(schema[trimmed_key]['schema'][key2]['required'])
                        # strip <-- from nested value
                        valid_default_dict[trimmed_key][trimmed_key2] = value2.strip(' <--')
                elif key == 'google_application_credentials':
                    # set value to existing path
                    valid_default_dict[trimmed_key] = credentials_file_path
                else:
                    # strip <-- from value
                    valid_default_dict[trimmed_key] = value.strip(' <--')
            config_path = tmp_config_file(valid_default_dict)
            default_config = ObservatoryConfig(config_path)
            self.assertTrue(default_config.is_valid)

            # create dict with test_config and check if it gives valid config instance
            test_dict = iterate_over_local_schema(schema, {}, {}, test_config=True)
            test_dict['google_application_credentials'] = credentials_file_path
            config_path = tmp_config_file(test_dict)
            test_config = ObservatoryConfig(config_path)
            self.assertTrue(test_config.is_valid)

    def test_save_default(self):
        """ Test if default config file is saved """
        with CliRunner().isolated_filesystem():
            # Set google application credentials
            self.set_google_application_credentials()

            # Create example config and save
            example_config_path = 'example_config_path.yaml'
            ObservatoryConfig.save_default('local', example_config_path)
            # Check file exists
            self.assertTrue(os.path.isfile(example_config_path))

            # Create test config and save
            test_config_path = 'test_config_path.yaml'
            ObservatoryConfig.save_default('local', test_config_path, test=True)
            # Check file exists
            self.assertTrue(os.path.isfile(test_config_path))

    def set_google_application_credentials(self) -> str:
        """ For valid configs, set google application credentials to path of existing file. """
        # Make google application credentials
        credentials_file_path = os.path.join(pathlib.Path().absolute(), 'google_application_credentials.json')
        with open(credentials_file_path, 'w') as f:
            f.write('')
        self.config_dict_complete_valid['google_application_credentials'] = credentials_file_path
        self.config_dict_incomplete_valid['google_application_credentials'] = credentials_file_path

        return credentials_file_path

    @staticmethod
    def parse_airflow_variables(airflow_class: Union[AirflowVar, AirflowConn]) -> list:
        """
        Parse airflow variables, create a dict for each variable with info on name, default value and whether
        variable is
        required or not.
        :param airflow_class: Either AirflowVar or AirflowConn Enum class.
        :return: List of dictionaries with variable info
        """
        variables = []
        for variable in airflow_class:
            variable_info = {}
            if variable.value['schema']:
                variable_info['name'] = variable.value['name']
            else:
                # skip variables that don't have a schema defined, these are hardcoded in e.g. docker file
                continue
            if variable.value['default']:
                variable_info['default'] = variable.value['default']
            else:
                variable_info['default'] = None
            required = variable.value['schema']['required']
            if required:
                variable_info['required'] = True
            else:
                variable_info['required'] = False
            variables.append(variable_info)
        return variables

    @staticmethod
    def create_variables_dict(airflow_class: Union[AirflowVar, AirflowConn], valid: bool) -> dict:
        """
        Create dictionary of airflow variables. Contains name as key. Value is dependent on whether the dictionary
        should
        contain 'valid' values or not. If valid the value is either 'random-string' or the default value. If not
        valid, the
        value is None.
        :param airflow_class: Either AirflowVar or AirflowConn Enum class.
        :param valid: Determines whether dict should contain valid or invalid values for the variables.
        :return: dictionary with airflow variables
        """
        config_dict = {}
        variables = TestLocalObservatoryConfig.parse_airflow_variables(airflow_class)
        for variable in variables:
            if valid:
                config_dict[variable['name']] = variable['default'].strip(' <--')
            else:
                config_dict[variable['name']] = None
        return config_dict


class TestTerraformObservatoryConfig(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.config_dict_complete_valid = {
            'backend': {
                'terraform': {
                    'organization': 'your-organization',
                    'workspaces_prefix': 'observatory-'
                }
            },
            'project_id': 'your-project-id',
            'google_application_credentials': '/path/to/service/account/key.json',
            'region': 'us-west1',
            'zone': 'us-west1-c',
            'data_location': 'us',
            'environment': 'dev',
            'database': {
                'tier': 'db-custom-4-15360',
                'backup_start_time': '23:00'
            },
            'airflow_main': {
                'machine_type': 'n2-standard-2',
                'disk_size': 20,
                'disk_type': 'pd-ssd'
            },
            'airflow_worker': {
                'machine_type': 'n1-standard-2',
                'disk_size': 20,
                'disk_type': 'pd-standard'
            },
            'airflow_worker_create': False,
            'airflow_secrets': {
                'fernet_key': 'eUO4rUub_3XiyY8ltOjPNl2e9SXD4WdOe6tXVyH7N2E=',
                'postgres_password': 'random_password$#@=?%^Q^$',
                'redis_password': 'random_password$#@=?%^Q^$',
                'airflow_ui_user_password': 'random_password',
                'airflow_ui_user_email': 'your.email@somehost.com'
            },
            'airflow_connections': {
                'terraform': 'mysql://:terraform-token@',
                'slack': 'https://:T00000000%2FB00000000%2FXXXXXXXXXXXXXXXXXXXXXXXX@https%3A%2F%2Fhooks.slack.com'
                         '%2Fservices',
                'crossref': 'mysql://myname:mypassword@myhost.com',
                'mag_releases_table': 'mysql://myname:mypassword@myhost.com',
                'mag_snapshots_container': 'mysql://myname:mypassword@myhost.com'
            },
            'airflow_variables': {
                'example_name': 'example_value'
            }
        }
        cls.config_dict_incomplete_valid = copy.deepcopy(cls.config_dict_complete_valid)
        del cls.config_dict_incomplete_valid['airflow_connections']['crossref']
        del cls.config_dict_incomplete_valid['airflow_connections']['mag_releases_table']
        del cls.config_dict_incomplete_valid['airflow_variables']

        cls.config_dict_complete_invalid = copy.deepcopy(cls.config_dict_complete_valid)
        cls.config_dict_complete_invalid['airflow_connections']['terraform'] = None
        cls.config_dict_complete_invalid['region'] = None

        cls.config_dict_incomplete_invalid = copy.deepcopy(cls.config_dict_complete_valid)
        del cls.config_dict_incomplete_invalid['airflow_secrets']
        del cls.config_dict_incomplete_invalid['zone']

    def test_load(self):
        """ Test if config is loaded from a yaml file correctly """
        with CliRunner().isolated_filesystem():
            self.set_google_application_credentials()
            config_path = tmp_config_file(self.config_dict_complete_valid)

            # Test that loaded config matches expected config
            expected_config = self.config_dict_complete_valid
            actual_config = ObservatoryConfig(config_path).load()
            self.assertEqual(expected_config, actual_config)

    def test_get_schema(self):
        """ Test that the created schema is in line with the values in variables.tf file """
        schema = ObservatoryConfig.get_schema('terraform', terraform_variables_tf_path())
        self.assertIsInstance(schema, dict)
        self.assertEqual(schema['backend'], {
            'required': True,
            'type': 'dict',
            'schema': {
                'terraform': {
                    'required': True,
                    'type': 'dict',
                    'schema': {
                        'organization': {
                            'required': True,
                            'type': 'string',
                            'meta': {
                                'default': 'your-organization <--'
                            }
                        },
                        'workspaces_prefix': {
                            'required': True,
                            'type': 'string',
                            'meta': {
                                'default': 'observatory- <--'
                            }
                        }
                    },
                }
            }
        })
        for var, value in schema.items():
            # every variable should have at least these fields
            self.assertIn('required', value)
            self.assertIn('type', value)
            # backend has slightly different structure (two times nested)
            if var == 'backend':
                continue
            self.assertIn('meta', value)
            self.assertIn('description', value['meta'])
            # nested variables
            try:
                for key2, value2 in value['schema'].items():
                    self.assertIn('required', value2)
                    self.assertIn('type', value2)
                    self.assertIn('default', value2['meta'])
            # unnested variables
            except KeyError:
                self.assertIn('default', value['meta'])

            if var == 'airflow_connections':
                for key2, value2 in value['schema'].items():
                    # connections should have isuri field
                    self.assertEqual(value2['isuri'], True)
                    # if the connection isn't required there should be a real default value
                    if value2['required'] is False:
                        self.assertIn('default', value2)
            # boolean should be lowercase
            if value['type'] == 'boolean':
                self.assertTrue(value['meta']['default'] in ['false', 'true'])

    def test_is_valid(self):
        """ Check that instances using the dicts defined above are valid/invalid as expected."""
        with CliRunner().isolated_filesystem():
            self.set_google_application_credentials()

            # All properties specified and valid
            config_path = tmp_config_file(self.config_dict_complete_valid)
            valid_config = ObservatoryConfig(config_path)
            self.assertTrue(valid_config.is_valid)

            # Some properties missing, but they are not required so still valid
            config_path = tmp_config_file(self.config_dict_incomplete_valid)
            self.assertTrue(ObservatoryConfig(config_path).is_valid)

            # All properties specified, but some that are required are None so invalid
            config_path = tmp_config_file(self.config_dict_complete_invalid)
            self.assertFalse(ObservatoryConfig(config_path).is_valid)

            # Some properties missing, and required properties are None so invalid
            config_path = tmp_config_file(self.config_dict_incomplete_invalid)
            self.assertFalse(ObservatoryConfig(config_path).is_valid)

            # All properties specified but google_application_credentials doesn't exist
            os.remove(valid_config.terraform.google_application_credentials)
            self.assertFalse(valid_config.is_valid)

    def test_iterate_over_terraform_schema(self):
        """ Check that iterate_over_terraform_schema returns a valid schema. Fields such as 'required' and
        'sensitive' are checked. The resulting schema is used to create an ObservatoryConfig instance and a check is
        done to see if this instance is valid. Tags ('!sensitive', '#') are removed from the keys as well as ' <--'
        from the values. """
        with CliRunner().isolated_filesystem():
            # Make google application credentials
            credentials_file_path = os.path.join(pathlib.Path().absolute(), 'google_application_credentials.json')
            with open(credentials_file_path, 'w') as f:
                f.write('')

            schema = ObservatoryConfig.get_schema('terraform', terraform_variables_tf_path())
            default_dict = iterate_over_terraform_schema(schema, {}, {}, test_config=False)
            # create dict that can be read in by config class and should be valid
            valid_default_dict = {}
            for key, value in default_dict.items():
                # check that variable only starts with '#' if it's not required and with '!sensitive' for sensitive
                # variables
                required = True
                sensitive = False
                if key.startswith('#'):
                    trimmed_key = key[1:]
                    required = False
                else:
                    trimmed_key = key
                if trimmed_key.startswith('!sensitive '):
                    trimmed_key = trimmed_key[11:]
                    sensitive = True

                if sensitive:
                    self.assertTrue(
                        trimmed_key in ['google_application_credentials', 'airflow_secrets', 'airflow_connections'])
                else:
                    self.assertFalse(
                        trimmed_key in ['google_application_credentials', 'airflow_secrets', 'airflow_connections'])
                if required:
                    self.assertTrue(schema[trimmed_key]['required'])
                else:
                    self.assertFalse(schema[trimmed_key]['required'])

                # backend has slightly different structure
                if key == 'backend':
                    valid_default_dict['backend'] = {
                        'terraform': {
                            'organization': 'your-organization',
                            'workspaces_prefix': 'observatory-'
                        }
                    }
                    continue
                # nested variables
                if isinstance(value, dict):
                    valid_default_dict[trimmed_key] = {}
                    for key2, value2 in value.items():
                        required = True
                        if key2.startswith('#'):
                            trimmed_key2 = key2[1:]
                            required = False
                        else:
                            trimmed_key2 = key2
                        # not all variables have a schema (e.g. 'airflow_variables')
                        try:
                            schema2 = schema[trimmed_key]['schema']
                            # check that nested variables only start with '#' if they are not required
                            if required:
                                self.assertTrue(schema2[key2]['required'])
                            else:
                                self.assertFalse(schema2[trimmed_key2]['required'])
                        except KeyError:
                            value2 = value2.strip(' <--')
                            valid_default_dict[trimmed_key][trimmed_key2] = value2
                            continue

                        # strip <-- from nested value and transform to right format based on schema type
                        if schema2[trimmed_key2]['type'] == 'string':
                            value2 = value2.strip(' <--')
                        elif schema2[trimmed_key2]['type'] == 'boolean':
                            value2 = eval(value2.strip(' <--').capitalize())
                        elif schema2[trimmed_key2]['type'] == 'number':
                            value2 = int(str(value2).strip(' <--'))
                        valid_default_dict[trimmed_key][trimmed_key2] = value2

                elif trimmed_key == 'google_application_credentials':
                    # set value to existing path
                    valid_default_dict[trimmed_key] = credentials_file_path
                else:
                    # strip <-- from value and transform to right format based on schema type
                    if schema[trimmed_key]['type'] == 'string':
                        value = value.strip(' <--')
                    elif schema[trimmed_key]['type'] == 'boolean':
                        value = eval(value.strip(' <--').capitalize())
                    elif schema[trimmed_key]['type'] == 'number':
                        value = int(str(value).strip(' <--'))
                    valid_default_dict[trimmed_key] = value

            config_path = tmp_config_file(valid_default_dict)
            default_config = ObservatoryConfig(config_path)
            # check if config is valid using the values from the schema
            self.assertTrue(default_config.is_valid)

            # create dict with test_config and check if it gives valid config instance
            test_dict = iterate_over_terraform_schema(schema, {}, {}, test_config=True)
            test_dict['google_application_credentials'] = credentials_file_path
            config_path = tmp_config_file(test_dict)
            test_config = ObservatoryConfig(config_path)
            self.assertTrue(test_config.is_valid)

    def test_save_default(self):
        """ Test if default config file is saved """
        with CliRunner().isolated_filesystem():
            # Set google application credentials
            self.set_google_application_credentials()

            # Create example config and save
            example_config_path = 'example_config_path.yaml'
            ObservatoryConfig.save_default('terraform', example_config_path, terraform_variables_tf_path())
            # Check file exists
            self.assertTrue(os.path.isfile(example_config_path))

            # Create test config and save
            test_config_path = 'test_config_path.yaml'
            ObservatoryConfig.save_default('terraform', test_config_path, terraform_variables_tf_path(), test=True)
            # Check file exists
            self.assertTrue(os.path.isfile(test_config_path))

    def test_yaml_constructor(self):
        """ Checks whether variables with a !sensitive tag are added to the config's sensitive_variables dict as
        expected."""
        with CliRunner().isolated_filesystem():
            # Set google application credentials
            self.set_google_application_credentials()

            # Create default config and save, this should contain variables with '!sensitive' tag
            default_config_path = 'default_config_path.yaml'
            ObservatoryConfig.save_default('terraform', default_config_path, terraform_variables_tf_path())
            # create config instance from file
            default_config = ObservatoryConfig(default_config_path)

            # retrieve sensitive variables from file
            with open(default_config_path, 'r') as f:
                config_lines = f.readlines()
            sensitive_variables = []
            for line in config_lines:
                if line.startswith('!sensitive '):
                    line = line.split(':')[0]
                    sensitive_variables.append(line[11:])
            # compare sensitive variables from config and sensitive variables from file
            self.assertEqual(default_config.sensitive_variables, sensitive_variables)

    def set_google_application_credentials(self) -> str:
        """ For valid configs, set google application credentials to path of existing file. """
        # Make google application credentials
        credentials_file_path = os.path.join(pathlib.Path().absolute(), 'google_application_credentials.json')
        with open(credentials_file_path, 'w') as f:
            f.write('')
        self.config_dict_complete_valid['google_application_credentials'] = credentials_file_path
        self.config_dict_incomplete_valid['google_application_credentials'] = credentials_file_path

        return credentials_file_path
