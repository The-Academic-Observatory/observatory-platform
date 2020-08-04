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

import itertools
import os
import pathlib
import unittest
from typing import Union
from unittest.mock import patch

import pendulum
import yaml
from click.testing import CliRunner

import observatory_platform.dags
import observatory_platform.database.telescopes.schema
from observatory_platform.utils.config_utils import (ObservatoryConfig,
                                                     observatory_package_path,
                                                     observatory_home,
                                                     dags_path,
                                                     telescope_path,
                                                     SubFolder,
                                                     find_schema,
                                                     schema_path,
                                                     AirflowVar,
                                                     AirflowConn)
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

    def test_schema_path(self):
        actual_path = schema_path('telescopes')
        expected_path = pathlib.Path(observatory_platform.database.telescopes.schema.__file__).resolve()
        expected_path = str(pathlib.Path(*expected_path.parts[:-1]).resolve())
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


def parse_airflow_variables(airflow_class: Union[AirflowVar, AirflowConn]) -> list:
    """
    Parse airflow variables, create a dict for each variable with info on name, default value and whether variable is
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


def create_variables_dict(airflow_class: Union[AirflowVar, AirflowConn], valid: bool) -> dict:
    """
    Create dictionary of airflow variables. Contains name as key. Value is dependent on whether the dictionary should
    contain 'valid' values or not. If valid the value is either 'random-string' or the default value. If not valid, the
    value is None.
    :param airflow_class: Either AirflowVar or AirflowConn Enum class.
    :param valid: Determines whether dict should contain valid or invalid values for the variables.
    :return: dictionary with airflow variables
    """
    config_dict = {}
    variables = parse_airflow_variables(airflow_class)
    for variable in variables:
        if variable['default']:
            config_dict[variable['name']] = variable['default']
        else:
            if valid:
                config_dict[variable['name']] = 'random-string'
            else:
                config_dict[variable['name']] = None
    return config_dict


class TestObservatoryConfig(unittest.TestCase):

    def setUp(self) -> None:
        self.config_file_name = 'config.yaml'
        self.config_dict_complete_valid = {
            'google_application_credentials': '/path/to/google_application_credentials.json',
            'fernet_key': 'nUKEUmwh5Fs8pRSaYo-v4jSB5-zcf5_0TvG4uulhzsE=',
            'airflow_variables': create_variables_dict(AirflowVar, valid=True),
            'airflow_connections': create_variables_dict(AirflowConn, valid=True)}

        self.config_dict_complete_invalid = {'google_application_credentials': None,
            'fernet_key': 'nUKEUmwh5Fs8pRSaYo-v4jSB5-zcf5_0TvG4uulhzsE=',
            'airflow_variables': create_variables_dict(AirflowVar, valid=False),
            'airflow_connections': create_variables_dict(AirflowConn, valid=False)}

        self.config_dict_incomplete_valid = {
            'google_application_credentials': '/path/to/google_application_credentials.json',
            'fernet_key': 'nUKEUmwh5Fs8pRSaYo-v4jSB5-zcf5_0TvG4uulhzsE='}

    def test_is_valid(self):
        with CliRunner().isolated_filesystem():
            # Set google application credentials
            TestObservatoryConfig.set_google_application_credentials(self.config_dict_complete_valid)
            google_application_credentials_path = TestObservatoryConfig.set_google_application_credentials(
                self.config_dict_incomplete_valid)

            # All properties specified and valid
            complete_valid = ObservatoryConfig.from_dict(self.config_dict_complete_valid)
            self.assertTrue(complete_valid.is_valid)

            # All properties specified but some that are required are None so invalid
            complete_invalid = ObservatoryConfig.from_dict(self.config_dict_complete_invalid)
            self.assertFalse(complete_invalid.is_valid)

            # Some properties missing, but they are not required so still valid
            incomplete_valid = ObservatoryConfig.from_dict(self.config_dict_incomplete_valid)
            self.assertTrue(incomplete_valid.is_valid)

            # All properties specified but google_application_credentials doesn't exist
            os.remove(google_application_credentials_path)
            google_app_cred_not_exist = ObservatoryConfig.from_dict(self.config_dict_complete_valid)
            self.assertFalse(google_app_cred_not_exist.is_valid)

    def test_save(self):
        with CliRunner().isolated_filesystem():
            # Set google application credentials
            TestObservatoryConfig.set_google_application_credentials(self.config_dict_complete_valid)

            # Create test config and save
            config = ObservatoryConfig.from_dict(self.config_dict_complete_valid)
            config.save(self.config_file_name)

            # Check file exists
            self.assertTrue(os.path.isfile(self.config_file_name))

            # Check file contents is as expected
            with open(self.config_file_name, 'r') as f:
                actual_data = f.read()
            expected_data = yaml.safe_dump(self.config_dict_complete_valid)
            self.assertEqual(expected_data, actual_data)

    def test_load(self):
        with CliRunner().isolated_filesystem():
            # Set google application credentials
            TestObservatoryConfig.set_google_application_credentials(self.config_dict_complete_valid)

            # Save actual config
            with open(self.config_file_name, 'w') as f:
                data = yaml.safe_dump(self.config_dict_complete_valid)
                f.write(data)

            # Test that loaded config matches expected config
            expected_config = ObservatoryConfig.from_dict(self.config_dict_complete_valid)
            actual_config = ObservatoryConfig.load(self.config_file_name)
            self.assertEqual(expected_config, actual_config)

    def test_to_dict(self):
        with CliRunner().isolated_filesystem():
            # Set google application credentials
            TestObservatoryConfig.set_google_application_credentials(self.config_dict_complete_valid)

            # Check that to_dict works
            actual_dict = ObservatoryConfig.from_dict(self.config_dict_complete_valid).to_dict()
            self.assertEqual(self.config_dict_complete_valid, actual_dict)

    def test_make_default(self):
        config = ObservatoryConfig.make_default()
        config_dict = config.to_dict()

        # Check that Fernet key has been set
        self.assertIsNotNone(config.fernet_key)

        # Check that generated dictionary matches original. Remove Fernet key because it should be different
        # every time
        del config_dict['fernet_key']
        del self.config_dict_complete_invalid['fernet_key']
        self.assertDictEqual(self.config_dict_complete_invalid, config_dict)

    def test_from_dict(self):
        with CliRunner().isolated_filesystem():
            # Set google application credentials
            TestObservatoryConfig.set_google_application_credentials(self.config_dict_complete_valid)

            config = ObservatoryConfig.from_dict(self.config_dict_complete_valid)
            self.assertDictEqual(self.config_dict_complete_valid, config.to_dict())

    @staticmethod
    def set_google_application_credentials(dict_) -> str:
        # Make google application credentials
        credentials_file_path = os.path.join(pathlib.Path().absolute(), 'google_application_credentials.json')
        with open(credentials_file_path, 'w') as f:
            f.write('')
        dict_['google_application_credentials'] = credentials_file_path

        return credentials_file_path

    def test_airflow_enum(self):
        for obj in itertools.chain(AirflowConn, AirflowVar):
            # check if connection value is dictionary
            self.assertIsInstance(obj.value, dict)
            # check if connection contains 'name'
            self.assertIn('name', obj.value)
            # check if 'name' is not-empty string
            self.assertIsInstance(obj.value['name'], str)
            self.assertTrue(obj.value['name'].strip())

            # check if connection contains 'default'
            self.assertIn('default', obj.value)
            # default can be None, if it is not None, check that it's not empty
            if obj.value['default'] is not None:
                self.assertTrue(obj.value['default'])

            # check if connection contains 'schema'
            self.assertIn('schema', obj.value)
            # check if 'schema' is in dictionary
            self.assertIsInstance(obj.value['schema'], dict)
            # if schema isn't empty, check that it contains at least 'type' and 'required'
            if obj.value['schema']:
                self.assertIn('type', obj.value['schema'])
                self.assertIsInstance(obj.value['schema']['type'], str)
                self.assertIn('required', obj.value['schema'])
                self.assertIsInstance(obj.value['schema']['required'], bool)
