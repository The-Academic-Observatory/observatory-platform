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

# Author: James Diprose

import os
import pathlib
import shutil
import unittest
from unittest.mock import patch

import pendulum
import yaml
from click.testing import CliRunner

import academic_observatory.dags
import academic_observatory.database.telescopes.schema
from academic_observatory.utils.config_utils import ObservatoryConfig, observatory_package_path, \
    dags_path, observatory_home, telescope_path, SubFolder, find_schema, schema_path
from academic_observatory.utils.test_utils import test_data_dir


class TestConfigUtils(unittest.TestCase):

    @patch('academic_observatory.utils.config_utils.pathlib.Path.home')
    def test_observatory_home_user_home(self, home_mock):
        # Tests with no OBSERVATORY_PATH environment variable
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
                path = observatory_home('telescopes')
                self.assertTrue(os.path.exists(path))
                self.assertEqual(f'{home_path}/.observatory/telescopes', path)

    @patch('academic_observatory.utils.config_utils.os.environ.get')
    def test_observatory_home_env_var(self, get_mock):
        root_path = '/tmp/df0acbb4-9690-4604-91f4-8f10ae3ebf58'
        observatory_home_path = os.path.join(root_path, 'home/airflow/gcs/data')
        os.makedirs(observatory_home_path, exist_ok=True)
        get_mock.return_value = observatory_home_path

        # Test that setting the OBSERVATORY_PATH environment variable works
        path = observatory_home()
        self.assertEqual(f'{observatory_home_path}/.observatory', path)

        # Test that subdirectories are created
        path = observatory_home('telescopes')
        self.assertTrue(os.path.exists(path))
        self.assertEqual(f'{observatory_home_path}/.observatory/telescopes', path)

        # Cleanup
        shutil.rmtree(root_path, ignore_errors=True)

        # Test that FileNotFoundError is thrown when invalid OBSERVATORY_PATH is set. The path in observatory_home_path
        # does not exist anymore due to the shutil.rmtree command.
        with self.assertRaises(FileNotFoundError):
            observatory_home()

    def test_observatory_package_path(self):
        expected_path = pathlib.Path(academic_observatory.__file__).resolve()
        expected_path = str(pathlib.Path(*expected_path.parts[:-2]).resolve())
        actual_path = observatory_package_path()
        self.assertEqual(expected_path, actual_path)
        self.assertTrue(os.path.exists(actual_path))

    def test_dags_path(self):
        expected_path = pathlib.Path(academic_observatory.dags.__file__).resolve()
        expected_path = str(pathlib.Path(*expected_path.parts[:-1]).resolve())
        actual_path = dags_path()
        self.assertEqual(expected_path, actual_path)
        self.assertTrue(os.path.exists(actual_path))

    def test_schema_path(self):
        actual_path = schema_path('telescopes')
        expected_path = pathlib.Path(academic_observatory.database.telescopes.schema.__file__).resolve()
        expected_path = str(pathlib.Path(*expected_path.parts[:-1]).resolve())
        self.assertEqual(expected_path, actual_path)
        self.assertTrue(os.path.exists(actual_path))

    def test_find_schema(self):
        schemas_path = os.path.join(test_data_dir(__file__), 'schemas')

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

    @patch('academic_observatory.utils.config_utils.pathlib.Path.home')
    def test_telescope_path(self, mock_pathlib_home):
        runner = CliRunner()
        with runner.isolated_filesystem():
            # Mock getting home path
            home_path = 'user-home'
            mock_pathlib_home.return_value = home_path

            # The name of the telescope to create and expected root folder
            telescope_name = 'grid'
            root_path = os.path.join(home_path, '.observatory', 'data', 'telescopes', telescope_name)

            # Create subdir
            path_downloaded = telescope_path(telescope_name, SubFolder.downloaded)
            expected = os.path.join(root_path, SubFolder.downloaded.value)
            self.assertEqual(expected, path_downloaded)
            self.assertTrue(os.path.exists(path_downloaded))

            # Create subdir
            path_extracted = telescope_path(telescope_name, SubFolder.extracted)
            expected = os.path.join(root_path, SubFolder.extracted.value)
            self.assertEqual(expected, path_extracted)
            self.assertTrue(os.path.exists(path_extracted))

            # Create subdir
            path_transformed = telescope_path(telescope_name, SubFolder.transformed)
            expected = os.path.join(root_path, SubFolder.transformed.value)
            self.assertEqual(expected, path_transformed)
            self.assertTrue(os.path.exists(path_transformed))


class TestObservatoryConfig(unittest.TestCase):

    def setUp(self) -> None:
        self.config_file_name = 'config.yaml'
        self.config_dict_complete_valid = {
            'project_id': 'my-project',
            'download_bucket_name': 'my-download-bucket',
            'transform_bucket_name': 'my-transform-bucket',
            'environment': 'dev',
            'google_application_credentials': '/path/to/google_application_credentials.json',
            'fernet_key': 'nUKEUmwh5Fs8pRSaYo-v4jSB5-zcf5_0TvG4uulhzsE=',
            'mag_releases_table_connection': 'mysql://azure-storage-account-name:url-encoded-sas-token@',
            'mag_snapshots_container_connection': 'mysql://azure-storage-account-name:url-encoded-sas-token@',
            'crossref_connection': 'mysql://:crossref-token@'
        }

        self.config_dict_complete_invalid = {
            'project_id': None,
            'download_bucket_name': None,
            'transform_bucket_name': None,
            'environment': 'dev',
            'google_application_credentials': None,
            'fernet_key': 'nUKEUmwh5Fs8pRSaYo-v4jSB5-zcf5_0TvG4uulhzsE=',
            'mag_releases_table_connection': 'mysql://azure-storage-account-name:url-encoded-sas-token@',
            'mag_snapshots_container_connection': 'mysql://azure-storage-account-name:url-encoded-sas-token@',
            'crossref_connection': 'mysql://:crossref-token@'
        }

        self.config_dict_incomplete_valid = {
            'project_id': 'my-project',
            'download_bucket_name': 'my-download-bucket',
            'transform_bucket_name': 'my-transform-bucket',
            'environment': 'dev',
            'google_application_credentials': '/path/to/google_application_credentials.json',
            'fernet_key': 'nUKEUmwh5Fs8pRSaYo-v4jSB5-zcf5_0TvG4uulhzsE='
        }

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

        # Check that fernet key has been set
        self.assertIsNotNone(config.fernet_key)

        # Check that generated dictionary matches original. Remove fernet key because it should be different
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
