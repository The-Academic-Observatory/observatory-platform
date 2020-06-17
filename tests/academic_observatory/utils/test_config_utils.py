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

import yaml
from click.testing import CliRunner

import academic_observatory.dags
import academic_observatory.database.analysis.bigquery.schema
import academic_observatory.debug_files
from academic_observatory.utils.config_utils import observatory_home, telescope_path, bigquery_schema_path, \
    debug_file_path, SubFolder, ObservatoryConfig, Environment, observatory_package_path, dags_path


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
        self.assertTrue(os.path.exists(path))
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

    def test_bigquery_schema_path(self):
        schema_name = 'unpaywall.json'
        expected_path = pathlib.Path(academic_observatory.database.analysis.bigquery.schema.__file__).resolve()
        expected_path = str(pathlib.Path(*expected_path.parts[:-1], schema_name).resolve())
        actual_path = bigquery_schema_path(schema_name)
        self.assertEqual(expected_path, actual_path)
        self.assertTrue(os.path.exists(actual_path))

    def test_debug_file_path(self):
        debug_file = 'unpaywall.jsonl.gz'
        expected_path = pathlib.Path(academic_observatory.debug_files.__file__).resolve()
        expected_path = str(pathlib.Path(*expected_path.parts[:-1], debug_file).resolve())
        actual_path = debug_file_path(debug_file)
        self.assertEqual(expected_path, actual_path)
        self.assertTrue(os.path.exists(actual_path))

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
    CONFIG_DICT_COMPLETE_VALID = {
        'project_id': 'my-project',
        'bucket_name': 'my-bucket',
        'location': 'us-west4',
        'dags_path': '/usr/local/airflow/dags',
        'google_application_credentials': '/run/secrets/google_application_credentials.json',
        'environment': 'dev'
    }

    CONFIG_DICT_COMPLETE_INVALID = {
        'project_id': None,
        'bucket_name': None,
        'location': None,
        'dags_path': '/usr/local/airflow/dags',
        'google_application_credentials': '/run/secrets/google_application_credentials.json',
        'environment': 'dev'
    }

    CONFIG_DICT_INCOMPLETE_VALID = {
        'project_id': 'my-project',
        'bucket_name': 'my-bucket',
        'location': 'us-west4',
        'environment': 'dev'
    }

    def setUp(self) -> None:
        self.config_file_name = 'config.yaml'

    def test_is_valid(self):
        # All properties specified and valid
        complete_valid = ObservatoryConfig.from_dict(TestObservatoryConfig.CONFIG_DICT_COMPLETE_VALID)
        self.assertTrue(complete_valid.is_valid)

        # All properties specified but some that are required are None so invalid
        complete_invalid = ObservatoryConfig.from_dict(TestObservatoryConfig.CONFIG_DICT_COMPLETE_INVALID)
        self.assertFalse(complete_invalid.is_valid)

        # Some properties missing, but they are not required so still valid
        incomplete_valid = ObservatoryConfig.from_dict(TestObservatoryConfig.CONFIG_DICT_INCOMPLETE_VALID)
        self.assertTrue(incomplete_valid.is_valid)

    def test_save(self):
        runner = CliRunner()
        with runner.isolated_filesystem():
            # Create test config and save
            dict_ = TestObservatoryConfig.CONFIG_DICT_COMPLETE_VALID
            config = ObservatoryConfig.from_dict(dict_)
            config.save(self.config_file_name)

            # Check file exists
            self.assertTrue(os.path.exists(self.config_file_name))

            # Check file contents is as expected
            with open(self.config_file_name, 'r') as f:
                actual_data = f.read()
            expected_data = yaml.safe_dump(TestObservatoryConfig.CONFIG_DICT_COMPLETE_VALID)
            self.assertEqual(expected_data, actual_data)

    def test_load(self):
        runner = CliRunner()
        with runner.isolated_filesystem():
            # Save actual config
            with open(self.config_file_name, 'w') as f:
                data = yaml.safe_dump(TestObservatoryConfig.CONFIG_DICT_COMPLETE_VALID)
                f.write(data)

            # Test that loaded config matches expected config
            expected_config = ObservatoryConfig.from_dict(TestObservatoryConfig.CONFIG_DICT_COMPLETE_VALID)
            actual_config = ObservatoryConfig.load(self.config_file_name)
            self.assertEqual(expected_config, actual_config)

    def test_to_dict(self):
        # Check that to_dict works
        expected_dict = TestObservatoryConfig.CONFIG_DICT_COMPLETE_VALID
        actual_dict = ObservatoryConfig.from_dict(expected_dict).to_dict()
        self.assertEqual(expected_dict, actual_dict)

    def assert_dict_equals_config(self, dict_, config):
        self.assertEqual(dict_['project_id'], config.project_id)
        self.assertEqual(dict_['bucket_name'], config.bucket_name)
        self.assertEqual(dict_['location'], config.location)
        self.assertEqual(dict_['dags_path'], config.dags_path)
        self.assertEqual(dict_['google_application_credentials'], config.google_application_credentials)
        self.assertEqual(Environment(dict_['environment']), config.environment)

    def test_make_default(self):
        dict_ = TestObservatoryConfig.CONFIG_DICT_COMPLETE_INVALID
        config = ObservatoryConfig.make_default()
        self.assert_dict_equals_config(dict_, config)

    def test_from_dict(self):
        dict_ = TestObservatoryConfig.CONFIG_DICT_COMPLETE_VALID
        config = ObservatoryConfig.from_dict(dict_)
        self.assert_dict_equals_config(dict_, config)
