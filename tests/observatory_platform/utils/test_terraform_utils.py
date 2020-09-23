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

# Author: Aniek Roelofs

import copy
import json
import os
import tarfile
import unittest
from click.testing import CliRunner
from datetime import datetime

from observatory_platform.utils.terraform_utils import TerraformApi
from tests.observatory_platform.config import test_fixtures_path


class TestTerraformApi(unittest.TestCase):
    # TODO change to 'observatory-unit-testing' once latest version is available
    organisation = 'COKI-project'
    workspace = datetime.now().strftime("%d_%m_%Y-%H_%M_%S_%f")
    token = os.getenv('TESTS_TERRAFORM_TOKEN')
    terraform_api = TerraformApi(token)

    @classmethod
    def setUpClass(cls) -> None:
        cls.terraform_api.create_workspace(cls.organisation, cls.workspace, auto_apply=True, description='test',
                                           version="0.13.0-beta3")

        # set up workspace variables, varying 'hcl' and 'sensitive'
        cls.literal_variable = {
            'key': 'literal_variable',
            'value': 'some_text',
            'category': 'terraform',
            'description': 'a description',
            'hcl': False,
            'sensitive': False
        }
        cls.literal_variable_sensitive = {
            'key': 'literal_variable_sensitive',
            'value': 'some_text',
            'category': 'terraform',
            'description': 'a description',
            'hcl': False,
            'sensitive': True
        }
        cls.hcl_variable = {
            'key': 'hcl_variable',
            'value': '{"test1"="value1", "test2"="value2"}',
            'category': 'terraform',
            'description': 'a description',
            'hcl': True,
            'sensitive': False
        }
        cls.hcl_variable_sensitive = {
            'key': 'hcl_variable_sensitive',
            'value': '{"test1"="value1", "test2"="value2"}',
            'category': 'terraform',
            'description': 'a description',
            'hcl': True,
            'sensitive': True
        }
        cls.valid_variables = [cls.literal_variable, cls.literal_variable_sensitive, cls.hcl_variable,
                               cls.hcl_variable_sensitive]
        # invalid category
        cls.invalid_variable = {
            'key': 'invalid',
            'value': 'some_text',
            'category': 'invalid',
            'description': 'a description',
            'hcl': False,
            'sensitive': False
        }

    @classmethod
    def tearDownClass(cls) -> None:
        cls.terraform_api.delete_workspace(cls.organisation, cls.workspace)

    def test_token_from_file(self):
        """ Test if token from json file is correctly retrieved """
        token = "24asdfAAD.atlasv1.AD890asdnlqADn6daQdf"
        token_json = {
            "credentials": {
                "app.terraform.io": {
                    "token": token
                }
            }
        }
        with CliRunner().isolated_filesystem():
            with open('token.json', 'w') as f:
                json.dump(token_json, f)
            self.assertEqual(token, TerraformApi.token_from_file('token.json'))

    def test_create_delete_workspace(self):
        """ Test response codes of successfully creating a workspace and when trying to create a workspace that
        already exists. """
        # first time, successful
        response_code = self.terraform_api.create_workspace(self.organisation, 'unittest', auto_apply=True,
                                                            description='test', version="0.13.0-beta3")
        self.assertEqual(response_code, 201)

        # second time, workspace already exists
        response_code = self.terraform_api.create_workspace(self.organisation, 'unittest', auto_apply=True,
                                                            description='test', version="0.13.0-beta3")
        self.assertEqual(response_code, 422)

        # delete workspace
        response_code = self.terraform_api.delete_workspace(self.organisation, 'unittest')
        self.assertEqual(response_code, 200)

        # try to delete non-existing workspace
        response_code = self.terraform_api.delete_workspace(self.organisation, 'unittest')
        self.assertEqual(response_code, 404)

    def test_workspace_id(self):
        """ Test that workspace id returns string or raises SystemExit for invalid workspace"""
        workspace_id = self.terraform_api.workspace_id(self.organisation, self.workspace)
        self.assertIsInstance(workspace_id, str)

        with self.assertRaises(SystemExit):
            self.terraform_api.workspace_id(self.organisation, 'non-existing-workspace')

    def test_create_var_attributes(self):
        """ Test the attributes dict that is created """
        for var in self.valid_variables:
            # minimal parameters
            attributes = self.terraform_api.create_var_attributes(var['key'], var['value'])
            self.assertIsInstance(attributes, dict)
            expected_attributes = {
                'key': var['key'],
                'value': var['value'],
                'category': 'terraform',
                'description': '',
                'hcl': 'false',
                'sensitive': 'false'
            }
            self.assertEqual(attributes, expected_attributes)

            # all parameters
            attributes = self.terraform_api.create_var_attributes(var['key'], var['value'], var['category'],
                                                                  var['description'], var['hcl'], var['sensitive'])
            expected_attributes = {
                'key': var['key'],
                'value': var['value'],
                'category': var['category'],
                'description': var['description'],
                'hcl': str(var['hcl']).lower(),
                'sensitive': str(var['sensitive']).lower()
            }
            self.assertIsInstance(attributes, dict)
            self.assertEqual(attributes, expected_attributes)

        var = self.invalid_variable
        with self.assertRaises(SystemExit):
            self.terraform_api.create_var_attributes(var['key'], var['value'], var['category'], var['description'],
                                                     var['hcl'], var['sensitive'])

    def test_add_delete_workspace_variable(self):
        """ Test whether workspace variable is successfully added and deleted"""
        # get workspace id
        workspace_id = self.terraform_api.workspace_id(self.organisation, self.workspace)
        # test the vars
        for var in self.valid_variables:
            attributes = self.terraform_api.create_var_attributes(var['key'], var['value'], var['category'],
                                                                  var['description'], var['hcl'], var['sensitive'])
            # add variable
            var_id = self.terraform_api.add_workspace_variable(attributes, workspace_id)
            self.assertIsInstance(var_id, str)

            # raise error trying to add variable with key that already exists
            with self.assertRaises(SystemExit):
                self.terraform_api.add_workspace_variable(attributes, workspace_id)

            # delete variable
            response_code = self.terraform_api.delete_workspace_variable(var_id, workspace_id)
            self.assertEqual(response_code, 204)

            # raise error trying to delete variable that doesn't exist
            with self.assertRaises(SystemExit):
                self.terraform_api.delete_workspace_variable(var_id, workspace_id)

    def test_update_workspace_variable(self):
        """ Test updating variable both with empty attributes (meaning the var won't change) and updated attributes. """
        # get workspace id
        workspace_id = self.terraform_api.workspace_id(self.organisation, self.workspace)

        for var in self.valid_variables:
            attributes = self.terraform_api.create_var_attributes(var['key'], var['value'], var['category'],
                                                                  var['description'], var['hcl'], var['sensitive'])
            # add variable
            var_id = self.terraform_api.add_workspace_variable(attributes, workspace_id)

            # leave out all properties, the variable will be left unchanged
            empty_attributes = {}
            response_code = self.terraform_api.update_workspace_variable(empty_attributes, var_id, workspace_id)
            self.assertEqual(response_code, 200)

            # change key and value
            attributes['key'] += '_updated'
            attributes['value'] = 'updated'
            # key can not be changed for sensitive variables
            if var == self.literal_variable_sensitive or var == self.hcl_variable_sensitive:
                with self.assertRaises(SystemExit):
                    self.terraform_api.update_workspace_variable(attributes, var_id, workspace_id)
            else:
                response_code = self.terraform_api.update_workspace_variable(attributes, var_id, workspace_id)
                self.assertEqual(response_code, 200)

            # delete variable
            self.terraform_api.delete_workspace_variable(var_id, workspace_id)

    def test_list_workspace_variables(self):
        """ Test listing workspace variables and the returned response. """
        # get workspace id
        workspace_id = self.terraform_api.workspace_id(self.organisation, self.workspace)

        for var in self.valid_variables:
            attributes = self.terraform_api.create_var_attributes(var['key'], var['value'], var['category'],
                                                                  var['description'], var['hcl'], var['sensitive'])
            # add variable
            self.terraform_api.add_workspace_variable(attributes, workspace_id)

        workspace_vars = self.terraform_api.list_workspace_variables(workspace_id)
        for returned_var in workspace_vars:
            self.assertIsInstance(returned_var, dict)

            self.assertIn('id', returned_var)
            self.assertIsInstance(returned_var['id'], str)

            self.assertIn('type', returned_var)
            self.assertIsInstance(returned_var['type'], str)

            self.assertIn('relationships', returned_var)
            self.assertIsInstance(returned_var['relationships'], dict)

            self.assertIn('links', returned_var)
            self.assertIsInstance(returned_var['links'], dict)

            self.assertIn('attributes', returned_var)
            self.assertIn('created-at', returned_var['attributes'])
            self.assertIsInstance(returned_var['attributes']['created-at'], str)
            # compare created var attributes and returned var attributes. Returned attributes has 'created-at' as
            # extra field. Sensitive values are 'None'
            del returned_var['attributes']['created-at']
            linked_variable = next(
                var for var in self.valid_variables if var["key"] == returned_var['attributes']['key'])
            if linked_variable['sensitive']:
                linked_variable['value'] = None
            self.assertEqual(returned_var['attributes'], linked_variable)

            # delete variable
            self.terraform_api.delete_workspace_variable(returned_var['id'], workspace_id)

    def test_create_configuration_version(self):
        """ Test that configuration version is uploaded successfully """
        # get workspace id
        workspace_id = self.terraform_api.workspace_id(self.organisation, self.workspace)
        # create configuration version
        upload_url = self.terraform_api.create_configuration_version(workspace_id)
        self.assertIsInstance(upload_url, str)

    def test_upload_configuration_files(self):
        """ Test that configuration files are uploaded successfully """
        # get workspace id
        workspace_id = self.terraform_api.workspace_id(self.organisation, self.workspace)

        # create configuration version
        upload_url = self.terraform_api.create_configuration_version(workspace_id)

        configuration_path = os.path.join(test_fixtures_path(), 'utils', 'terraform_utils', 'main.tf')
        configuration_tar = 'conf.tar.gz'

        with CliRunner().isolated_filesystem():
            # create tar.gz file of main.tf
            with tarfile.open(configuration_tar, 'w:gz') as tar:
                tar.add(configuration_path, arcname=os.path.basename(configuration_path))

            # test that configuration was created successfully
            response_code = self.terraform_api.upload_configuration_files(upload_url, configuration_tar)
            self.assertEqual(response_code, 200)

    def test_create_run(self):
        """ Test creating a run (with auto-apply) """
        # get workspace id
        workspace_id = self.terraform_api.workspace_id(self.organisation, self.workspace)
        # create configuration version
        upload_url = self.terraform_api.create_configuration_version(workspace_id)

        configuration_path = os.path.join(test_fixtures_path(), 'utils', 'terraform_utils', 'main.tf')
        configuration_tar = 'conf.tar.gz'

        with CliRunner().isolated_filesystem():
            # create tar.gz file of main.tf
            with tarfile.open(configuration_tar, 'w:gz') as tar:
                tar.add(configuration_path, arcname=os.path.basename(configuration_path))
            # upload configuration files
            self.terraform_api.upload_configuration_files(upload_url, configuration_tar)

        print(workspace_id)
        # create run without target
        run_id = self.terraform_api.create_run(workspace_id, target_addrs=None, message="No target")
        print(run_id)
        # self.assertIsInstance(run_id, str)

        # create run with target
        run_id = self.terraform_api.create_run(workspace_id, target_addrs="random_id.random", message="Targeting "
                                                                                                      "random_id")
        print(run_id)
        # self.assertIsInstance(run_id, str)

    def test_get_run_details(self):
        """ Test retrieval of run details """
        # get workspace id
        workspace_id = self.terraform_api.workspace_id(self.organisation, self.workspace)
        # create configuration version
        upload_url = self.terraform_api.create_configuration_version(workspace_id)

        configuration_path = os.path.join(test_fixtures_path(), 'utils', 'terraform_utils', 'main.tf')
        configuration_tar = 'conf.tar.gz'

        with CliRunner().isolated_filesystem():
            # create tar.gz file of main.tf
            with tarfile.open(configuration_tar, 'w:gz') as tar:
                tar.add(configuration_path, arcname=os.path.basename(configuration_path))
            # upload configuration files
            self.terraform_api.upload_configuration_files(upload_url, configuration_tar)

        # possible states
        run_states = ['pending', 'plan_queued', 'planning', 'planned', 'cost_estimating', 'cost_estimated',
                      'policy_checking', 'policy_override', 'policy_soft_failed', 'policy_checked', 'confirmed',
                      'planned_and_finished', 'apply_queued', 'applying', 'applied', 'discarded', 'errored', 'canceled',
                      'force_canceled']

        # create run with target
        run_id = self.terraform_api.create_run(workspace_id, target_addrs="random_id.random", message="Targeting "
                                                                                                      "random_id")
        run_details = self.terraform_api.get_run_details(run_id)
        self.assertIsInstance(run_details, dict)
        run_status = run_details['data']['attributes']['status']
        self.assertIn(run_status, run_states)

        # create run without target
        run_id = self.terraform_api.create_run(workspace_id, target_addrs=None, message="No target")
        run_details = self.terraform_api.get_run_details(run_id)
        self.assertIsInstance(run_details, dict)
        run_status = run_details['data']['attributes']['status']
        self.assertIn(run_status, run_states)

    def test_plan_variable_changes(self):
        """ Test the lists that are returned by plan_variable_changes. """
        # get workspace id
        workspace_id = self.terraform_api.workspace_id(self.organisation, self.workspace)
        var_ids = {}
        for var in self.valid_variables[1:4]:
            attributes = self.terraform_api.create_var_attributes(var['key'], var['value'], var['category'],
                                                                  var['description'], var['hcl'], var['sensitive'])
            # add variable
            var_id = self.terraform_api.add_workspace_variable(attributes, workspace_id)
            var_ids[var['key']] = var_id

        new_vars = []
        # variable 0 will be added and variable 3 will be deleted compared to workspace_vars
        for var in copy.deepcopy(self.valid_variables[0:3]):
            new_vars.append(var)

        # leave variables 1 and 2 unchanged. Variable 1 is sensitive, so should be in the 'edit' list even when
        # unchanged
        add, edit, unchanged, delete = self.terraform_api.plan_variable_changes(new_vars, workspace_id)

        # Add, attributes of variable 0
        self.assertEqual(add, [self.valid_variables[0]])
        # Edit, tuple of details from variable 1
        var_id_1 = var_ids[self.valid_variables[1]['key']]
        self.assertEqual(edit, [(new_vars[1], var_id_1, 'sensitive')])
        # Unchanged, attributes of variable 2
        self.assertEqual(unchanged, [self.valid_variables[2]])
        # Delete, tuple of details from variable 3. Variable is sensitive so value not displayed
        key_3 = self.valid_variables[3]['key']
        var_id_3 = var_ids[key_3]
        self.assertEqual(delete, [(key_3, var_id_3, 'sensitive')])

        # change variables 1 and 2
        new_vars[1]['value'] = 'updated'
        new_vars[2]['value'] = 'updated'
        add, edit, unchanged, delete = self.terraform_api.plan_variable_changes(new_vars, workspace_id)

        # Add, attributes of variable 0
        self.assertEqual(add, [self.valid_variables[0]])
        # Edit, tuple of details from variable 1 & 2
        var_id_1 = var_ids[self.valid_variables[1]['key']]
        var_1 = (new_vars[1], var_id_1, 'sensitive')
        var_id_2 = var_ids[self.valid_variables[2]['key']]
        value_2 = self.valid_variables[2]['value']
        var_2 = (new_vars[2], var_id_2, value_2)
        self.assertEqual(edit, [var_1, var_2])
        # Unchanged, empty
        self.assertEqual(unchanged, [])
        # Delete, tuple of details from variable 3. Variable is sensitive so value not displayed
        key_3 = self.valid_variables[3]['key']
        var_id_3 = var_ids[key_3]
        self.assertEqual(delete, [(key_3, var_id_3, 'sensitive')])

        # delete variables
        for key, value in var_ids.items():
            self.terraform_api.delete_workspace_variable(value, workspace_id)

    def test_update_workspace_variables(self):
        """ Test whether variables in workspace are updated correctly. """
        # get workspace id
        workspace_id = self.terraform_api.workspace_id(self.organisation, self.workspace)
        var_ids = {}
        for var in self.valid_variables[1:4]:
            attributes = self.terraform_api.create_var_attributes(var['key'], var['value'], var['category'],
                                                                  var['description'], var['hcl'], var['sensitive'])
            # add variable
            var_id = self.terraform_api.add_workspace_variable(attributes, workspace_id)
            var_ids[var['key']] = var_id

        new_vars = []
        # variable 0 will be added and variable 3 will be deleted compared to workspace_vars
        for var in copy.deepcopy(self.valid_variables[0:3]):
            new_vars.append(var)

        # leave variable 1 and 2 unchanged. Variable 1 is sensitive, so should be in
        # the 'edit' list even when unchanged
        add, edit, unchanged, delete = self.terraform_api.plan_variable_changes(new_vars, workspace_id)

        # update variables
        self.terraform_api.update_workspace_variables(add, edit, delete, workspace_id)

        workspace_vars = self.terraform_api.list_workspace_variables(workspace_id)
        all_attributes = []
        for var in workspace_vars:
            # delete created at key so attributes can be compared
            del var['attributes']['created-at']
            all_attributes.append(var['attributes'])

        # variable 0 should be added, variable 1 edited, variable 2 should still exist and variable 3 not anymore
        new_vars[1]['value'] = None
        expected_attributes = [new_vars[0], new_vars[1], new_vars[2]]
        # check elements in lists are the same, regardless of their order
        self.assertCountEqual(all_attributes, expected_attributes)
