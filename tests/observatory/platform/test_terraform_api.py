# Copyright 2020 Curtin University. All Rights Reserved.
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

# Author: Aniek Roelofs, James Diprose

import json
import os
import tarfile
import unittest

from click.testing import CliRunner

from observatory.platform.terraform_api import TerraformApi, TerraformVariable
from observatory.platform.utils.test_utils import random_id, test_fixtures_path

VALID_VARIABLE_DICTS = [
    {
        "type": "vars",
        "attributes": {
            "key": "literal_variable",
            "value": "some_text",
            "sensitive": False,
            "category": "terraform",
            "hcl": False,
            "description": "a description",
        },
    },
    {
        "type": "vars",
        "attributes": {
            "key": "literal_variable_sensitive",
            "value": "some_text",
            "sensitive": True,
            "category": "terraform",
            "hcl": False,
            "description": "a description",
        },
    },
    {
        "type": "vars",
        "attributes": {
            "key": "hcl_variable",
            "value": '{"test1"="value1", "test2"="value2"}',
            "category": "terraform",
            "description": "a description",
            "hcl": True,
            "sensitive": False,
        },
    },
    {
        "type": "vars",
        "attributes": {
            "key": "hcl_variable_sensitive",
            "value": '{"test1"="value1", "test2"="value2"}',
            "category": "terraform",
            "description": "a description",
            "hcl": True,
            "sensitive": True,
        },
    },
]

INVALID_VARIABLE_DICT = {
    "type": "vars",
    "attributes": {
        "key": "invalid",
        "value": "some_text",
        "category": "invalid",
        "description": "a description",
        "hcl": False,
        "sensitive": False,
    },
}


class TestTerraformVariable(unittest.TestCase):
    def test_from_dict_to_dict(self):
        # Check that from and to dict work
        for expected_dict in VALID_VARIABLE_DICTS:
            var = TerraformVariable.from_dict(expected_dict)
            self.assertIsInstance(var, TerraformVariable)
            actual_dict = var.to_dict()
            self.assertDictEqual(expected_dict, actual_dict)

        # Test that value error is raised because of incorrect category
        with self.assertRaises(ValueError):
            TerraformVariable.from_dict(INVALID_VARIABLE_DICT)


class TestTerraformApi(unittest.TestCase):
    organisation = os.getenv("TEST_TERRAFORM_ORGANISATION")
    workspace = random_id()
    token = os.getenv("TEST_TERRAFORM_TOKEN")
    terraform_api = TerraformApi(token)
    version = TerraformApi.TERRAFORM_WORKSPACE_VERSION
    description = "test"

    def __init__(self, *args, **kwargs):
        super(TestTerraformApi, self).__init__(*args, **kwargs)

    @classmethod
    def setUpClass(cls) -> None:
        cls.terraform_api.create_workspace(
            cls.organisation, cls.workspace, auto_apply=True, description=cls.description, version=cls.version
        )

    @classmethod
    def tearDownClass(cls) -> None:
        cls.terraform_api.delete_workspace(cls.organisation, cls.workspace)

    def test_token_from_file(self):
        """Test if token from json file is correctly retrieved"""

        token = "24asdfAAD.atlasv1.AD890asdnlqADn6daQdf"
        token_json = {"credentials": {"app.terraform.io": {"token": token}}}

        with CliRunner().isolated_filesystem():
            with open("token.json", "w") as f:
                json.dump(token_json, f)
            self.assertEqual(token, TerraformApi.token_from_file("token.json"))

    def test_create_delete_workspace(self):
        """Test response codes of successfully creating a workspace and when trying to create a workspace that
        already exists."""

        # First time, successful
        workspace_id = self.workspace + "unittest"
        response_code = self.terraform_api.create_workspace(
            self.organisation, workspace_id, auto_apply=True, description=self.description, version=self.version
        )
        self.assertEqual(response_code, 201)

        # Second time, workspace already exists
        response_code = self.terraform_api.create_workspace(
            self.organisation, workspace_id, auto_apply=True, description=self.description, version=self.version
        )
        self.assertEqual(response_code, 422)

        # Delete workspace
        response_code = self.terraform_api.delete_workspace(self.organisation, workspace_id)
        self.assertEqual(response_code, 200)

        # Try to delete non-existing workspace
        response_code = self.terraform_api.delete_workspace(self.organisation, workspace_id)
        self.assertEqual(response_code, 404)

    def test_workspace_id(self):
        """Test that workspace id returns string or raises SystemExit for invalid workspace"""

        workspace_id = self.terraform_api.workspace_id(self.organisation, self.workspace)
        self.assertIsInstance(workspace_id, str)

        with self.assertRaises(SystemExit):
            self.terraform_api.workspace_id(self.organisation, "non-existing-workspace")

    def test_add_delete_workspace_variable(self):
        """Test whether workspace variable is successfully added and deleted"""

        # Get workspace id
        workspace_id = self.terraform_api.workspace_id(self.organisation, self.workspace)

        # Test the vars
        for var_dict in VALID_VARIABLE_DICTS:
            # Add variable
            var = TerraformVariable.from_dict(var_dict)
            var.var_id = self.terraform_api.add_workspace_variable(var, workspace_id)
            self.assertIsInstance(var.var_id, str)

            # Raise error trying to add variable with key that already exists
            with self.assertRaises(ValueError):
                self.terraform_api.add_workspace_variable(var, workspace_id)

            # Delete variable
            response_code = self.terraform_api.delete_workspace_variable(var, workspace_id)
            self.assertEqual(response_code, 204)

            # Raise error trying to delete variable that doesn't exist
            with self.assertRaises(ValueError):
                self.terraform_api.delete_workspace_variable(var, workspace_id)

    def test_update_workspace_variable(self):
        """Test updating variable both with empty attributes (meaning the var won't change) and updated attributes."""
        # get workspace id
        workspace_id = self.terraform_api.workspace_id(self.organisation, self.workspace)

        for var_dict in VALID_VARIABLE_DICTS:
            # Make variable
            var = TerraformVariable.from_dict(var_dict)

            # Add variable to workspace and set var_id
            var.var_id = self.terraform_api.add_workspace_variable(var, workspace_id)

            # Change key and value
            var.key = var.key + "_updated"
            var.value = "updated"

            # Key can not be changed for sensitive variables
            if var.sensitive:
                with self.assertRaises(ValueError):
                    self.terraform_api.update_workspace_variable(var, workspace_id)
            else:
                response_code = self.terraform_api.update_workspace_variable(var, workspace_id)
                self.assertEqual(response_code, 200)

            # Delete variable
            self.terraform_api.delete_workspace_variable(var, workspace_id)

    def test_list_workspace_variables(self):
        """Test listing workspace variables and the returned response."""
        # get workspace id
        workspace_id = self.terraform_api.workspace_id(self.organisation, self.workspace)
        expected_vars_index = dict()
        for var_dict in VALID_VARIABLE_DICTS:
            # Make variable
            var = TerraformVariable.from_dict(var_dict)

            # Add variable
            self.terraform_api.add_workspace_variable(var, workspace_id)

            # Add to index
            expected_vars_index[var.key] = var

        # Fetch workspace variables
        workspace_vars = self.terraform_api.list_workspace_variables(workspace_id)

        # Check that the number of variables created is correct
        self.assertTrue(len(VALID_VARIABLE_DICTS))

        # Check expected and actual
        for actual_var in workspace_vars:
            # Make variable
            expected_var = expected_vars_index[actual_var.key]
            expected_var.var_id = actual_var.var_id
            if expected_var.sensitive:
                expected_var.value = None

            # Check that variable is TerraformVariable instance
            self.assertIsInstance(actual_var, TerraformVariable)

            # Check that expected and actual variables match
            self.assertDictEqual(expected_var.to_dict(), actual_var.to_dict())

            # Delete variable
            self.terraform_api.delete_workspace_variable(actual_var, workspace_id)

    def test_create_configuration_version(self):
        """Test that configuration version is uploaded successfully"""
        # get workspace id
        workspace_id = self.terraform_api.workspace_id(self.organisation, self.workspace)

        # create configuration version
        upload_url, _ = self.terraform_api.create_configuration_version(workspace_id)
        self.assertIsInstance(upload_url, str)

    def test_check_configuration_version_status(self):
        # get workspace id
        workspace_id = self.terraform_api.workspace_id(self.organisation, self.workspace)

        # create configuration version
        _, configuration_id = self.terraform_api.create_configuration_version(workspace_id)

        # get status
        configuration_status = self.terraform_api.get_configuration_version_status(configuration_id)
        self.assertIn(configuration_status, ["pending", "uploaded", "errored"])

    def test_upload_configuration_files(self):
        """Test that configuration files are uploaded successfully"""
        # get workspace id
        workspace_id = self.terraform_api.workspace_id(self.organisation, self.workspace)

        # create configuration version
        upload_url, _ = self.terraform_api.create_configuration_version(workspace_id)

        configuration_path = test_fixtures_path("utils", "main.tf")
        configuration_tar = "conf.tar.gz"

        with CliRunner().isolated_filesystem():
            # create tar.gz file of main.tf
            with tarfile.open(configuration_tar, "w:gz") as tar:
                tar.add(configuration_path, arcname=os.path.basename(configuration_path))

            # test that configuration was created successfully
            response_code = self.terraform_api.upload_configuration_files(upload_url, configuration_tar)
            self.assertEqual(response_code, 200)

    def test_create_run(self):
        """Test creating a run (with auto-apply)"""
        # get workspace id
        workspace_id = self.terraform_api.workspace_id(self.organisation, self.workspace)
        # create configuration version
        upload_url, configuration_id = self.terraform_api.create_configuration_version(workspace_id)

        configuration_path = test_fixtures_path("utils", "main.tf")
        configuration_tar = "conf.tar.gz"

        with CliRunner().isolated_filesystem():
            # create tar.gz file of main.tf
            with tarfile.open(configuration_tar, "w:gz") as tar:
                tar.add(configuration_path, arcname=os.path.basename(configuration_path))
            # upload configuration files
            self.terraform_api.upload_configuration_files(upload_url, configuration_tar)

        # wait until configuration files are processed and uploaded
        configuration_status = None
        while configuration_status != "uploaded":
            configuration_status = self.terraform_api.get_configuration_version_status(configuration_id)

        # create run without target
        run_id = self.terraform_api.create_run(workspace_id, target_addrs=None, message="No target")
        self.assertIsInstance(run_id, str)

        # create run with target
        run_id = self.terraform_api.create_run(
            workspace_id, target_addrs="random_id.random", message="Targeting " "random_id"
        )
        self.assertIsInstance(run_id, str)

    def test_get_run_details(self):
        """Test retrieval of run details"""
        # get workspace id
        workspace_id = self.terraform_api.workspace_id(self.organisation, self.workspace)
        # create configuration version
        upload_url, configuration_id = self.terraform_api.create_configuration_version(workspace_id)

        configuration_path = test_fixtures_path("utils", "main.tf")
        configuration_tar = "conf.tar.gz"

        with CliRunner().isolated_filesystem():
            # create tar.gz file of main.tf
            with tarfile.open(configuration_tar, "w:gz") as tar:
                tar.add(configuration_path, arcname=os.path.basename(configuration_path))
            # upload configuration files
            self.terraform_api.upload_configuration_files(upload_url, configuration_tar)

        # wait until configuration files are processed and uploaded
        configuration_status = None
        while configuration_status != "uploaded":
            configuration_status = self.terraform_api.get_configuration_version_status(configuration_id)

        # possible states
        run_states = [
            "pending",
            "plan_queued",
            "planning",
            "planned",
            "cost_estimating",
            "cost_estimated",
            "policy_checking",
            "policy_override",
            "policy_soft_failed",
            "policy_checked",
            "confirmed",
            "planned_and_finished",
            "apply_queued",
            "applying",
            "applied",
            "discarded",
            "errored",
            "canceled",
            "force_canceled",
        ]

        # create run with target
        run_id = self.terraform_api.create_run(
            workspace_id, target_addrs="random_id.random", message="Targeting " "random_id"
        )
        run_details = self.terraform_api.get_run_details(run_id)
        self.assertIsInstance(run_details, dict)
        run_status = run_details["data"]["attributes"]["status"]
        self.assertIn(run_status, run_states)

        # create run without target
        run_id = self.terraform_api.create_run(workspace_id, target_addrs=None, message="No target")
        run_details = self.terraform_api.get_run_details(run_id)
        self.assertIsInstance(run_details, dict)
        run_status = run_details["data"]["attributes"]["status"]
        self.assertIn(run_status, run_states)

    def test_plan_variable_changes(self):
        """Test the lists that are returned by plan_variable_changes."""

        # Get workspace id
        workspace_id = self.terraform_api.workspace_id(self.organisation, self.workspace)

        # Make variables to create and update
        create_vars = [TerraformVariable.from_dict(var_dict) for var_dict in VALID_VARIABLE_DICTS[1:4]]
        update_vars = [TerraformVariable.from_dict(var_dict) for var_dict in VALID_VARIABLE_DICTS[0:3]]

        # Create variables
        for var in create_vars:
            self.terraform_api.add_workspace_variable(var, workspace_id)

        # Leave variable 1 and 2 unchanged. Variable 1 is sensitive, so should be in
        # the 'edit' list even when unchanged
        add, edit, unchanged, delete = self.terraform_api.plan_variable_changes(update_vars, workspace_id)

        # Check lengths
        expected_length = 1
        self.assertEqual(len(add), expected_length)
        self.assertEqual(len(edit), expected_length)
        self.assertEqual(len(unchanged), expected_length)
        self.assertEqual(len(delete), expected_length)

        # Add: should contain variable with key literal_variable (index 0 update_vars)
        self.assertDictEqual(update_vars[0].to_dict(), add[0].to_dict())

        # Edit: should contain a tuple of variables with key literal_variable_sensitive (index 0 create_vars),
        # the old variable and the new variable. var_id should not be None
        edit_old_var = edit[0][0]
        edit_new_var = edit[0][1]
        expected_key = "literal_variable_sensitive"
        self.assertEqual(edit_old_var.key, expected_key)
        self.assertEqual(edit_new_var.key, expected_key)
        self.assertIsNotNone(edit_old_var.var_id)
        self.assertIsNotNone(edit_new_var.var_id)

        # Unchanged: should contain variable with key hcl_variable
        self.assertDictEqual(create_vars[1].to_dict(), unchanged[0].to_dict())

        # Delete:
        expected_key = "hcl_variable_sensitive"
        delete_var = delete[0]
        self.assertEqual(delete_var.key, expected_key)
        self.assertIsNotNone(delete_var.var_id)

        # Change variables 1 and 2
        update_vars[1].value = "updated"
        update_vars[2].value = "updated"
        add, edit, unchanged, delete = self.terraform_api.plan_variable_changes(update_vars, workspace_id)

        # Check lengths of results
        self.assertEqual(len(add), 1)
        self.assertEqual(len(edit), 2)
        self.assertEqual(len(unchanged), 0)
        self.assertEqual(len(delete), 1)

        # Add: should contain variable with key literal_variable (index 0 update_vars)
        self.assertDictEqual(update_vars[0].to_dict(), add[0].to_dict())

        # Edit: literal_variable_sensitive and hcl_variable changed
        edit1_old_var = edit[0][0]
        edit1_new_var = edit[0][1]
        expected_key = "literal_variable_sensitive"
        self.assertEqual(edit1_old_var.key, expected_key)
        self.assertEqual(edit1_new_var.key, expected_key)
        self.assertIsNotNone(edit1_old_var.var_id)
        self.assertIsNotNone(edit1_new_var.var_id)

        edit2_old_var = edit[1][0]
        edit2_new_var = edit[1][1]
        expected_key = "hcl_variable"
        self.assertEqual(edit2_old_var.key, expected_key)
        self.assertEqual(edit2_new_var.key, expected_key)
        self.assertIsNotNone(edit2_old_var.var_id)
        self.assertIsNotNone(edit2_new_var.var_id)

        # Delete: key hcl_variable_sensitive
        expected_key = "hcl_variable_sensitive"
        delete_var = delete[0]
        self.assertEqual(delete_var.key, expected_key)
        self.assertIsNotNone(delete_var.var_id)

        # Delete variables
        workspace_vars = self.terraform_api.list_workspace_variables(workspace_id)
        for var in workspace_vars:
            self.terraform_api.delete_workspace_variable(var, workspace_id)

    def test_update_workspace_variables(self):
        """Test whether variables in workspace are updated correctly."""

        # Get workspace id
        workspace_id = self.terraform_api.workspace_id(self.organisation, self.workspace)

        # Create variables
        for var_dict in VALID_VARIABLE_DICTS[1:4]:
            var = TerraformVariable.from_dict(var_dict)
            self.terraform_api.add_workspace_variable(var, workspace_id)

        # Variable 0 will be added and variable 3 will be deleted compared to workspace_vars
        var_index = dict()
        new_vars = []
        for var_dict in VALID_VARIABLE_DICTS[0:3]:
            var = TerraformVariable.from_dict(var_dict)
            var_index[var.key] = var
            new_vars.append(var)

        # Leave variable 1 and 2 unchanged. Variable 1 is sensitive, so should be in
        # the 'edit' list even when unchanged
        add, edit, unchanged, delete = self.terraform_api.plan_variable_changes(new_vars, workspace_id)

        # Update variables
        self.terraform_api.update_workspace_variables(add, edit, delete, workspace_id)

        # Check that variables have been updated as expected
        workspace_vars = self.terraform_api.list_workspace_variables(workspace_id)

        # Should be same number of variables listed as created
        self.assertTrue(len(workspace_vars), len(new_vars))

        # Check that attributes match
        for actual_var in workspace_vars:
            self.assertIn(actual_var.key, var_index)

            # Make expected variable
            expected_var = var_index[actual_var.key]
            if expected_var.sensitive:
                expected_var.value = None
            expected_var.var_id = actual_var.var_id

            self.assertEqual(expected_var.to_dict(), actual_var.to_dict())
