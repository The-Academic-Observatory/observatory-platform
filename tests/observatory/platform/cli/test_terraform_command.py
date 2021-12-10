# Copyright 2021 Curtin University
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

import os
from unittest.mock import MagicMock, PropertyMock, call, patch

from click.testing import CliRunner
from observatory.platform.cli.click_utils import INDENT1, INDENT2, indent
from observatory.platform.cli.terraform_command import (
    TerraformAPIBuilder,
    TerraformAPIConfig,
    TerraformBuilder,
    TerraformCommand,
    TerraformConfig,
)
from observatory.platform.terraform_api import TerraformVariable
from observatory.platform.utils.test_utils import ObservatoryTestCase, save_terraform_api_config, save_terraform_config


class TestTerraformCommand(ObservatoryTestCase):
    def test_config(self):
        with CliRunner().isolated_filesystem() as t:
            # Test when config exists and type is 'terraform'
            config_path = save_terraform_config(t)
            cmd = TerraformCommand(config_path, "terraform_credentials", config_type="terraform")
            self.assertIsInstance(cmd.config, TerraformConfig)

            # Test when config exists and type is 'terraform-api'
            config_path = save_terraform_api_config(t)
            cmd = TerraformCommand(config_path, "terraform_credentials", config_type="terraform-api")
            self.assertIsInstance(cmd.config, TerraformAPIConfig)

            # Test when config does not exist
            cmd = TerraformCommand("config_path", "terraform_credentials", config_type="NA")
            self.assertIsNone(cmd.config)

    def test_terraform_builder(self):
        with CliRunner().isolated_filesystem() as t:
            # Test with 'terraform' config type
            config_path = save_terraform_config(t)
            cmd = TerraformCommand(config_path, "terraform_credentials", config_type="terraform")
            self.assertIsInstance(cmd.terraform_builder, TerraformBuilder)

            # Test with 'terraform-api' config type
            config_path = save_terraform_api_config(t)
            cmd = TerraformCommand(config_path, "terraform_credentials", config_type="terraform-api")
            self.assertIsInstance(cmd.terraform_builder, TerraformAPIBuilder)

    def test_is_environment_valid(self):
        with CliRunner().isolated_filesystem() as t:
            config_path = save_terraform_config(t)
            with open("terraform_credentials", "w") as f:
                f.write("foo")

            # Test valid environment (valid config, terraform credentials exist, valid builder environment)
            config = TerraformConfig.load(config_path)
            with patch(
                "observatory.platform.cli.terraform_command.TerraformCommand.terraform_builder", new_callable=MagicMock
            ) as mock_builder, patch(
                "observatory.platform.cli.terraform_command.TerraformCommand.config",
                new_callable=PropertyMock,
                return_value=config,
            ):
                mock_builder.is_environment_valid = True
                cmd = TerraformCommand(config_path, "terraform_credentials", config_type="NA")

                self.assertTrue(cmd.is_environment_valid)

            # Test invalid environment (config is not valid)
            config = TerraformConfig()
            with patch(
                "observatory.platform.cli.terraform_command.TerraformCommand.terraform_builder", new_callable=MagicMock
            ) as mock_builder, patch(
                "observatory.platform.cli.terraform_command.TerraformCommand.config",
                new_callable=PropertyMock,
                return_value=config,
            ):
                mock_builder.is_environment_valid = True
                cmd = TerraformCommand(config_path, "terraform_credentials", config_type="NA")

                self.assertFalse(cmd.is_environment_valid)

            # Test invalid environment (invalid builder environment)
            config = TerraformConfig.load(config_path)
            with patch(
                "observatory.platform.cli.terraform_command.TerraformCommand.terraform_builder", new_callable=MagicMock
            ) as mock_builder, patch(
                "observatory.platform.cli.terraform_command.TerraformCommand.config",
                new_callable=PropertyMock,
                return_value=config,
            ):
                mock_builder.is_environment_valid = False
                cmd = TerraformCommand(config_path, "terraform_credentials", config_type="NA")

                self.assertFalse(cmd.is_environment_valid)

            # Test invalid environment (config does not exist)
            os.remove(config_path)
            config = TerraformConfig.load(config_path)
            with patch(
                "observatory.platform.cli.terraform_command.TerraformCommand.terraform_builder", new_callable=MagicMock
            ) as mock_builder, patch(
                "observatory.platform.cli.terraform_command.TerraformCommand.config",
                new_callable=PropertyMock,
                return_value=config,
            ):
                mock_builder.is_environment_valid = False
                cmd = TerraformCommand(config_path, "terraform_credentials", config_type="NA")

                self.assertFalse(cmd.is_environment_valid)

            # Test invalid environment (terraform credentials do not exist)
            os.remove("terraform_credentials")
            config_path = save_terraform_config(t)
            config = TerraformConfig.load(config_path)
            with patch(
                "observatory.platform.cli.terraform_command.TerraformCommand.terraform_builder", new_callable=MagicMock
            ) as mock_builder, patch(
                "observatory.platform.cli.terraform_command.TerraformCommand.config",
                new_callable=PropertyMock,
                return_value=config,
            ):
                mock_builder.is_environment_valid = False
                cmd = TerraformCommand(config_path, "terraform_credentials", config_type="NA")

                self.assertFalse(cmd.is_environment_valid)

    @patch("builtins.print")
    def test_print_variable(self, mock_print):
        # Test print non-sensitive variable
        var = TerraformVariable("key", "value")
        TerraformCommand.print_variable(var)
        mock_print.assert_called_once_with(indent(f"* \x1B[3m{var.key}\x1B[23m: {var.value}", INDENT2))

        # Test print sensitive variable
        mock_print.reset_mock()
        var = TerraformVariable("key", "value", sensitive=True)
        TerraformCommand.print_variable(var)
        mock_print.assert_called_once_with(indent(f"* \x1B[3m{var.key}\x1B[23m: sensitive", INDENT2))

    @patch("builtins.print")
    def test_print_variable_update(self, mock_print):
        # Test print non-sensitive variable
        old_var = TerraformVariable("old_key", "old_value")
        new_var = TerraformVariable("new_key", "new_value")
        TerraformCommand.print_variable_update(old_var, new_var)
        mock_print.assert_called_once_with(
            indent(f"* \x1B[3m{old_var.key}\x1B[23m:\n{old_var.value} ->\n{new_var.value}", INDENT2)
        )

        # Test print sensitive variable
        mock_print.reset_mock()
        old_var = TerraformVariable("old_key", "old_value", sensitive=True)
        new_var = TerraformVariable("new_key", "new_value")
        TerraformCommand.print_variable_update(old_var, new_var)
        mock_print.assert_called_once_with(indent(f"* \x1B[3m{old_var.key}\x1B[23m: sensitive -> sensitive", INDENT2))

    @patch("observatory.platform.cli.terraform_command.TerraformCommand.terraform_builder", autospec=True)
    def test_build_terraform(self, mock_builder):
        cmd = TerraformCommand("config_path", "terraform_credentials", config_type="NA")
        cmd.build_terraform()
        mock_builder.build_terraform.assert_called_once_with()

    @patch("observatory.platform.cli.terraform_command.TerraformCommand.terraform_builder", autospec=True)
    def test_build_image(self, mock_builder):
        cmd = TerraformCommand("config_path", "terraform_credentials", config_type="NA")
        cmd.build_image()
        mock_builder.build_image.assert_called_once_with()

    @patch("builtins.print")
    def test_print_summary(self, mock_print):
        with CliRunner().isolated_filesystem() as t:
            # Test for 'terraform' config type
            config_path = save_terraform_config(t)
            cmd = TerraformCommand(config_path, "terraform_credentials", config_type="terraform")

            # Set up used variables
            config = TerraformConfig.load(config_path)
            organization = config.terraform.organization
            environment = config.backend.environment.value
            workspace = config.terraform_workspace_id
            suffix = environment

            expected_params = [
                "\nTerraform Cloud Workspace: ",
                indent(f"Organization: {organization}", INDENT1),
                indent(
                    f"- Name: {workspace} (prefix: '{config.WORKSPACE_PREFIX}' + suffix: '{suffix}')",
                    INDENT1,
                ),
                indent(f"- Settings: ", INDENT1),
                indent(f"- Auto apply: True", INDENT2),
                indent(f"- Terraform Variables:", INDENT1),
            ]

            cmd.print_summary()
            self.assertListEqual([call(args) for args in expected_params], mock_print.call_args_list)

            # Test for 'terraform-api' config type
            mock_print.reset_mock()
            config_path = save_terraform_api_config(t)
            cmd = TerraformCommand(config_path, "terraform_credentials", config_type="terraform-api")

            # Set up used variables
            config = TerraformAPIConfig.load(config_path)
            organization = config.terraform.organization
            environment = config.backend.environment.value
            workspace = config.terraform_workspace_id
            suffix = config.api.name + "-" + environment

            expected_params = [
                "\nTerraform Cloud Workspace: ",
                indent(f"Organization: {organization}", INDENT1),
                indent(
                    f"- Name: {workspace} (prefix: '{config.WORKSPACE_PREFIX}' + suffix: '{suffix}')",
                    INDENT1,
                ),
                indent(f"- Settings: ", INDENT1),
                indent(f"- Auto apply: True", INDENT2),
                indent(f"- Terraform Variables:", INDENT1),
            ]

            cmd.print_summary()
            self.assertListEqual([call(args) for args in expected_params], mock_print.call_args_list)

    @patch("observatory.platform.cli.terraform_command.TerraformApi", new_callable=MagicMock)
    @patch("click.confirm")
    def test_create_workspace(self, mock_confirm, mock_terraform_api):
        with CliRunner().isolated_filesystem() as t:
            # Set up mock return values
            mock_confirm.side_effect = [True, False]
            mock_terraform_api.token_from_file = lambda x: "token"
            mock_terraform_api().workspace_id.return_value = "workspace_id"

            config_path = save_terraform_config(t)
            config = TerraformConfig.load(config_path)
            cmd = TerraformCommand(config_path, "terraform_credentials", config_type="terraform")

            # Test with click confirm 'True'
            cmd.create_workspace()
            mock_terraform_api().create_workspace.assert_called_once_with(
                config.terraform.organization, config.terraform_workspace_id, auto_apply=True, description=""
            )
            self.assertListEqual(
                [call(var, "workspace_id") for var in config.terraform_variables],
                mock_terraform_api().add_workspace_variable.call_args_list,
            )

            # Test with click confirm 'False'
            mock_terraform_api().create_workspace.reset_mock()
            mock_terraform_api().add_workspace_variable.reset_mock()

            cmd.create_workspace()

            mock_terraform_api().create_workspace.assert_not_called()
            mock_terraform_api().add_workspace_variable.assert_not_called()

    @patch("observatory.platform.cli.terraform_command.TerraformApi", new_callable=MagicMock)
    @patch("observatory.platform.cli.terraform_command.TerraformCommand.print_variable")
    @patch("observatory.platform.cli.terraform_command.TerraformCommand.print_variable_update")
    @patch("click.confirm")
    def test_update_workspace(self, mock_confirm, mock_print_var_update, mock_print_var, mock_terraform_api):
        with CliRunner().isolated_filesystem() as t:
            # Set up mock return values
            mock_confirm.side_effect = [True, True, False]
            mock_terraform_api.token_from_file = lambda x: "token"
            mock_terraform_api().workspace_id.return_value = "workspace_id"
            add = TerraformVariable("add", "add")
            edit = (TerraformVariable("edit_old", "edit_old"), TerraformVariable("edit_new", "edit_new"))
            unchanged = TerraformVariable("unchanged", "unchanged")
            delete = TerraformVariable("delete", "delete")
            mock_terraform_api().plan_variable_changes.side_effect = [
                ([add], [edit], [unchanged], [delete]),
                ([], [], [], []),
                ([], [], [], []),
            ]

            config_path = save_terraform_config(t)
            cmd = TerraformCommand(config_path, "terraform_credentials", config_type="terraform")

            # Test with click confirm 'True'
            cmd.update_workspace()
            mock_terraform_api().update_workspace_variables.assert_called_once_with(
                [add], [edit], [delete], "workspace_id"
            )
            self.assertListEqual([call(add), call(delete), call(unchanged)], mock_print_var.call_args_list)
            self.assertListEqual([call(edit[0], edit[1])], mock_print_var_update.call_args_list)

            # Test with empty variable changes
            mock_terraform_api().update_workspace_variables.reset_mock()
            mock_print_var.reset_mock()
            mock_print_var_update.reset_mock()

            cmd.update_workspace()
            mock_terraform_api().update_workspace_variables.assert_called_once_with([], [], [], "workspace_id")
            mock_print_var.assert_not_called()
            mock_print_var_update.assert_not_called()

            # Test with click confirm 'False'
            mock_terraform_api().update_workspace_variables.reset_mock()

            cmd.create_workspace()
            mock_terraform_api().update_workspace_variables.assert_not_called()
