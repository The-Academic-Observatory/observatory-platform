# Copyright 2020 Curtin University
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

import click

INDENT1 = 2
INDENT2 = 3
INDENT3 = 4
INDENT4 = 5


def indent(string: str, num_spaces: int) -> str:
    return string.rjust(len(string) + num_spaces)


# class OptionRequiredIfTerraform(click.Option):
#     """ Make variables.tf file required when generating config file for terraform. """
#
#     def full_process_value(self, ctx, value):
#         value = super(OptionRequiredIfTerraform, self).full_process_value(ctx, value)
#
#         if value is None and ctx.params['command'] == 'config_terraform.yaml':
#             msg = 'Terraform variables.tf file required if command is "config_terraform.yaml"'
#             raise click.MissingParameter(ctx=ctx, param=self, message=msg)
#         return value
