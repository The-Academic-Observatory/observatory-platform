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

# Author: James Diprose


import subprocess
from subprocess import Popen
from typing import Tuple

from observatory.platform.cli.cli_utils import INDENT1, indent
from observatory.platform.utils.proc_utils import stream_process


class VmImageBuilder:
    def __init__(self):
        pass

    def build_image(self) -> Tuple[str, str, int]:
        """Build the Observatory Platform Google Compute image with Packer.
        :return: output and error stream results and proc return code.
        """

        # Make Terraform files
        self.build_terraform()

        # Load template
        template_vars = {
            "credentials_file": self.config.google_cloud.credentials,
            "project_id": self.config.google_cloud.project_id,
            "zone": self.config.google_cloud.zone,
            "environment": self.config.backend.environment.value,
        }
        variables = []
        for key, val in template_vars.items():
            variables.append("-var")
            variables.append(f"{key}={val}")

        # Build the containers first
        args = ["packer", "build"] + variables + ["-force", "observatory-image.json"]

        if self.debug:
            print("Executing subprocess:")
            print(indent(f"Command: {subprocess.list2cmdline(args)}", INDENT1))
            print(indent(f"Cwd: {self.terraform_build_path}", INDENT1))

        proc: Popen = subprocess.Popen(
            args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd=self.terraform_build_path
        )

        # Wait for results
        # Debug always true here because otherwise nothing gets printed and you don't know what the state of the
        # image building is
        output, error = stream_process(proc, True)
        return output, error, proc.returncode
