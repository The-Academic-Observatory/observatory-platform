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


import distutils.dir_util
import os
import shutil
import subprocess
from subprocess import Popen
from typing import Tuple
from observatory.platform.cli.cli_utils import indent, INDENT1
from observatory.platform.config import module_file_path
from observatory.platform.docker.platform_runner import PlatformRunner
from observatory.platform.observatory_config import TerraformConfig
from observatory.platform.utils.jinja2_utils import render_template
from observatory.platform.utils.proc_utils import stream_process


def copy_dir(source_path: str, destination_path: str):
    distutils.dir_util.copy_tree(source_path, destination_path)


class TerraformBuilder:
    def __init__(self, config: TerraformConfig, debug: bool = False):
        """Create a TerraformBuilder instance, which is used to build, start and stop an Observatory Platform instance.

        :param config: the TerraformConfig.
        :param debug: whether to print debug statements.
        """

        self.config = config
        self.api_package_path = module_file_path("observatory.api", nav_back_steps=-3)
        self.terraform_path = module_file_path("observatory.platform.terraform")
        self.api_path = module_file_path("observatory.api.server")
        self.debug = debug

        self.build_path = os.path.join(config.observatory.observatory_home, "build", "terraform")
        self.platform_runner = PlatformRunner(config=config, docker_build_path=os.path.join(self.build_path, "docker"))
        self.packages_build_path = os.path.join(self.build_path, "packages")
        self.terraform_build_path = os.path.join(self.build_path, "terraform")
        os.makedirs(self.packages_build_path, exist_ok=True)
        os.makedirs(self.terraform_build_path, exist_ok=True)

    @property
    def is_environment_valid(self) -> bool:
        """Return whether the environment for building the Packer image is valid.

        :return: whether the environment for building the Packer image is valid.
        """

        return all([self.packer_exe_path is not None])

    @property
    def packer_exe_path(self) -> str:
        """The path to the Packer executable.

        :return: the path or None.
        """

        return shutil.which("packer")

    @property
    def gcloud_exe_path(self) -> str:
        """The path to the Google Cloud SDK executable.

        :return: the path or None.
        """

        return shutil.which("gcloud")

    def make_files(self):
        # Clear terraform/packages path
        if os.path.exists(self.packages_build_path):
            shutil.rmtree(self.packages_build_path)
        os.makedirs(self.packages_build_path)

        # Copy local packages
        for package in self.config.python_packages:
            if package.type == "editable":
                destination_path = os.path.join(self.packages_build_path, package.name)
                copy_dir(package.host_package, destination_path)

        # Clear terraform/terraform path, but keep the .terraform folder and other hidden files necessary for terraform.
        if os.path.exists(self.terraform_build_path):
            terraform_files = os.listdir(self.terraform_build_path)
            terraform_files_to_delete = [file for file in terraform_files if not file.startswith(".")]
            for file in terraform_files_to_delete:
                if os.path.isfile(os.path.join(self.terraform_build_path, file)):
                    os.remove(os.path.join(self.terraform_build_path, file))
                else:
                    shutil.rmtree(os.path.join(self.terraform_build_path, file))
        else:
            os.makedirs(self.terraform_build_path)

        # Copy terraform files into build/terraform
        copy_dir(self.terraform_path, self.terraform_build_path)

        # Make startup scripts
        self.make_startup_script(True, "startup-main.tpl")
        self.make_startup_script(False, "startup-worker.tpl")

    def make_startup_script(self, is_airflow_main_vm: bool, file_name: str):
        # Load and render template
        template_path = os.path.join(self.terraform_path, "startup.tpl.jinja2")
        render = render_template(template_path, is_airflow_main_vm=is_airflow_main_vm)

        # Save file
        output_path = os.path.join(self.terraform_build_path, file_name)
        with open(output_path, "w") as f:
            f.write(render)

    def build_terraform(self):
        """Build the Observatory Platform Terraform files.

        :return: None.
        """

        self.make_files()
        self.platform_runner.make_files()

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

    def gcloud_activate_service_account(self) -> Tuple[str, str, int]:
        args = ["gcloud", "auth", "activate-service-account", "--key-file", self.config.google_cloud.credentials]

        if self.debug:
            print("Executing subprocess:")
            print(indent(f"Command: {subprocess.list2cmdline(args)}", INDENT1))
            print(indent(f"Cwd: {self.api_package_path}", INDENT1))

        proc: Popen = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd=self.api_package_path)

        # Wait for results
        # Debug always true here because otherwise nothing gets printed and you don't know what the state of the
        # image building is
        output, error = stream_process(proc, True)
        return output, error, proc.returncode
