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

from observatory.api.server.openapi_renderer import OpenApiRenderer
from observatory.platform.cli.click_utils import indent, INDENT1
from observatory.platform.observatory_config import TerraformConfig, BackendType
from observatory.platform.platform_builder import PlatformBuilder
from observatory.platform.utils.config_utils import module_file_path
from observatory.platform.utils.jinja2_utils import render_template
from observatory.platform.utils.proc_utils import stream_process


def copy_dir(source_path: str, destination_path: str, ignore):
    distutils.dir_util.copy_tree(source_path, destination_path)


class TerraformBuilder:
    def __init__(self, config_path: str, debug: bool = False):
        """Create a TerraformBuilder instance, which is used to build, start and stop an Observatory Platform instance.

        :param config_path: the path to the config.yaml configuration file.
        :param debug: whether to print debug statements.
        """

        self.api_package_path = module_file_path("observatory.api", nav_back_steps=-3)
        self.terraform_path = module_file_path("observatory.platform.terraform")
        self.api_path = module_file_path("observatory.api.server")
        self.debug = debug

        # Load config
        config_exists = os.path.exists(config_path)
        if not config_exists:
            raise FileExistsError(f"Terraform config file does not exist: {config_path}")
        else:
            self.config: TerraformConfig = TerraformConfig.load(config_path)
            self.config_is_valid = self.config.is_valid
            if self.config_is_valid:
                self.build_path = os.path.join(self.config.observatory.observatory_home, "build", "terraform")
                self.platform_builder = PlatformBuilder(
                    config_path=config_path,
                    docker_build_path=os.path.join(self.build_path, "docker"),
                    backend_type=BackendType.terraform,
                )
                self.packages_build_path = os.path.join(self.build_path, "packages")
                self.terraform_build_path = os.path.join(self.build_path, "terraform")
                os.makedirs(self.packages_build_path, exist_ok=True)
                os.makedirs(self.terraform_build_path, exist_ok=True)

    @property
    def is_environment_valid(self) -> bool:
        """Return whether the environment for building the Packer image is valid.

        :return: whether the environment for building the Packer image is valid.
        """

        return all([self.packer_exe_path is not None, self.config_is_valid, self.config is not None])

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

        ignore = shutil.ignore_patterns("__pycache__", "*.eggs", "*.egg-info")

        # Copy local packages
        for package in self.config.python_packages:
            if package.type == "editable":
                destination_path = os.path.join(self.packages_build_path, package.name)
                copy_dir(package.host_package, destination_path, ignore)

        # Copy terraform files into build/terraform: ignore jinja2 templates
        copy_dir(self.terraform_path, self.terraform_build_path, shutil.ignore_patterns("*.jinja2", "__pycache__"))

        # Make startup scripts
        self.make_startup_script(True, "startup-main.tpl")
        self.make_startup_script(False, "startup-worker.tpl")

        # Make OpenAPI specification
        self.make_open_api_template()

    def make_startup_script(self, is_airflow_main_vm: bool, file_name: str):
        # Load and render template
        template_path = os.path.join(self.terraform_path, "startup.tpl.jinja2")
        render = render_template(template_path, is_airflow_main_vm=is_airflow_main_vm)

        # Save file
        output_path = os.path.join(self.terraform_build_path, file_name)
        with open(output_path, "w") as f:
            f.write(render)

    def make_open_api_template(self):
        # Load and render template
        specification_path = os.path.join(self.api_path, "openapi.yaml.jinja2")
        renderer = OpenApiRenderer(specification_path, cloud_endpoints=True)
        render = renderer.render()

        # Save file
        output_path = os.path.join(self.terraform_build_path, "openapi.yaml.tpl")
        with open(output_path, "w") as f:
            f.write(render)

    def build_terraform(self):
        """Build the Observatory Platform Terraform files.

        :return: None.
        """

        self.make_files()
        self.platform_builder.make_files()

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

    def gcloud_builds_submit(self) -> Tuple[str, str, int]:
        # Build the google container image
        project_id = self.config.google_cloud.project_id
        # --gcs-logs-dir is specified to avoid storage.objects.get access error, see:
        # https://github.com/google-github-actions/setup-gcloud/issues/105
        # the _cloudbuild bucket is created already to store the build image
        args = [
            "gcloud",
            "builds",
            "submit",
            "--tag",
            f"gcr.io/{project_id}/observatory-api",
            "--project",
            project_id,
            "--gcs-log-dir",
            f"gs://{project_id}_cloudbuild/logs",
        ]
        if self.debug:
            print("Executing subprocess:")
            print(indent(f"Command: {subprocess.list2cmdline(args)}", INDENT1))
            print(indent(f"Cwd: {self.api_package_path}", INDENT1))

        proc: Popen = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd=self.api_package_path)

        # Wait for results
        # Debug always true here because otherwise nothing gets printed and you don't know what the state of the
        # image building is
        output, error = stream_process(proc, True)

        info_filepath = os.path.join(self.terraform_build_path, "api_image_build.txt")
        with open(info_filepath, "w") as f:
            f.writelines(line + "\n" for line in output.splitlines()[-2:])
        return output, error, proc.returncode
