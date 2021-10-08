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
from observatory.platform.docker.platform_runner import PlatformRunner
from observatory.platform.observatory_config import TerraformConfig, BackendType
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
                self.platform_runner = PlatformRunner(
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
        self.platform_runner.make_files()
