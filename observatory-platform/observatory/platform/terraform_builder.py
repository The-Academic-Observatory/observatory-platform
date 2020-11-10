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

from observatory.platform.observatory_config import TerraformConfig, BackendType
from observatory.platform.platform_builder import PlatformBuilder
from observatory.platform.utils.config_utils import observatory_home, module_file_path
from observatory.platform.utils.jinja2_utils import render_template
from observatory.platform.utils.proc_utils import stream_process

BUILD_PATH = observatory_home('build', 'terraform')


def copy_dir(source_path: str, destination_path: str, ignore):
    distutils.dir_util.copy_tree(source_path, destination_path)


class TerraformBuilder:

    def __init__(self, config_path: str, build_path: str = BUILD_PATH):
        """ Create a PackerBuilder instance, which is used to build, start and stop an Observatory Platform instance.

        :param config_path: The path to the config.yaml configuration file.
        """

        self.backend_type = BackendType.terraform
        self.config_path = config_path
        self.build_path = build_path
        self.package_path = module_file_path('observatory.platform', nav_back_steps=-3)
        self.terraform_path = module_file_path('observatory.platform.terraform')
        self.packages_build_path = os.path.join(build_path, 'packages')
        self.terraform_build_path = os.path.join(build_path, 'terraform')
        self.platform_builder = PlatformBuilder(config_path, build_path=build_path, backend_type=self.backend_type)
        self.debug = True

        # Load config
        self.config_exists = os.path.exists(config_path)
        self.config_is_valid = False
        self.config = None
        if self.config_exists:
            self.config: TerraformConfig = TerraformConfig.load(config_path)
            self.config_is_valid = self.config.is_valid

    @property
    def is_environment_valid(self) -> bool:
        """ Return whether the environment for building the Packer image is valid.

        :return: whether the environment for building the Packer image is valid.
        """

        return all([self.packer_exe_path is not None,
                    self.config_exists,
                    self.config_is_valid,
                    self.config is not None])

    @property
    def packer_exe_path(self) -> str:
        """ The path to the Docker executable.

        :return: the path or None.
        """

        return shutil.which("packer")

    def make_files(self):
        ignore = shutil.ignore_patterns('__pycache__', '*.eggs', '*.egg-info')

        # Copy Observatory Platform
        destination_path = os.path.join(self.packages_build_path, 'observatory-platform')
        copy_dir(self.package_path, destination_path, ignore)

        # Copy DAGs projects
        for dags_project in self.config.dags_projects:
            destination_path = os.path.join(self.packages_build_path, dags_project.package_name)
            copy_dir(dags_project.path, destination_path, ignore)

        # Copy terraform files into build/terraform: ignore jinja2 templates
        copy_dir(self.terraform_path, self.terraform_build_path, shutil.ignore_patterns('*.jinja2', '__pycache__'))

        # Make startup scripts
        self.make_startup_script(True, 'startup-main.tpl')
        self.make_startup_script(False, 'startup-worker.tpl')

    def make_startup_script(self, is_airflow_main_vm: bool, file_name: str):
        # Load and render template
        template_path = os.path.join(self.terraform_path, 'startup.tpl.jinja2')
        render = render_template(template_path, is_airflow_main_vm=is_airflow_main_vm)

        # Save file
        output_path = os.path.join(self.terraform_build_path, file_name)
        with open(output_path, 'w') as f:
            f.write(render)

    def build_terraform(self):
        """ Build the Observatory Platform Terraform files.

        :return: None.
        """

        self.platform_builder.make_files()
        self.make_files()

    def build_image(self) -> Tuple[str, str, int]:
        """ Build the Observatory Platform Google Compute image with Packer.

        :return: output and error stream results and proc return code.
        """

        # Make Terraform files
        self.build_terraform()

        # Load template
        template_vars = {
            'credentials_file': self.config.google_cloud.credentials,
            'project_id': self.config.google_cloud.project_id,
            'zone': self.config.google_cloud.zone
        }
        variables = []
        for key, val in template_vars.items():
            variables.append('-var')
            variables.append(f'{key}={val}')

        # Build the containers first
        args = ['packer', 'build'] + variables + ['-force', 'observatory-image.json']
        proc: Popen = subprocess.Popen(args,
                                       stdout=subprocess.PIPE,
                                       stderr=subprocess.PIPE,
                                       cwd=self.terraform_build_path)

        # Wait for results
        output, error = stream_process(proc, self.debug)
        return output, error, proc.returncode
