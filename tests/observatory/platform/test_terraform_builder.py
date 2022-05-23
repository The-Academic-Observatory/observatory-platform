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

import os
import shutil
import unittest
from filecmp import dircmp
from unittest.mock import Mock, PropertyMock, patch

from click.testing import CliRunner
from observatory.platform.observatory_config import (
    BackendType,
    PythonPackage,
    TerraformAPIConfig,
    TerraformConfig,
)
from observatory.platform.terraform_builder import TerraformAPIBuilder, TerraformBuilder, default_observatory_home
from observatory.platform.utils.config_utils import module_file_path
from observatory.platform.utils.file_utils import get_file_hash
from observatory.platform.utils.test_utils import save_terraform_api_config, save_terraform_config


class Popen(Mock):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @property
    def returncode(self):
        return 0


class TestTerraformBuilder(unittest.TestCase):
    @patch("observatory.platform.terraform_builder.PlatformBuilder.__init__")
    @patch("observatory.platform.terraform_builder.TerraformConfig.load", wraps=TerraformConfig.load)
    def test_terraform_builder(self, mock_config_load, mock_platform_builder):
        mock_platform_builder.return_value = None
        with CliRunner().isolated_filesystem() as t:
            config_path = save_terraform_config(t)
            config = TerraformConfig.load(config_path)

            # Test attributes of terraform builder
            expected_build_path = os.path.join(config.backend.observatory_home, "build", "terraform")
            expected_packages_build_path = os.path.join(expected_build_path, "packages")
            expected_terraform_build_path = os.path.join(expected_build_path, "terraform")
            expected_terraform_path = module_file_path("observatory.platform.terraform")

            builder = TerraformBuilder(config_path=config_path)
            self.assertEqual(expected_build_path, builder.build_path)
            mock_config_load.assert_called_with(config_path)
            self.assertEqual(config.is_valid, builder.config_is_valid)
            self.assertFalse(builder.debug)
            self.assertEqual(expected_packages_build_path, builder.packages_build_path)
            mock_platform_builder.assert_called_once_with(
                config_path=config_path,
                docker_build_path=os.path.join(expected_build_path, "docker"),
                backend_type=BackendType.terraform,
            )
            self.assertEqual(expected_terraform_build_path, builder.terraform_build_path)
            self.assertEqual(expected_terraform_path, builder.terraform_path)

            # Test debug property
            builder = TerraformBuilder(config_path=config_path, debug=True)
            self.assertTrue(builder.debug)

            # Test with config path to file that does not exist
            with self.assertRaises(FileExistsError):
                TerraformBuilder(config_path="fake/config/path")

    def test_is_environment_valid(self):
        with CliRunner().isolated_filesystem() as t:
            config_path = save_terraform_config(t)
            builder = TerraformBuilder(config_path=config_path)

            with patch(
                "observatory.platform.terraform_builder.TerraformBuilder.packer_exe_path",
                new_callable=PropertyMock,
                return_value="path/to/packer",
            ):
                self.assertTrue(builder.is_environment_valid)

            with patch(
                "observatory.platform.terraform_builder.TerraformBuilder.packer_exe_path",
                new_callable=PropertyMock,
                return_value=None,
            ):
                self.assertFalse(builder.is_environment_valid)

    @patch("shutil.which")
    def test_packer_exe_path(self, mock_which):
        """Test that the path to the Packer executable is found"""
        with CliRunner().isolated_filesystem() as t:
            config_path = save_terraform_config(t)
            builder = TerraformBuilder(config_path=config_path)

            mock_which.return_value = "/path/to/packer"
            self.assertEqual("/path/to/packer", builder.packer_exe_path)

    def test_build_terraform(self):
        """Test building of the terraform files"""
        with CliRunner().isolated_filesystem() as t:
            # Save default config file
            config_path = save_terraform_config(t)

            builder = TerraformBuilder(config_path=config_path)
            # Mock methods so calls can be tracked
            builder.make_files = Mock()
            builder.platform_builder.make_files = Mock()

            builder.build_terraform()
            builder.make_files.assert_called_once_with()
            builder.platform_builder.make_files.assert_called_once_with()

    @patch("subprocess.Popen")
    @patch("observatory.platform.terraform_builder.stream_process")
    def test_build_image(self, mock_stream_process, mock_subprocess):
        """Test building of the observatory platform"""
        with CliRunner().isolated_filesystem() as t:
            mock_subprocess.return_value = Popen()
            mock_stream_process.return_value = ("", "")

            # Save default config file
            config_path = save_terraform_config(t)

            # Make observatory files
            builder = TerraformBuilder(config_path=config_path)
            self.assertTrue(builder.config_is_valid)

            # Build the image
            output, error, return_code = builder.build_image()

            # Assert that the image built
            expected_return_code = 0
            self.assertEqual(expected_return_code, return_code)

    def test_make_files(self):
        with CliRunner().isolated_filesystem() as t:
            # Save default config file
            config_path = save_terraform_config(t)

            # Make observatory files
            builder = TerraformBuilder(config_path=config_path)
            self.assertTrue(builder.config_is_valid)

            # Test when package build dir already exists
            os.makedirs(builder.packages_build_path)
            with patch("observatory.platform.terraform_builder.shutil.rmtree", wraps=shutil.rmtree) as mock_rmtree:
                builder.make_files()
                mock_rmtree.assert_called_once()

            # Test when package build dir does not exist yet
            os.rmdir(builder.packages_build_path)
            with patch("observatory.platform.terraform_builder.shutil.rmtree", wraps=shutil.rmtree) as mock_rmtree:
                builder.make_files()
                mock_rmtree.assert_not_called()

            # Test with editable package, dir should have same content as package
            observatory_package = module_file_path("observatory.platform", nav_back_steps=-3)
            package = PythonPackage(
                name="observatory-platform",
                type="editable",
                host_package=observatory_package,
                docker_package=os.path.basename(observatory_package),
            )
            with patch(
                "observatory.platform.observatory_config.ObservatoryConfig.python_packages",
                new_callable=PropertyMock,
                return_value=[package],
            ):
                builder.make_files()
                # Compare terraform build dir
                dcmp = dircmp(builder.terraform_path, builder.terraform_build_path)
                self.assertEqual([], dcmp.left_only)
                self.assertEqual(["startup-main.tpl", "startup-worker.tpl"], dcmp.right_only)
                # Compare packages build dir
                self.assertTrue(os.listdir(package.host_package) != [])
                destination_path = os.path.join(builder.packages_build_path, package.name)
                dcmp = dircmp(observatory_package, destination_path)
                self.assertEqual([], dcmp.left_only)
                self.assertEqual([], dcmp.right_only)

            # Test with other type of package, dir should not be created
            package.type = "pypi"
            observatory_package = module_file_path("observatory.platform", nav_back_steps=-3)
            package = PythonPackage(
                name="observatory-platform",
                type="pypi",
                host_package=observatory_package,
                docker_package=os.path.basename(observatory_package),
            )
            with patch(
                "observatory.platform.observatory_config.ObservatoryConfig.python_packages",
                new_callable=PropertyMock,
                return_value=[package],
            ):
                builder.make_files()
                # Compare terraform build dir
                dcmp = dircmp(builder.terraform_path, builder.terraform_build_path)
                self.assertEqual([], dcmp.left_only)
                self.assertEqual(["startup-main.tpl", "startup-worker.tpl"], dcmp.right_only)
                # Compare packages build dir
                destination_path = os.path.join(builder.packages_build_path, package.name)
                self.assertFalse(os.path.isdir(destination_path))

    @patch("observatory.platform.terraform_builder.render_template")
    def test_make_startup_script(self, mock_render_template):
        with CliRunner().isolated_filesystem() as t:
            # Save default config file
            config_path = save_terraform_config(t)

            # Initialise builder
            builder = TerraformBuilder(config_path=config_path)

            mock_render_template.return_value = "render_content"
            file_name = "file_name"
            is_airflow_main_vm = False
            template_path = os.path.join(builder.terraform_path, "startup.tpl.jinja2")

            builder.make_startup_script(is_airflow_main_vm=is_airflow_main_vm, file_name=file_name)
            mock_render_template.assert_called_once_with(template_path, is_airflow_main_vm=is_airflow_main_vm)
            actual_hash = get_file_hash(
                file_path=os.path.join(builder.terraform_build_path, file_name), algorithm="md5"
            )
            self.assertEqual("a728191895d4caf99568192fa2ad666a", actual_hash)


class TestTerraformAPIBuilder(unittest.TestCase):
    @patch("observatory.platform.terraform_builder.TerraformAPIConfig.load", wraps=TerraformAPIConfig.load)
    def test_terraform_api_builder(self, mock_config_load):
        with CliRunner().isolated_filesystem() as t:
            with patch.dict(os.environ, {"OBSERVATORY_HOME": os.path.join(t, ".observatory")}):
                config_path = save_terraform_api_config(t)
                config = TerraformAPIConfig.load(config_path)

                # Test attributes of terraform api builder
                builder = TerraformAPIBuilder(config_path=config_path)
                mock_config_load.assert_called_with(config_path)
                self.assertEqual(config.is_valid, builder.config_is_valid)

                # Test debug property
                builder = TerraformAPIBuilder(config_path=config_path, debug=True)
                self.assertTrue(builder.debug)

                # Test with config path to file that does not exist
                with self.assertRaises(FileExistsError):
                    TerraformAPIBuilder(config_path="fake/config/path")

    def test_api_server_path(self):
        with CliRunner().isolated_filesystem() as t:
            with patch.dict(os.environ, {"OBSERVATORY_HOME": os.path.join(t, ".observatory")}):
                config_path = save_terraform_api_config(t)
                builder = TerraformAPIBuilder(config_path)

                # Create "server" dir
                server_dir = os.path.join(builder.config.api.package, "server")
                os.makedirs(server_dir)
                self.assertEqual(server_dir, builder.api_server_path)

    def test_terraform_path(self):
        with CliRunner().isolated_filesystem() as t:
            with patch.dict(os.environ, {"OBSERVATORY_HOME": os.path.join(t, ".observatory")}):
                config_path = save_terraform_api_config(t)
                builder = TerraformAPIBuilder(config_path)

                terraform_path = os.path.join(builder.config.api.package, "terraform")
                self.assertEqual(terraform_path, builder.terraform_path)

    def test_terraform_build_path(self):
        with CliRunner().isolated_filesystem() as t:
            with patch.dict(os.environ, {"OBSERVATORY_HOME": os.path.join(t, ".observatory")}):
                config_path = save_terraform_api_config(t)
                builder = TerraformAPIBuilder(config_path)

                terraform_build_path = os.path.join(
                    builder.config.backend.observatory_home, "build", "terraform-api", builder.config.api.name,
                    "terraform"
                )
                self.assertEqual(terraform_build_path, builder.terraform_build_path)

    def test_is_environment_valid(self):
        with CliRunner().isolated_filesystem() as t:
            with patch.dict(os.environ, {"OBSERVATORY_HOME": os.path.join(t, ".observatory")}):
                config_path = save_terraform_api_config(t)
                builder = TerraformAPIBuilder(config_path=config_path)

                with patch(
                    "observatory.platform.terraform_builder.TerraformAPIBuilder.gcloud_exe_path",
                    new_callable=PropertyMock,
                    return_value="path/to/gcloud",
                ):
                    self.assertTrue(builder.is_environment_valid)

                with patch(
                    "observatory.platform.terraform_builder.TerraformAPIBuilder.gcloud_exe_path",
                    new_callable=PropertyMock,
                    return_value=None,
                ):
                    self.assertFalse(builder.is_environment_valid)

    @patch("shutil.which")
    def test_gcloud_exe_path(self, mock_which):
        """Test that the path to the Gcloud executable is found"""
        with CliRunner().isolated_filesystem() as t:
            with patch.dict(os.environ, {"OBSERVATORY_HOME": os.path.join(t, ".observatory")}):
                config_path = save_terraform_api_config(t)
                builder = TerraformAPIBuilder(config_path=config_path)

                mock_which.return_value = "/path/to/gcloud"
                self.assertEqual("/path/to/gcloud", builder.gcloud_exe_path)

    def test_build_terraform(self):
        """Test building of the terraform files"""
        with CliRunner().isolated_filesystem() as t:
            with patch.dict(os.environ, {"OBSERVATORY_HOME": os.path.join(t, ".observatory")}):
                # Save default config file
                config_path = save_terraform_api_config(t)

                builder = TerraformAPIBuilder(config_path=config_path)
                # Mock methods so calls can be tracked
                builder.make_files = Mock()

                builder.build_terraform()
                builder.make_files.assert_called_once_with()

    def test_build_image(self):
        """Test building of gcloud image"""
        with CliRunner().isolated_filesystem() as t:
            with patch.dict(os.environ, {"OBSERVATORY_HOME": os.path.join(t, ".observatory")}):
                # Save default config file
                config_path = save_terraform_api_config(t)

                builder = TerraformAPIBuilder(config_path=config_path)
                # Mock methods so calls can be tracked
                builder.gcloud_activate_service_account = Mock()
                builder.gcloud_builds_submit = Mock()

                builder.build_image()
                builder.gcloud_activate_service_account.assert_called_once_with()
                builder.gcloud_builds_submit.assert_called_once_with("local")

    def test_make_files(self):
        with CliRunner().isolated_filesystem() as t:
            with patch.dict(os.environ, {"OBSERVATORY_HOME": os.path.join(t, ".observatory")}):
                # Save default config file
                config_path = save_terraform_api_config(t)

                # Initialise builder
                builder = TerraformAPIBuilder(config_path=config_path)
                builder.make_open_api_template = Mock()

                # Test when package build dir already exists
                os.makedirs(builder.terraform_build_path)
                with patch("observatory.platform.terraform_builder.shutil.rmtree", wraps=shutil.rmtree) as mock_rmtree:
                    builder.make_files()
                    mock_rmtree.assert_called_once()
                    builder.make_open_api_template.assert_called_once_with()

                # Test when package build dir does not exist yet
                shutil.rmtree(builder.terraform_build_path)
                builder.make_open_api_template.reset_mock()
                with patch("observatory.platform.terraform_builder.shutil.rmtree", wraps=shutil.rmtree) as mock_rmtree:
                    builder.make_files()
                    mock_rmtree.assert_not_called()
                    builder.make_open_api_template.assert_called_once_with()

                # Compare terraform build dir
                dcmp = dircmp(builder.terraform_path, builder.terraform_build_path)
                self.assertTrue(os.listdir(builder.terraform_path) != [])
                self.assertEqual([], dcmp.left_only)
                self.assertEqual([], dcmp.right_only)

    @patch("observatory.platform.terraform_builder.OpenApiRenderer", autospec=True)
    def test_make_openapi_template(self, mock_renderer):
        mock_renderer.return_value.render.return_value = "rendered content"
        with CliRunner().isolated_filesystem() as t:
            with patch.dict(os.environ, {"OBSERVATORY_HOME": os.path.join(t, ".observatory")}):
                # Save default config file
                config_path = save_terraform_api_config(t)

                # Initialise builder and make build dir
                builder = TerraformAPIBuilder(config_path=config_path)
                os.makedirs(builder.terraform_build_path, exist_ok=True)

                # Make template
                builder.make_open_api_template()

                # Test renderer and created content from template
                specification_path = os.path.join(builder.api_server_path, "openapi.yaml.jinja2")
                mock_renderer.assert_called_once_with(specification_path, usage_type="cloud_endpoints")
                actual_hash = get_file_hash(
                    file_path=os.path.join(builder.terraform_build_path, "openapi.yaml.tpl"), algorithm="md5"
                )
                self.assertEqual("c747d94f6475ac8d5ab206ee84160a0c", actual_hash)

    @patch("subprocess.Popen")
    @patch("observatory.platform.terraform_builder.stream_process")
    def test_gcloud_activate_service_account(self, mock_stream_process, mock_subprocess):
        with CliRunner().isolated_filesystem() as t:
            with patch.dict(os.environ, {"OBSERVATORY_HOME": os.path.join(t, ".observatory")}):
                mock_subprocess.return_value = Popen()
                mock_stream_process.return_value = ("out", "err")

                # Save default config file
                config_path = save_terraform_api_config(t)

                # Initialise builder
                builder = TerraformAPIBuilder(config_path=config_path)

                # Activate service account
                output, error, return_code = builder.gcloud_activate_service_account()

                # Assert the output and subprocess args
                mock_subprocess.assert_called_once_with(
                    [
                        "gcloud",
                        "auth",
                        "activate-service-account",
                        "--key-file",
                        builder.config.google_cloud.credentials,
                    ],
                    stdout=-1,
                    stderr=-1,
                    cwd=builder.config.api.package,
                )
                self.assertEqual("out", output)
                self.assertEqual("err", error)
                self.assertEqual(0, return_code)

                # Test when debug is True
                builder = TerraformAPIBuilder(config_path=config_path, debug=True)
                with patch("builtins.print") as mock_print:
                    builder.gcloud_activate_service_account()
                    self.assertEqual(3, mock_print.call_count)

    @patch("subprocess.Popen")
    @patch("observatory.platform.terraform_builder.stream_process")
    def test_gcloud_builds_submit(self, mock_stream_process, mock_subprocess):
        with CliRunner().isolated_filesystem() as t:
            with patch.dict(os.environ, {"OBSERVATORY_HOME": os.path.join(t, ".observatory")}):
                mock_subprocess.return_value = Popen()
                mock_stream_process.return_value = ("out", "err")

                # Save default config file
                config_path = save_terraform_api_config(t)

                # Initialise builder
                builder = TerraformAPIBuilder(config_path=config_path)

                # Build and submit gcloud image
                output, error, return_code = builder.gcloud_builds_submit("image_tag")

                # Assert the output and subprocess args
                project_id = builder.config.google_cloud.project_id
                mock_subprocess.assert_called_once_with(
                    [
                        "gcloud",
                        "builds",
                        "submit",
                        "--tag",
                        f"us-docker.pkg.dev/{project_id}/observatory-platform/{builder.config.api.name}:image_tag",
                        "--project",
                        project_id,
                    ],
                    stdout=-1,
                    stderr=-1,
                    cwd=builder.config.api.package,
                )
                self.assertEqual("out", output)
                self.assertEqual("err", error)
                self.assertEqual(0, return_code)

                # Test when debug is True
                builder = TerraformAPIBuilder(config_path=config_path, debug=True)
                with patch("builtins.print") as mock_print:
                    builder.gcloud_builds_submit("image_tag")
                    self.assertEqual(3, mock_print.call_count)
