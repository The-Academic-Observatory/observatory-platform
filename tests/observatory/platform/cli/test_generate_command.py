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

# Author: James Diprose, Tuan Chien, Aniek Roelofs

import logging
import importlib
import subprocess
import sys
import os
import re
import shutil
import unittest
from click.testing import CliRunner
from datetime import datetime
from unittest.mock import patch, call
from unittest import TestLoader, TestResult

from observatory.platform.cli.cli import generate
from observatory.platform.cli.generate_command import GenerateCommand, write_rendered_template
from observatory.platform.utils.config_utils import module_file_path
from observatory.platform.utils.file_utils import _hash_file
from observatory.platform.utils.proc_utils import stream_process


class TestGenerateCommand(unittest.TestCase):
    def test_generate_fernet_key(self):
        cmd = GenerateCommand()

        # Test that keys are random
        num_keys = 100
        random_keys = [cmd.generate_fernet_key() for _ in range(num_keys)]
        self.assertEqual(len(set(random_keys)), num_keys)

        # Test that keys have length of 44
        expected_key_len = 44
        [self.assertEqual(expected_key_len, len(key)) for key in random_keys]

    def test_generate_local_config(self):
        cmd = GenerateCommand()
        config_path = "config.yaml"

        with CliRunner().isolated_filesystem():
            cmd.generate_local_config(config_path)
            self.assertTrue(os.path.exists(config_path))

    def test_generate_terraform_config(self):
        cmd = GenerateCommand()
        config_path = "config-terraform.yaml"

        with CliRunner().isolated_filesystem():
            cmd.generate_terraform_config(config_path)
            self.assertTrue(os.path.exists(config_path))

    @patch("click.confirm")
    def test_generate_workflows_project(self, mock_cli_confirm):
        """ Test generate a new workflows project

        :return: None.
        """
        cmd = GenerateCommand()
        runner = CliRunner()
        with runner.isolated_filesystem():
            # Test creating new project
            project_path = os.path.join(os.getcwd(), "my-project")
            package_name = "my_dags"
            cmd.generate_workflows_project(project_path, package_name, "Author Name")

            # Check that all directories and __init__.py files exist
            init_dirs = [
                package_name,
                os.path.join(package_name, "dags"),
                os.path.join(package_name, "database"),
                os.path.join(package_name, "database", "schema"),
                os.path.join(package_name, "workflows"),
                "tests",
                os.path.join("tests", "workflows"),
            ]
            for d in init_dirs:
                init_file_path = os.path.join(project_path, d, "__init__.py")
                self.assertTrue(os.path.isfile(init_file_path))
                with open(init_file_path, "a") as f:
                    f.write("test")

            # Check that setup files exist
            setup_cfg_path = os.path.join(project_path, "setup.cfg")
            setup_py_path = os.path.join(project_path, "setup.py")

            self.assertTrue(os.path.isfile(setup_cfg_path))
            self.assertTrue(os.path.isfile(setup_py_path))

            # Check that all docs files exist
            docs_dirs = ["_build", "_static", "_templates", "workflows"]
            for d in docs_dirs:
                dir_path = os.path.join(project_path, "docs", d)
                self.assertTrue(os.path.isdir(dir_path))

            docs_files = ["conf.py", "generate_schema_csv.py", "index.rst", "make.bat", "Makefile"]
            for f in docs_files:
                file_path = os.path.join(project_path, "docs", f)
                self.assertTrue(os.path.isfile(file_path))

            # Test creating new project when files already exist
            mock_cli_confirm.return_value = "y"
            cmd.generate_workflows_project(project_path, package_name, "Author Name")

            # Check that file hash stayed the same (no new empty init files)
            for d in init_dirs:
                init_file_path = os.path.join(project_path, d, "__init__.py")
                self.assertTrue(os.path.isfile(init_file_path))
                self.assertEqual('098f6bcd4621d373cade4e832627b4f6', _hash_file(init_file_path, "md5"))

            # Check that setup files exist
            self.assertEqual(2, mock_cli_confirm.call_count)
            self.assertTrue(os.path.isfile(setup_cfg_path))
            self.assertTrue(os.path.isfile(setup_py_path))

    def test_generate_workflow(self):
        """ Test generate workflow command and run unit tests that are generated for each of the workflow types.

        :return: None.
        """
        runner = CliRunner()
        with runner.isolated_filesystem():
            cmd = GenerateCommand()

            # Copy actual observatory platform inside isolated filesystem
            observatory_dir = os.path.join(os.getcwd(), "observatory-platform")
            shutil.copytree(module_file_path("observatory.platform"), observatory_dir)

            # Create new workflows project
            project_path = os.path.join(os.getcwd(), "my-project")
            package_name = "unittest_dags"
            result = runner.invoke(generate, ["project", project_path, package_name, "Author Name"], input="n")
            self.assertEqual(0, result.exit_code)

            # Get expected file dirs
            dag_dst_dir = os.path.join(project_path, package_name, "dags")
            utils_dst_dir = os.path.join(project_path, package_name, "utils")
            workflow_dst_dir = os.path.join(project_path, package_name, "workflows")
            schema_dst_dir = os.path.join(project_path, package_name, "database", "schema")
            test_dst_dir = os.path.join(project_path, "tests", "workflows")
            doc_dst_dir = os.path.join(project_path, "docs")

            # Test valid workflows
            for workflow_name, workflow_type in [
                ("MyWorkflow", "Workflow"),
                ("MyStream", "StreamTelescope"),
                ("MySnapshot", "SnapshotTelescope"),
                ("MyOrganisation", "OrganisationTelescope"),
            ]:
                cmd.generate_workflow(project_path, package_name, workflow_type, workflow_name)

                # Get expected file paths
                workflow_module = re.sub(r"([A-Z])", r"_\1", workflow_name).lower().strip("_")
                dag_dst_file = os.path.join(dag_dst_dir, f"{workflow_module}.py")
                workflow_dst_file = os.path.join(workflow_dst_dir, f"{workflow_module}.py")
                test_dst_file = os.path.join(test_dst_dir, f"test_{workflow_module}.py")
                index_dst_file = os.path.join(doc_dst_dir, "index.rst")
                doc_dst_file = os.path.join(doc_dst_dir, "workflows", f"{workflow_module}.md")
                schema_dst_file = os.path.join(
                    schema_dst_dir, f"{workflow_module}_{datetime.now().strftime('%Y-%m-%d')}.json"
                )
                identifiers_dst_file = os.path.join(utils_dst_dir, "identifiers.py")

                # Check whether all expected files are generated
                for file in [
                    dag_dst_file,
                    workflow_dst_file,
                    test_dst_file,
                    index_dst_file,
                    doc_dst_file,
                    schema_dst_file,
                ]:
                    self.assertTrue(os.path.exists(file))

                # Check if identifiers file exists
                if workflow_type == "OrganisationTelescope":
                    self.assertTrue(os.path.exists(identifiers_dst_file))

                # Check whether the template files do not contain any errors by running the created test files
                proc = subprocess.Popen([sys.executable, "-m", "unittest", test_dst_file], stdout=subprocess.PIPE,
                                        stderr=subprocess.PIPE, env=dict(os.environ, PYTHONPATH=project_path))
                out, err = stream_process(proc, debug=True)
                self.assertEqual("OK", err.splitlines()[-1])

                # # Modify test file to insert module in system path
                # with open(test_dst_file, "r") as f_in, open(test_dst_file + "tmp", "w") as f_out:
                #     f_out.write(f"import sys\nsys.path.insert(0, '{project_path}')\n")
                #     f_out.write(f_in.read())
                # os.rename(test_dst_file + "tmp", test_dst_file)

                # Load unit test as test suite and run unit test, this works
                # sys.path.insert(0, project_path)
                # importlib.import_module(f"tests.workflows.test_{workflow_module}")
                # module = sys.modules[f"tests.workflows.test_{workflow_module}"]
                # test_suite = TestLoader().loadTestsFromModule(module)
                # test_suite = TestLoader().discover(
                #     test_dst_dir, pattern=f"test_{workflow_module}.py", top_level_dir=project_path
                # )

                # # Check that tests were found
                # found_tests = False
                # for suite in test_suite._tests:
                #     if suite._tests:
                #         found_tests = True
                #         break
                # self.assertTrue(found_tests)
                #
                # # Run the unit tests
                # result = test_suite.run(result=TestResult())
                # self.assertTrue(result.wasSuccessful(), msg=result.errors)

            # Test that identifiers file is only created if it does not exist
            cmd.generate_workflow(project_path, package_name, "OrganisationTelescope", "MyOrganisation2")
            self.assertEqual("7f1de3572c0fb605e4d24e7a2e1c4e30", _hash_file(identifiers_dst_file, "md5"))

            # Test invalid workflow type
            with self.assertRaises(Exception):
                cmd.generate_workflow(project_path, package_name, "Invalid", "MyWorkflow")

    @patch("click.confirm")
    def test_write_rendered_template(self, mock_click_confirm):
        """ Test writing a rendered template file, only overwrite when file exists if confirmed by user

        :param mock_click_confirm: Mock the click.confirm user confirmation
        :return: None.
        """
        with CliRunner().isolated_filesystem():
            # Create file to test function when file already exists
            file_path = "test.txt"
            with open(file_path, "w") as f:
                f.write("test")
            self.assertEqual("098f6bcd4621d373cade4e832627b4f6", _hash_file(file_path, "md5"))

            mock_click_confirm.return_value = False
            write_rendered_template(file_path, template="some text", file_type="test")
            # Assert that file content stays the same ('test')
            self.assertEqual("098f6bcd4621d373cade4e832627b4f6", _hash_file(file_path, "md5"))

            mock_click_confirm.return_value = True
            write_rendered_template(file_path, template="some text", file_type="test")
            # Assert that file content is now 'some text' instead of 'test'
            self.assertEqual("552e21cd4cd9918678e3c1a0df491bc3", _hash_file(file_path, "md5"))
