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

import os
import shutil
import subprocess
import sys
import unittest
from click.testing import CliRunner
from datetime import datetime
from unittest.mock import patch, call

from observatory.platform.cli.cli import generate
from observatory.platform.cli.generate_command import GenerateCommand, write_rendered_template
from observatory.platform.utils.config_utils import module_file_path
from observatory.platform.utils.file_utils import _hash_file
from observatory.platform.utils.proc_utils import wait_for_process


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

    def test_generate_new_workflows_project(self):
        with CliRunner().isolated_filesystem():
            project_path = os.path.join(os.getcwd(), 'my-project')
            package_name = 'my_dags'
            CliRunner().invoke(generate, ["project", project_path, package_name])

            # Check that all directories and __init__.py files exist
            init_dirs = [package_name, os.path.join(package_name, "dags"), os.path.join(package_name,
                                                                                                "database"),
                         os.path.join(package_name, "database", "schema"), os.path.join(package_name, "workflows"),
                         "tests", os.path.join("tests", "workflows")]
            for d in init_dirs:
                init_file_path = os.path.join(project_path, d, "__init__.py")
                self.assertTrue(os.path.isfile(init_file_path))

            # Check that setup files exist
            setup_cfg_path = os.path.join(project_path, "setup.cfg")
            setup_py_path = os.path.join(project_path, "setup.py")

            self.assertTrue(os.path.isfile(setup_cfg_path))
            self.assertTrue(os.path.isfile(setup_py_path))

    def test_generate_workflow(self):
        """ Test generate workflow command.

        :return: None.
        """
        runner = CliRunner()
        with runner.isolated_filesystem():
            # Copy actual observatory platform inside isolated filesystem
            observatory_dir = os.path.join(os.getcwd(), "observatory-platform")
            shutil.copytree(module_file_path('observatory.platform'), observatory_dir)

            # Create new workflows project
            project_path = os.path.join(os.getcwd(), 'my-project')
            package_name = 'unittest_dags'
            result = runner.invoke(generate, ["project", project_path, package_name], input='n')
            self.assertEqual(0, result.exit_code)

            # Fake install package
            eggs_info_dir = os.path.join(project_path, f"{package_name}.egg-info")
            os.makedirs(eggs_info_dir, exist_ok=True)
            with open(os.path.join(eggs_info_dir, "top_level.txt"), "w") as f:
                f.write(package_name + "\n")

            # Install package
            # proc = subprocess.Popen(["git", "init"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd=project_path)
            # out, err = wait_for_process(proc)
            # #TODO check that package does not exist yet, raise error and exit if it does
            # proc = subprocess.Popen(["pip3", "show", package_name], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            # proc = subprocess.Popen(["pip3", "install", "-e", "."], stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            #                         cwd=project_path)
            # out, err = wait_for_process(proc)
            # #TODO uninstall package after tests, even when tests failed

            # Get expected file dirs
            dag_dst_dir = os.path.join(project_path, package_name, "dags")
            workflow_dst_dir = os.path.join(project_path, package_name, "workflows")
            schema_dst_dir = os.path.join(project_path, package_name, "database", "schema")
            test_dst_dir = os.path.join(project_path, "tests", "workflows")
            doc_dst_dir = os.path.join(project_path, "docs")
            identifiers_dst_dir = os.path.join(observatory_dir, "api", "client", "identifiers")

            # Get expected file paths
            dag_dst_file = os.path.join(dag_dst_dir, "my_test_workflow.py")
            workflow_dst_file = os.path.join(workflow_dst_dir, "my_test_workflow.py")
            test_dst_file = os.path.join(test_dst_dir, "test_my_test_workflow.py")
            index_dst_file = os.path.join(doc_dst_dir, "index.rst")
            doc_dst_file = os.path.join(doc_dst_dir, "my_test_workflow.md")
            schema_dst_file = os.path.join(schema_dst_dir, f"my_test_workflow"
                                                           f"_{datetime.now().strftime('%Y-%m-%d')}.json")
            identifiers_dst_file = os.path.join(identifiers_dst_dir, 'identifiers.py')

            # # Set up isolated file paths and directories
            # current_dir = os.getcwd()
            # observatory_dir = os.path.join(current_dir, "observatory-platform")
            # dags_folder = os.path.join(observatory_dir, "observatory", "dags")
            # mock_module_file_path.return_value = observatory_dir
            #
            # # Copy actual observatory platform inside isolated filesystem
            # shutil.copytree(module_file_path('observatory.platform'), observatory_dir)
            #
            # # Create expected file paths
            # dag_dst_dir = os.path.join(dags_folder, "dags")
            # dag_dst_file = os.path.join(dag_dst_dir, "my_test_telescope.py")
            #
            # telescope_dst_dir = os.path.join(dags_folder, "telescopes")
            # telescope_dst_file = os.path.join(telescope_dst_dir, "my_test_telescope.py")
            #
            # test_dst_dir = os.path.join(observatory_dir, "tests", "observatory", "dags", "telescopes")
            # test_dst_file = os.path.join(test_dst_dir, "test_my_test_telescope.py")
            #
            # doc_dst_dir = os.path.join(observatory_dir, "docs", "telescopes")
            # doc_dst_file = os.path.join(doc_dst_dir, "my_test_telescope.md")
            # doc_index_file = os.path.join(doc_dst_dir, "index.rst")
            #
            # schema_dst_dir = os.path.join(dags_folder, "database", "schema")
            # schema_dst_file = os.path.join(schema_dst_dir, f"my_test_telescope_{datetime.now().strftime('%Y-%m-%d')}.json")
            #
            # identifiers_dst_dir = os.path.join(observatory_dir, "api", "client", "identifiers")
            # identifiers_dst_file = os.path.join(identifiers_dst_dir, 'identifiers.py')
            #
            # # Create expected destination dirs
            # for dst_dir in [dag_dst_dir, telescope_dst_dir, test_dst_dir, doc_dst_dir, schema_dst_dir,
            #                 identifiers_dst_dir]:
            #     os.makedirs(dst_dir)

            # Test valid workflows
            for workflow_type in ['Workflow', 'StreamTelescope', 'SnapshotTelescope', 'OrganisationTelescope']:
                result = runner.invoke(generate, ["workflow", workflow_type, "MyTestWorkflow", "-p", project_path])
                self.assertEqual(0, result.exit_code)

                # Check whether all expected files are generated
                for file in [dag_dst_file, workflow_dst_file, test_dst_file, index_dst_file, doc_dst_file,
                             schema_dst_file]:
                    self.assertTrue(os.path.exists(file))
                #TODO mock observatory api path
                if workflow_type == 'OrganisationTelescope':
                    self.assertTrue(os.path.exists(identifiers_dst_file))

                # Check whether the template files do not contain any errors
                proc = subprocess.Popen([sys.executable, "-m", "unittest", test_dst_file], stdout=subprocess.PIPE,
                                        stderr=subprocess.PIPE, env=dict(os.environ, PYTHONPATH=project_path))
                #                                                                          TEST_GCP_PROJECT_ID="project_id",
                #                                                                          TEST_GCP_DATA_LOCATION="data_location"
                out, err = wait_for_process(proc)
                print(out, err)
                self.assertEqual("OK", err.splitlines()[-1])

                #TODO, would be nicer to run tests & validate results, but can't fix modulenotfound error
                # from unittest import TestLoader, TestResult
                # sys.path.insert(0, project_path)
                # test_suite = TestLoader().discover(project_path, pattern="test_*.py", top_level_dir=project_path)
                # result = test_suite.run(result=TestResult())

            # Test invalid workflow type
            result = runner.invoke(generate, ["workflow", "invalid_type", "MyTestWorkflow", "-p", project_path])
            self.assertEqual(1, result.exit_code)

            # Test invalid workflows project, no package

    @patch('click.confirm')
    def test_write_rendered_template(self, mock_click_confirm):
        """ Test writing a rendered template file, only overwrite when file exists if confirmed by user

        :param mock_click_confirm: Mock the click.confirm user confirmation
        :return: None.
        """
        with CliRunner().isolated_filesystem():
            # Create file to test function when file already exists
            file_path = 'test.txt'
            with open(file_path, 'w') as f:
                f.write('test')
            self.assertEqual('098f6bcd4621d373cade4e832627b4f6', _hash_file(file_path, 'md5'))

            mock_click_confirm.return_value = False
            write_rendered_template(file_path, template="some text", file_type="test")
            # Assert that file content stays the same ('test')
            self.assertEqual('098f6bcd4621d373cade4e832627b4f6', _hash_file(file_path, 'md5'))

            mock_click_confirm.return_value = True
            write_rendered_template(file_path, template="some text", file_type="test")
            # Assert that file content is now 'some text' instead of 'test'
            self.assertEqual('552e21cd4cd9918678e3c1a0df491bc3', _hash_file(file_path, 'md5'))