# Copyright 2019 Curtin University. All Rights Reserved.
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

import random
import string
import unittest

import pendulum
import yaml

from observatory_platform.observatory_config import (
    Workflow,
    workflows_to_json_string,
    json_string_to_workflows,
)


class TestObservatoryConfigValidator(unittest.TestCase):
    def setUp(self) -> None:
        self.schema = dict()
        self.schema["google_cloud"] = {
            "required": True,
            "type": "dict",
            "schema": {"credentials": {"required": True, "type": "string", "google_application_credentials": True}},
        }

    def test_workflows_to_json_string(self):
        workflows = [
            Workflow(
                dag_id="my_dag",
                name="My DAG",
                class_name="observatory_platform.workflows.vm_workflow.VmCreateWorkflow",
                kwargs=dict(dt=pendulum.datetime(2021, 1, 1)),
            )
        ]
        json_string = workflows_to_json_string(workflows)
        self.assertEqual(
            '[{"dag_id": "my_dag", "name": "My DAG", "class_name": "observatory_platform.workflows.vm_workflow.VmCreateWorkflow", "cloud_workspace": null, "kwargs": {"dt": "2021-01-01T00:00:00+00:00"}}]',
            json_string,
        )

    def test_json_string_to_workflows(self):
        json_string = '[{"dag_id": "my_dag", "name": "My DAG", "class_name": "observatory_platform.workflows.vm_workflow.VmCreateWorkflow", "cloud_workspace": null, "kwargs": {"dt": "2021-01-01T00:00:00+00:00"}}]'
        actual_workflows = json_string_to_workflows(json_string)
        self.assertEqual(
            [
                Workflow(
                    dag_id="my_dag",
                    name="My DAG",
                    class_name="observatory_platform.workflows.vm_workflow.VmCreateWorkflow",
                    kwargs=dict(dt=pendulum.datetime(2021, 1, 1)),
                )
            ],
            actual_workflows,
        )


def tmp_config_file(dict_: dict) -> str:
    """
    Dumps dict into a yaml file that is saved in a randomly named file. Used to as config file to create
    ObservatoryConfig instance.
    :param dict_: config dict
    :return: path of temporary file
    """
    content = yaml.safe_dump(dict_).replace("'!", "!").replace("':", ":")
    file_name = "".join(random.choices(string.ascii_lowercase, k=10))
    with open(file_name, "w") as f:
        f.write(content)
    return file_name
