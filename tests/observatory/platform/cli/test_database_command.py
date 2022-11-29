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

# Author: Tuan Chien


import unittest
import os
from click.testing import CliRunner
from observatory.platform.cli.database_command import DatabaseCommand
from observatory.platform.utils.test_utils import ObservatoryApiEnvironment, find_free_port


class TestPlatformCommand(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.host = "localhost"
        self.port = find_free_port()

    def test_database_upgrade(self):
        with CliRunner().isolated_filesystem() as t:
            env = ObservatoryApiEnvironment(host=self.host, port=self.port)
            with env.create():
                os.environ["OBSERVATORY_DB_URI"] = f"sqlite://"

                cmd = DatabaseCommand()
                cmd.upgrade()
                cmd.history()  # Visually inspect
