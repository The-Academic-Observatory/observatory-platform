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

import pathlib
import uuid
from typing import Any
from unittest.mock import Mock

import tests.fixtures


def random_id():
    """ Generate a random id for bucket name.

    :return: a random string id.
    """
    return str(uuid.uuid4()).replace("-", "")


def test_fixtures_path() -> str:
    """ Get the path to the Observatory Platform package root folder.

    :return: the path to the Observatory Platform package root folder.
    """

    file_path = pathlib.Path(tests.fixtures.__file__).resolve()
    path = pathlib.Path(*file_path.parts[:-1])
    return str(path.resolve())


class MockUrlOpen(Mock):
    def __init__(self, status: int, **kwargs: Any):
        super().__init__(**kwargs)
        self.status = status

    def getcode(self):
        return self.status
