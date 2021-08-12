# Copyright 2019-2020 Curtin University
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

import os
import pathlib
import uuid

import tests.fixtures


def random_id():
    """Generate a random id for bucket name.

    :return: a random string id.
    """
    return str(uuid.uuid4()).replace("-", "")


def test_fixtures_path(*subdirs) -> str:
    """Get the path to the Observatory Platform test data directory.

    :return: he Observatory Platform test data directory.
    """

    file_path = pathlib.Path(tests.fixtures.__file__).resolve()
    base_path = str(pathlib.Path(*file_path.parts[:-1]).resolve())
    return os.path.join(base_path, *subdirs)
