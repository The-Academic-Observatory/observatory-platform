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
from typing import ClassVar, Union

# from observatory.platform.observatory_config import ObservatoryConfig, TerraformConfig

INDENT1 = 2
INDENT2 = 3
INDENT3 = 4
INDENT4 = 5


def indent(string: str, num_spaces: int) -> str:
    """Left indent a string.

    :param string: the string to indent.
    :param num_spaces: the number of spaces to indent the string with.
    :return: the indented string.
    """

    assert num_spaces > 0, "indent: num_spaces must be > 0"
    return string.rjust(len(string) + num_spaces)


def comment(string: str) -> str:
    """Add a Python comment character in front of a string.
    :param string: String to comment out.
    :return: Commented string.
    """

    return f"# {string}"


def load_config(cls: ClassVar, config_path: str) -> Union["TerraformConfig", "ObservatoryConfig"]:
    """Load a config file.
    :param cls: the config file class.
    :param config_path: the path to the config file.
    :return: the config file.
    """

    # Load config
    config_exists = os.path.exists(config_path)
    if not config_exists:
        raise FileExistsError(f"Observatory config file does not exist: {config_path}")
    else:
        return cls.load(config_path)

        # print(indent(f"- path: {config_path}", INDENT2))
        # if cfg.is_valid:
        #     print(indent("- file valid", INDENT2))
        # else:
        #     print(indent("- file invalid", INDENT2))
        #     for key, value in cfg.validator.errors.items():
        #         print(indent(f"- {key}: {value}", INDENT3))
