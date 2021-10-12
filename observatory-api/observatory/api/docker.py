# Copyright 2021 Curtin University
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

import docker


def build_docker_file(context: str, tag: str, dockerfile: str = "Dockerfile") -> bool:
    """Build a Docker file.
    :param context: the docker build context.
    :param tag: the Docker tag name.
    :param dockerfile: the Docker file name.
    :return: whether the build was successful or not.
    """

    client = docker.from_env()
    success = False
    for line in client.api.build(path=context, dockerfile=dockerfile, rm=True, decode=True, tag=tag):
        if "stream" in line:
            stream = line["stream"].rstrip()
            print(stream)
        elif "error" in line:
            error = line["error"].rstrip()
            print(error)
            success = False
        elif "aux" in line:
            success = True

    return success
