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

from observatory.platform.docker.platform_runner import PlatformRunner
from observatory.platform.observatory_config import BuildConfig


class BuildCommand:
    def __init__(self, config: BuildConfig):
        """Create the build command.
        :param config: the build config.
        """

        self.config = config

    def build_image(self, tag: str):
        """Build the observatory docker image.
        :param tag: the Docker tag.
        :return:  None
        """

        pb = PlatformRunner(config=self.config, tag=tag)
        pb.build()

    def build_vm_image(self):

        pass
