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

from alembic.config import Config
from alembic import command
import os
import observatory.platform


class DatabaseCommand:
    def __init__(self):
        self.observatory_dir = os.path.dirname(observatory.platform.__file__)
        self.alembic_ini_path = os.path.join(self.observatory_dir, "alembic.ini")
        self.alembic_cfg = Config(self.alembic_ini_path)

    def upgrade(self):
        """Upgrade database to alembic head."""

        os.chdir(self.observatory_dir)
        command.upgrade(self.alembic_cfg, "head")

    def history(self):
        """Print out revision history"""

        command.history(self.alembic_cfg, verbose=True)
