# Copyright 2022 Curtin University
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

# Author: Aniek Roelofs

from typing import Dict


class APIError(Exception):
    def __init__(self, error: Dict[str, str], status_code: int):
        self.code = error.get("code")
        self.description = error.get("description")
        self.status_code = status_code


# Authentication error handler
class AuthError(APIError):
    def __init__(self, error: Dict[str, str], status_code: int = 403):
        super().__init__(error, status_code)
