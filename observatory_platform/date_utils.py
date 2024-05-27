# Copyright 2024 Curtin University
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

# Author: Keegan Smith

from zoneinfo import ZoneInfo

from dateutil import parser


def normalise_datetime(dt_string: str) -> str:
    """
    Converts a date or datetime string to an isoformatted datetime string at +0000UTC

    :param dt_string:  The string to convert
    :return: The ISO formatted datetime string
    """
    dt = parser.parse(dt_string)  # Parse string to datetime object
    if not dt.utcoffset():  # If no timezone present, assume +0000UTC
        dt = dt.replace(tzinfo=ZoneInfo("UTC"))
    dt = dt.astimezone(ZoneInfo("UTC"))

    return dt.isoformat()
