# Copyright 2019 Curtin University
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

import argparse
import datetime
import logging
import os


def validate_datetime(date_str: str, date_format: str) -> datetime.datetime:
    """ Validate whether a given date string is in the correct format.

    :param date_str: the date string to validate.
    :param date_format: the date format string. See https://docs.python.org/3/library/datetime.html#strftime-and-strptime-format-codes
    for valid values for date format.
    :return: the parsed and validated date.
    """
    try:
        return datetime.datetime.strptime(date_str, date_format)
    except ValueError:
        msg = f"Invalid date error: {date_str}. Should be in the format {date_format}."
        logging.error(msg)
        raise argparse.ArgumentTypeError(msg)


def validate_path(string: str):
    """ Validate whether the given string is a valid path.
    :param string: a string that might be a file path.
    :return: the string if it validated otherwise raise an argparse.ArgumentTypeError.
    """

    if not os.path.exists(string):
        msg = f"Path does not exist: {string}"
        logging.error(msg)
        raise argparse.ArgumentTypeError(msg)
    else:
        return string
