import unittest
import os
from argparse import ArgumentTypeError
from academic_observatory.utils import validate_path, validate_datetime
import datetime


class TestArgparseUtils(unittest.TestCase):

    def test_validate_date(self):
        # Check that ArgumentTypeError is raised when incorrect date format used
        with self.assertRaises(ArgumentTypeError):
            validate_datetime('2020-01-01', "%Y-%m")

        # Check that correct datetime.datetime type is returned
        datetime_expected = datetime.datetime(year=2020, month=1, day=1)
        datetime_actual = validate_datetime('2020-01-01', "%Y-%m-%d")
        self.assertEqual(datetime_actual, datetime_expected)

    def test_validate_path(self):
        with self.assertRaises(ArgumentTypeError):
            validate_path(os.path.join(os.path.dirname(__file__), 'ishouldnotexist'))
