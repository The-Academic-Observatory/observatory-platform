import unittest
import os
from argparse import ArgumentTypeError
from academic_observatory.utils import validate_path


class TestArgparseUtils(unittest.TestCase):

    def test_validate_path(self):
        with self.assertRaises(ArgumentTypeError):
            validate_path(os.path.join(os.path.dirname(__file__), 'ishouldnotexist'))
