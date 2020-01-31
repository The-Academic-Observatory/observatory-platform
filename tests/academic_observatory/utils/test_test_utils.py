import os
import unittest

from academic_observatory.utils import test_data_dir


class TestTestUtils(unittest.TestCase):

    def test_test_data_dir(self):
        # Test that the correct directory is found
        path = test_data_dir(__file__)
        self.assertTrue(path.endswith(os.path.join('academic-observatory', 'tests', 'data')))
