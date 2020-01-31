import os
import random
import unittest

from academic_observatory.utils import ao_home, unique_id


class TestAoUtils(unittest.TestCase):

    def test_ao_home(self):
        # Create subdir
        path = ao_home(unique_id(str(random.random())))
        self.assertTrue(os.path.exists(path))

        # Make sure we don't remove the home directory!
        if path != ao_home():
            os.removedirs(path)
