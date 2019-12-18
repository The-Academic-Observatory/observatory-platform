import unittest

from academic_observatory.utils import get_url_domain_suffix


class TesUrlUtils(unittest.TestCase):

    def test_get_url_domain_suffix(self):
        expected = 'curtin.edu.au'

        level_one = get_url_domain_suffix('https://www.curtin.edu.au/')
        self.assertEqual(level_one, expected)

        level_two = get_url_domain_suffix('https://alumniandgive.curtin.edu.au/')
        self.assertEqual(level_two, expected)

        level_two_with_path = get_url_domain_suffix("https://alumniandgive.curtin.edu.au/giving-to-curtin/")
        self.assertEqual(level_two_with_path, expected)

        level_two_no_https = get_url_domain_suffix("alumniandgive.curtin.edu.au")
        self.assertEqual(level_two_no_https, expected)
