import unittest

from academic_observatory.utils import to_json_lines


class TestJsonUtil(unittest.TestCase):

    def test_to_json_lines(self):
        hello = dict()
        hello['hello'] = 'world'
        hello_list = [hello] * 3

        # Multiple items
        expected = '{"hello": "world"}\n{"hello": "world"}\n{"hello": "world"}\n'
        actual = to_json_lines(hello_list)
        self.assertEqual(expected, actual)

        # One item
        expected = '{"hello": "world"}\n'
        actual = to_json_lines([hello])
        self.assertEqual(expected, actual)

        # No items
        expected = ''
        actual = to_json_lines([])
        self.assertEqual(expected, actual)
