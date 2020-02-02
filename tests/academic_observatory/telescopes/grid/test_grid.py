import filecmp
import json
import os
import unittest
from typing import List

from academic_observatory.telescopes.grid import grid_path, grid_index_path, parse_institute, ao_home, save_grid_index, \
    load_grid_index, parse_grid_release
from academic_observatory.utils import test_data_dir


class TestGrid(unittest.TestCase):
    grid_index_test_path: str
    grid_release_test_path: str
    grid_institute_test_path: str
    grid_index_gen_path: str
    grid_data: List

    @classmethod
    def setUpClass(cls):
        test_data_dir_ = test_data_dir(__file__)
        cls.grid_index_test_path = os.path.join(test_data_dir_, 'grid', 'grid_index_test.csv')
        cls.grid_release_test_path = os.path.join(test_data_dir_, 'grid', 'grid_release_test.json')
        cls.grid_institute_test_path = os.path.join(test_data_dir_, 'grid', 'grid_institute_test.json')
        cls.grid_index_gen_path = os.path.join(ao_home('tests'), 'grid_index_gen.csv')
        cls.grid_data = [('grid.1001.0', 'Australian National University', 'Education',
                          'http://www.anu.edu.au/', 'www.anu.edu.au', 'anu.edu.au', 'AU'),
                         ('grid.1002.3', 'Monash University', 'Education', 'http://www.monash.edu/',
                          'www.monash.edu', 'monash.edu', 'AU'),
                         ('grid.1003.2', 'University of Queensland', 'Education', 'http://www.uq.edu.au/',
                          'www.uq.edu.au', 'uq.edu.au', 'AU')]

    def test_grid_path(self):
        path = grid_path()
        self.assertTrue(os.path.exists(path))

    def test_grid_index_path(self):
        path = grid_index_path()
        self.assertEqual(path, os.path.join(grid_path(), "grid_index.csv"))

    def test_load_grid_index(self):
        grid_index = load_grid_index(TestGrid.grid_index_test_path)
        grid_index_expected = {'anu.edu.au': 'grid.1001.0', 'monash.edu': 'grid.1002.3',
                               'uq.edu.au': 'grid.1003.2'}
        self.assertDictEqual(grid_index, grid_index_expected)

    def test_parse_institute(self):
        with open(TestGrid.grid_institute_test_path) as fp:
            institute = json.load(fp)
        result = parse_institute(institute)
        expected_result = TestGrid.grid_data[0]
        self.assertEqual(result, expected_result)

    def test_parse_grid_release(self):
        with open(TestGrid.grid_release_test_path) as fp:
            grid_release = json.load(fp)
        version, results = parse_grid_release(grid_release)
        expected_version = '2016_06_30'
        self.assertEqual(version, expected_version)
        self.assertListEqual(results, TestGrid.grid_data)

    def test_save_grid_index(self):
        save_grid_index(TestGrid.grid_index_gen_path, TestGrid.grid_data)
        self.assertTrue(filecmp.cmp(TestGrid.grid_index_gen_path, TestGrid.grid_index_test_path))
        os.remove(TestGrid.grid_index_gen_path)
