# Copyright 2020 Curtin University
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

import datetime
import logging
import os
import shutil
import unittest
import xml.etree.ElementTree as ET
from typing import List, Dict
from unittest.mock import patch

import vcr
from click.testing import CliRunner

from academic_observatory.telescopes.fundref import (
    FundrefRelease,
    FundrefTelescope,
    add_funders_relationships,
    decompress_release,
    geonames_to_dict,
    get_filepath_geonames,
    get_geoname_data,
    list_releases,
    parse_fundref_registry_rdf,
    recursive_funders,
    transform_release
)
from academic_observatory.utils.config_utils import telescope_path, SubFolder
from academic_observatory.utils.data_utils import _hash_file
from tests.academic_observatory.config import test_fixtures_path


class TestFundref(unittest.TestCase):
    """ Tests for the functions used by the fundref telescope """

    def __init__(self, *args, **kwargs):
        """ Constructor which sets up variables used by tests.

        :param args: arguments.
        :param kwargs: keyword arguments.
        """

        super(TestFundref, self).__init__(*args, **kwargs)

        # Fundref releases list
        self.list_fundref_releases_path = os.path.join(test_fixtures_path(), 'vcr_cassettes',
                                                       'list_fundref_releases.yaml')
        self.list_fundref_releases_hash = '5ddd47c136277f58876b1a1d2d7a6379'

        # Fundref test release
        self.fundref_test_path = FundrefTelescope.DEBUG_FILE_PATH
        self.fundref_test_url = FundrefTelescope.TELESCOPE_DEBUG_URL
        self.fundref_nested_element = ET.Element(
            '{http://data.crossref.org/fundingdata/xml/schema/grant/grant-1.2/}country',
            attrib={'{http://www.w3.org/1999/02/22-rdf-syntax-ns#}resource': 'http://sws.geonames.org/6252001/'})
        self.fundref_nested_element.tail = '\n      '
        self.fundref_test_date = '3000-01-01'
        self.fundref_test_download_file_name = 'fundref_3000_01_01.tar.gz'
        self.fundref_test_decompress_file_name = 'fundref_3000_01_01.rdf'
        self.fundref_test_transform_file_name = 'fundref_3000_01_01.rdf'
        self.fundref_test_download_hash = 'c9c61c5053208752e8926f159d58b101'
        self.fundref_test_decompress_hash = 'ed14c816d89b4334675bd11514f9cac2'
        self.fundref_test_transform_hash = 'eb609adaa7fc9f0ba948a29490ceaa2d'

        # Geonames file
        self.geonames_file_name = FundrefTelescope.GEONAMES_FILE_NAME
        self.geonames_test_path = FundrefTelescope.DEBUG_GEONAMES_FILE_PATH
        self.geonames_test_dict = {'4361885': ('Maryland', 'US'), '6252001': ('United States', 'US'),
                                   '6254928': ('Virginia', 'US')}
        self.geonames_test_hash = '01c88dadbdd964bd9191063a28e3b6e6'

        logging.info("Check that test fixtures exist")
        self.assertTrue(os.path.isfile(self.list_fundref_releases_path))
        self.assertTrue(os.path.isfile(self.fundref_test_path))
        self.assertTrue(os.path.isfile(self.geonames_test_path))
        self.assertTrue(self.list_fundref_releases_hash,
                        _hash_file(self.list_fundref_releases_path, algorithm='md5'))
        self.assertTrue(self.fundref_test_download_hash, _hash_file(self.fundref_test_path, algorithm='md5'))
        self.assertTrue(self.geonames_test_hash, _hash_file(self.geonames_test_path, algorithm='md5'))

        # Turn logging to warning because vcr prints too much at info level
        logging.basicConfig()
        logging.getLogger().setLevel(logging.WARNING)

    def test_list_releases(self):
        """ Test that list releases returns a dictionary with releases.

        :return: None.
        """
        with vcr.use_cassette(self.list_fundref_releases_path):
            releases = list_releases(FundrefTelescope.TELESCOPE_URL)
            self.assertIsInstance(releases, List)
            for release in releases:
                self.assertIsInstance(release, Dict)
                self.assertIn('url', release)
                self.assertIn('date', release)

    def test_release_date(self):
        """ Test that date obtained from url is string and in correct format.

        :return: None.
        """
        with vcr.use_cassette(self.list_fundref_releases_path):
            releases_list = list_releases(FundrefTelescope.TELESCOPE_URL)
            for release in releases_list:
                release = FundrefRelease(release['url'], release['date'])
                date = release.date
                self.assertIsInstance(date, str)
                self.assertTrue(datetime.datetime.strptime(date, "%Y-%m-%d"))

    @patch('academic_observatory.utils.config_utils.pathlib.Path.home')
    def test_filepath_download(self, home_mock):
        """ Test that path of downloaded file is correct for given url.

        :param home_mock: Mock observatory home path
        :return: None.
        """
        with CliRunner().isolated_filesystem():
            # Create home path and mock getting home path
            home_path = 'user-home'
            os.makedirs(home_path, exist_ok=True)
            home_mock.return_value = home_path

            with CliRunner().isolated_filesystem():
                release = FundrefRelease(self.fundref_test_url, self.fundref_test_date)
                file_path_download = release.get_filepath(SubFolder.downloaded)
                path = telescope_path(FundrefTelescope.DAG_ID, SubFolder.downloaded)
                self.assertEqual(os.path.join(path, self.fundref_test_download_file_name), file_path_download)

    @patch('academic_observatory.utils.config_utils.pathlib.Path.home')
    def test_filepath_extract(self, home_mock):
        """ Test that path of decompressed/extracted file is correct for given url.

        :param home_mock: Mock observatory home path
        :return: None.
        """
        with CliRunner().isolated_filesystem():
            # Create home path and mock getting home path
            home_path = 'user-home'
            os.makedirs(home_path, exist_ok=True)
            home_mock.return_value = home_path

            with CliRunner().isolated_filesystem():
                release = FundrefRelease(self.fundref_test_url, self.fundref_test_date)
                file_path_extract = release.get_filepath(SubFolder.extracted)
                path = telescope_path(FundrefTelescope.DAG_ID, SubFolder.extracted)
                self.assertEqual(os.path.join(path, self.fundref_test_decompress_file_name), file_path_extract)

    @patch('academic_observatory.utils.config_utils.pathlib.Path.home')
    def test_filepath_transform(self, home_mock):
        """ Test that path of transformed file is correct for given url.

        :param home_mock: Mock observatory home path
        :return: None.
        """
        with CliRunner().isolated_filesystem():
            # Create home path and mock getting home path
            home_path = 'user-home'
            os.makedirs(home_path, exist_ok=True)
            home_mock.return_value = home_path

            with CliRunner().isolated_filesystem():
                release = FundrefRelease(self.fundref_test_url, self.fundref_test_date)
                file_path_transform = release.filepath_transform
                path = telescope_path(FundrefTelescope.DAG_ID, SubFolder.transformed)
                self.assertEqual(os.path.join(path, self.fundref_test_transform_file_name), file_path_transform)

    def test_download_release_date(self):
        """ Test that the test url contains the correct date.

        :return: None.
        """
        with CliRunner().isolated_filesystem():
            release = FundrefRelease(self.fundref_test_url, self.fundref_test_date)
            self.assertEqual(self.fundref_test_date, release.date)

    def test_decompress_release(self):
        """ Test that the release is decompressed as expected.

        :return: None.
        """

        with CliRunner().isolated_filesystem():
            release = FundrefRelease(self.fundref_test_url, self.fundref_test_date)
            # 'download' release
            shutil.copyfile(self.fundref_test_path, release.filepath_download)

            decompress_file_path = decompress_release(release)
            decompress_file_name = os.path.basename(decompress_file_path)

            self.assertTrue(os.path.exists(decompress_file_path))
            self.assertEqual(self.fundref_test_decompress_file_name, decompress_file_name)
            self.assertEqual(self.fundref_test_decompress_hash, _hash_file(decompress_file_path, algorithm='md5'))

    def test_transform_release(self):
        """ Test that the release is transformed as expected.

        :return: None.
        """

        with CliRunner().isolated_filesystem():
            release = FundrefRelease(self.fundref_test_url, self.fundref_test_date)
            # 'download' release
            shutil.copyfile(self.fundref_test_path, release.filepath_download)
            # 'download' geonames
            shutil.copyfile(self.geonames_test_path, get_filepath_geonames())
            # decompress
            decompress_release(release)
            # transform
            transform_file_path = transform_release(release)
            transform_file_name = os.path.basename(transform_file_path)

            self.assertTrue(os.path.exists(transform_file_path))
            self.assertEqual(self.fundref_test_transform_file_name, transform_file_name)
            self.assertEqual(self.fundref_test_transform_hash, _hash_file(transform_file_path, algorithm='md5'))

    @patch('academic_observatory.utils.config_utils.pathlib.Path.home')
    def test_get_filepath_geonames(self, home_mock):
        """ Test that path of geonames countries file is correct.

        :param home_mock: Mock observatory home path
        :return: None.
        """
        with CliRunner().isolated_filesystem():
            # Create home path and mock getting home path
            home_path = 'user-home'
            os.makedirs(home_path, exist_ok=True)
            home_mock.return_value = home_path

            with CliRunner().isolated_filesystem():
                file_path_geonames = get_filepath_geonames()
                path = telescope_path(FundrefTelescope.DAG_ID, SubFolder.transformed)
                self.assertEqual(os.path.join(path, self.geonames_file_name), file_path_geonames)

    def test_geonames_to_dict(self):
        """ Test that correct dictionary is created from geonames dump.

        :return: None.
        """
        with CliRunner().isolated_filesystem():
            # 'download' geonames
            shutil.copyfile(self.geonames_test_path, get_filepath_geonames())
            # create dict from geonames dump
            geonames_dict = geonames_to_dict(get_filepath_geonames())

            self.assertIsInstance(geonames_dict, Dict)
            self.assertDictEqual(self.geonames_test_dict, geonames_dict)

    def test_get_geoname_data(self):
        """ Test that correct geoname data is returned for given xml element.

        :return: None.
        """
        with CliRunner().isolated_filesystem():
            # 'download' geonames
            shutil.copyfile(self.geonames_test_path, get_filepath_geonames())
            # create dict from geonames dump
            geonames_dict = geonames_to_dict(get_filepath_geonames())
            # get geoname data from a xml element from fundref registry
            name, country_code = get_geoname_data(self.fundref_nested_element, geonames_dict)

            self.assertIsInstance(name, str)
            self.assertIsInstance(country_code, str)
            self.assertEqual('United States', name)
            self.assertEqual('US', country_code)

    def test_parse_fundref_registry_rdf(self):
        """ Test that correct funders list and dictionary are returned when parsing funders registry.

        :return: None.
        """
        with CliRunner().isolated_filesystem():
            release = FundrefRelease(self.fundref_test_url, self.fundref_test_date)
            # 'download' release
            shutil.copyfile(self.fundref_test_path, release.filepath_download)
            # decompress release
            decompress_file_path = decompress_release(release)
            # 'download' geonames
            shutil.copyfile(self.geonames_test_path, get_filepath_geonames())
            # parse fundref registry
            funders, funders_by_key = parse_fundref_registry_rdf(decompress_file_path, get_filepath_geonames())

            self.assertIsInstance(funders, List)
            self.assertListEqual(funders, [
                {'funder': 'http://dx.doi.org/10.13039/100000001', 'pre_label': 'National Science Foundation',
                 'alt_label': ['NSF', 'US NSF', 'USA NSF'],
                 'narrower': ['http://dx.doi.org/10.13039/100005447', 'http://dx.doi.org/10.13039/100014591',
                              'http://dx.doi.org/10.13039/100014073', 'http://dx.doi.org/10.13039/100000081',
                              'http://dx.doi.org/10.13039/100000076', 'http://dx.doi.org/10.13039/100015815',
                              'http://dx.doi.org/10.13039/100005716', 'http://dx.doi.org/10.13039/100014072',
                              'http://dx.doi.org/10.13039/100014411', 'http://dx.doi.org/10.13039/100010608',
                              'http://dx.doi.org/10.13039/100000088', 'http://dx.doi.org/10.13039/100005441',
                              'http://dx.doi.org/10.13039/100000179', 'http://dx.doi.org/10.13039/100000085',
                              'http://dx.doi.org/10.13039/100000084', 'http://dx.doi.org/10.13039/100000083',
                              'http://dx.doi.org/10.13039/100000086', 'http://dx.doi.org/10.13039/100014074',
                              'http://dx.doi.org/10.13039/100014071'], 'broader': [],
                 'modified': '2020-02-28T20:45:11.000000', 'created': '2009-07-06T18:53:11.000000',
                 'funding_body_type': 'National government', 'funding_body_sub_type': 'gov', 'region': 'Americas',
                 'country': 'United States', 'country_code': 'US', 'state': 'Virginia', 'tax_id': None,
                 'continuation_of': [], 'renamed_as': [], 'replaces': [], 'affil_with': [], 'merged_with': [],
                 'incorporated_into': [], 'is_replaced_by': [], 'incorporates': [], 'split_into': [], 'status': None,
                 'merger_of': [], 'split_from': None, 'formly_known_as': None, 'notation': None},
                {'funder': 'http://dx.doi.org/10.13039/100000002', 'pre_label': 'National Institutes of Health',
                 'alt_label': ['NIH'],
                 'narrower': ['http://dx.doi.org/10.13039/100006545', 'http://dx.doi.org/10.13039/100012893',
                              'http://dx.doi.org/10.13039/100000066', 'http://dx.doi.org/10.13039/100000053',
                              'http://dx.doi.org/10.13039/100006086', 'http://dx.doi.org/10.13039/100000092',
                              'http://dx.doi.org/10.13039/100000052', 'http://dx.doi.org/10.13039/100000135',
                              'http://dx.doi.org/10.13039/100008460', 'http://dx.doi.org/10.13039/100000093',
                              'http://dx.doi.org/10.13039/100000097', 'http://dx.doi.org/10.13039/100000065',
                              'http://dx.doi.org/10.13039/100000118', 'http://dx.doi.org/10.13039/100000072',
                              'http://dx.doi.org/10.13039/100000060', 'http://dx.doi.org/10.13039/100000061',
                              'http://dx.doi.org/10.13039/100006084', 'http://dx.doi.org/10.13039/100009633',
                              'http://dx.doi.org/10.13039/100000055', 'http://dx.doi.org/10.13039/100006108',
                              'http://dx.doi.org/10.13039/100000071', 'http://dx.doi.org/10.13039/100000069',
                              'http://dx.doi.org/10.13039/100000054', 'http://dx.doi.org/10.13039/100000056',
                              'http://dx.doi.org/10.13039/100006955', 'http://dx.doi.org/10.13039/100000064',
                              'http://dx.doi.org/10.13039/100000049', 'http://dx.doi.org/10.13039/100000098',
                              'http://dx.doi.org/10.13039/100000057', 'http://dx.doi.org/10.13039/100000026',
                              'http://dx.doi.org/10.13039/100000025', 'http://dx.doi.org/10.13039/100005440',
                              'http://dx.doi.org/10.13039/100000096', 'http://dx.doi.org/10.13039/100000124',
                              'http://dx.doi.org/10.13039/100000027', 'http://dx.doi.org/10.13039/100000070',
                              'http://dx.doi.org/10.13039/100000051', 'http://dx.doi.org/10.13039/100006085',
                              'http://dx.doi.org/10.13039/100000062', 'http://dx.doi.org/10.13039/100000050'],
                 'broader': ['http://dx.doi.org/10.13039/100000016'], 'modified': '2020-03-12T22:59:31.000000',
                 'created': '2009-07-06T18:53:11.000000', 'funding_body_type': 'National government',
                 'funding_body_sub_type': 'gov', 'region': 'Americas', 'country': 'United States', 'country_code': 'US',
                 'state': 'Maryland', 'tax_id': None, 'continuation_of': [], 'renamed_as': [], 'replaces': [],
                 'affil_with': [], 'merged_with': [], 'incorporated_into': [], 'is_replaced_by': [], 'incorporates': [],
                 'split_into': [], 'status': None, 'merger_of': [], 'split_from': None, 'formly_known_as': None,
                 'notation': None}])
            self.assertIsInstance(funders_by_key, Dict)
            self.assertEqual(23411, len(funders_by_key))
            for key in funders_by_key:
                self.assertTrue(key.startswith('http://dx.doi.org/10.13039/'))
                self.assertIsInstance(funders_by_key[key], Dict)

    def test_recursive_funders(self):
        """ Test that correct children/parents list and depth are returned.

        :return:
        """
        with CliRunner().isolated_filesystem():
            release = FundrefRelease(self.fundref_test_url, self.fundref_test_date)
            # 'download' release
            shutil.copyfile(self.fundref_test_path, release.filepath_download)
            # decompress release
            decompress_file_path = decompress_release(release)
            # 'download' geonames
            shutil.copyfile(self.geonames_test_path, get_filepath_geonames())
            # parse fundref registry
            funders, funders_by_key = parse_fundref_registry_rdf(decompress_file_path, get_filepath_geonames())
            # iterate through funders recursively
            children, returned_depth = recursive_funders(funders_by_key, funders[1], 0, 'narrower')

            self.assertIsInstance(children, List)
            self.assertEqual(returned_depth, 1)
            for child in children:
                self.assertIn('children', child)

            parents, returned_depth = recursive_funders(funders_by_key, funders[1], 0, 'broader')
            self.assertIsInstance(parents, List)
            self.assertEqual(returned_depth, 1)
            for parent in parents:
                self.assertIn('parent', parent)

    def test_add_funders_relationships(self):
        """ Test that children are added to each funder dictionary.

        :return: None.
        """
        with CliRunner().isolated_filesystem():
            release = FundrefRelease(self.fundref_test_url, self.fundref_test_date)
            # 'download' release
            shutil.copyfile(self.fundref_test_path, release.filepath_download)
            # decompress release
            decompress_file_path = decompress_release(release)
            # 'download' geonames
            shutil.copyfile(self.geonames_test_path, get_filepath_geonames())
            # parse fundref registry
            funders, funders_by_key = parse_fundref_registry_rdf(decompress_file_path, get_filepath_geonames())
            # add funders relationships
            funders = add_funders_relationships(funders, funders_by_key)

            self.assertListEqual(funders, [
                {'funder': 'http://dx.doi.org/10.13039/100000001', 'pre_label': 'National Science Foundation',
                 'alt_label': ['NSF', 'US NSF', 'USA NSF'],
                 'narrower': ['http://dx.doi.org/10.13039/100005447', 'http://dx.doi.org/10.13039/100014591',
                              'http://dx.doi.org/10.13039/100014073', 'http://dx.doi.org/10.13039/100000081',
                              'http://dx.doi.org/10.13039/100000076', 'http://dx.doi.org/10.13039/100015815',
                              'http://dx.doi.org/10.13039/100005716', 'http://dx.doi.org/10.13039/100014072',
                              'http://dx.doi.org/10.13039/100014411', 'http://dx.doi.org/10.13039/100010608',
                              'http://dx.doi.org/10.13039/100000088', 'http://dx.doi.org/10.13039/100005441',
                              'http://dx.doi.org/10.13039/100000179', 'http://dx.doi.org/10.13039/100000085',
                              'http://dx.doi.org/10.13039/100000084', 'http://dx.doi.org/10.13039/100000083',
                              'http://dx.doi.org/10.13039/100000086', 'http://dx.doi.org/10.13039/100014074',
                              'http://dx.doi.org/10.13039/100014071'], 'broader': [],
                 'modified': '2020-02-28T20:45:11.000000', 'created': '2009-07-06T18:53:11.000000',
                 'funding_body_type': 'National government', 'funding_body_sub_type': 'gov', 'region': 'Americas',
                 'country': 'United States', 'country_code': 'US', 'state': 'Virginia', 'tax_id': None,
                 'continuation_of': [], 'renamed_as': [], 'replaces': [], 'affil_with': [], 'merged_with': [],
                 'incorporated_into': [], 'is_replaced_by': [], 'incorporates': [], 'split_into': [], 'status': None,
                 'merger_of': [], 'split_from': None, 'formly_known_as': None, 'notation': None,
                 'children': [{'funder': 'http://dx.doi.org/10.13039/100005447', 'name': None, 'children': []},
                              {'funder': 'http://dx.doi.org/10.13039/100014591', 'name': None, 'children': []},
                              {'funder': 'http://dx.doi.org/10.13039/100014073', 'name': None, 'children': []},
                              {'funder': 'http://dx.doi.org/10.13039/100000081', 'name': None, 'children': []},
                              {'funder': 'http://dx.doi.org/10.13039/100000076', 'name': None, 'children': []},
                              {'funder': 'http://dx.doi.org/10.13039/100015815', 'name': None, 'children': []},
                              {'funder': 'http://dx.doi.org/10.13039/100005716', 'name': None, 'children': []},
                              {'funder': 'http://dx.doi.org/10.13039/100014072', 'name': None, 'children': []},
                              {'funder': 'http://dx.doi.org/10.13039/100014411', 'name': None, 'children': []},
                              {'funder': 'http://dx.doi.org/10.13039/100010608', 'name': None, 'children': []},
                              {'funder': 'http://dx.doi.org/10.13039/100000088', 'name': None, 'children': []},
                              {'funder': 'http://dx.doi.org/10.13039/100005441', 'name': None, 'children': []},
                              {'funder': 'http://dx.doi.org/10.13039/100000179', 'name': None, 'children': []},
                              {'funder': 'http://dx.doi.org/10.13039/100000085', 'name': None, 'children': []},
                              {'funder': 'http://dx.doi.org/10.13039/100000084', 'name': None, 'children': []},
                              {'funder': 'http://dx.doi.org/10.13039/100000083', 'name': None, 'children': []},
                              {'funder': 'http://dx.doi.org/10.13039/100000086', 'name': None, 'children': []},
                              {'funder': 'http://dx.doi.org/10.13039/100014074', 'name': None, 'children': []},
                              {'funder': 'http://dx.doi.org/10.13039/100014071', 'name': None, 'children': []}],
                 'bottom': True, 'parents': [], 'top': False},
                {'funder': 'http://dx.doi.org/10.13039/100000002', 'pre_label': 'National Institutes of Health',
                 'alt_label': ['NIH'],
                 'narrower': ['http://dx.doi.org/10.13039/100006545', 'http://dx.doi.org/10.13039/100012893',
                              'http://dx.doi.org/10.13039/100000066', 'http://dx.doi.org/10.13039/100000053',
                              'http://dx.doi.org/10.13039/100006086', 'http://dx.doi.org/10.13039/100000092',
                              'http://dx.doi.org/10.13039/100000052', 'http://dx.doi.org/10.13039/100000135',
                              'http://dx.doi.org/10.13039/100008460', 'http://dx.doi.org/10.13039/100000093',
                              'http://dx.doi.org/10.13039/100000097', 'http://dx.doi.org/10.13039/100000065',
                              'http://dx.doi.org/10.13039/100000118', 'http://dx.doi.org/10.13039/100000072',
                              'http://dx.doi.org/10.13039/100000060', 'http://dx.doi.org/10.13039/100000061',
                              'http://dx.doi.org/10.13039/100006084', 'http://dx.doi.org/10.13039/100009633',
                              'http://dx.doi.org/10.13039/100000055', 'http://dx.doi.org/10.13039/100006108',
                              'http://dx.doi.org/10.13039/100000071', 'http://dx.doi.org/10.13039/100000069',
                              'http://dx.doi.org/10.13039/100000054', 'http://dx.doi.org/10.13039/100000056',
                              'http://dx.doi.org/10.13039/100006955', 'http://dx.doi.org/10.13039/100000064',
                              'http://dx.doi.org/10.13039/100000049', 'http://dx.doi.org/10.13039/100000098',
                              'http://dx.doi.org/10.13039/100000057', 'http://dx.doi.org/10.13039/100000026',
                              'http://dx.doi.org/10.13039/100000025', 'http://dx.doi.org/10.13039/100005440',
                              'http://dx.doi.org/10.13039/100000096', 'http://dx.doi.org/10.13039/100000124',
                              'http://dx.doi.org/10.13039/100000027', 'http://dx.doi.org/10.13039/100000070',
                              'http://dx.doi.org/10.13039/100000051', 'http://dx.doi.org/10.13039/100006085',
                              'http://dx.doi.org/10.13039/100000062', 'http://dx.doi.org/10.13039/100000050'],
                 'broader': ['http://dx.doi.org/10.13039/100000016'], 'modified': '2020-03-12T22:59:31.000000',
                 'created': '2009-07-06T18:53:11.000000', 'funding_body_type': 'National government',
                 'funding_body_sub_type': 'gov', 'region': 'Americas', 'country': 'United States', 'country_code': 'US',
                 'state': 'Maryland', 'tax_id': None, 'continuation_of': [], 'renamed_as': [], 'replaces': [],
                 'affil_with': [], 'merged_with': [], 'incorporated_into': [], 'is_replaced_by': [], 'incorporates': [],
                 'split_into': [], 'status': None, 'merger_of': [], 'split_from': None, 'formly_known_as': None,
                 'notation': None,
                 'children': [{'funder': 'http://dx.doi.org/10.13039/100006545', 'name': None, 'children': []},
                              {'funder': 'http://dx.doi.org/10.13039/100012893', 'name': None, 'children': []},
                              {'funder': 'http://dx.doi.org/10.13039/100000066', 'name': None, 'children': []},
                              {'funder': 'http://dx.doi.org/10.13039/100000053', 'name': None, 'children': []},
                              {'funder': 'http://dx.doi.org/10.13039/100006086', 'name': None, 'children': []},
                              {'funder': 'http://dx.doi.org/10.13039/100000092', 'name': None, 'children': []},
                              {'funder': 'http://dx.doi.org/10.13039/100000052', 'name': None, 'children': []},
                              {'funder': 'http://dx.doi.org/10.13039/100000135', 'name': None, 'children': []},
                              {'funder': 'http://dx.doi.org/10.13039/100008460', 'name': None, 'children': []},
                              {'funder': 'http://dx.doi.org/10.13039/100000093', 'name': None, 'children': []},
                              {'funder': 'http://dx.doi.org/10.13039/100000097', 'name': None, 'children': []},
                              {'funder': 'http://dx.doi.org/10.13039/100000065', 'name': None, 'children': []},
                              {'funder': 'http://dx.doi.org/10.13039/100000118', 'name': None, 'children': []},
                              {'funder': 'http://dx.doi.org/10.13039/100000072', 'name': None, 'children': []},
                              {'funder': 'http://dx.doi.org/10.13039/100000060', 'name': None, 'children': []},
                              {'funder': 'http://dx.doi.org/10.13039/100000061', 'name': None, 'children': []},
                              {'funder': 'http://dx.doi.org/10.13039/100006084', 'name': None, 'children': []},
                              {'funder': 'http://dx.doi.org/10.13039/100009633', 'name': None, 'children': []},
                              {'funder': 'http://dx.doi.org/10.13039/100000055', 'name': None, 'children': []},
                              {'funder': 'http://dx.doi.org/10.13039/100006108', 'name': None, 'children': []},
                              {'funder': 'http://dx.doi.org/10.13039/100000071', 'name': None, 'children': []},
                              {'funder': 'http://dx.doi.org/10.13039/100000069', 'name': None, 'children': []},
                              {'funder': 'http://dx.doi.org/10.13039/100000054', 'name': None, 'children': []},
                              {'funder': 'http://dx.doi.org/10.13039/100000056', 'name': None, 'children': []},
                              {'funder': 'http://dx.doi.org/10.13039/100006955', 'name': None, 'children': []},
                              {'funder': 'http://dx.doi.org/10.13039/100000064', 'name': None, 'children': []},
                              {'funder': 'http://dx.doi.org/10.13039/100000049', 'name': None, 'children': []},
                              {'funder': 'http://dx.doi.org/10.13039/100000098', 'name': None, 'children': []},
                              {'funder': 'http://dx.doi.org/10.13039/100000057', 'name': None, 'children': []},
                              {'funder': 'http://dx.doi.org/10.13039/100000026', 'name': None, 'children': []},
                              {'funder': 'http://dx.doi.org/10.13039/100000025', 'name': None, 'children': []},
                              {'funder': 'http://dx.doi.org/10.13039/100005440', 'name': None, 'children': []},
                              {'funder': 'http://dx.doi.org/10.13039/100000096', 'name': None, 'children': []},
                              {'funder': 'http://dx.doi.org/10.13039/100000124', 'name': None, 'children': []},
                              {'funder': 'http://dx.doi.org/10.13039/100000027', 'name': None, 'children': []},
                              {'funder': 'http://dx.doi.org/10.13039/100000070', 'name': None, 'children': []},
                              {'funder': 'http://dx.doi.org/10.13039/100000051', 'name': None, 'children': []},
                              {'funder': 'http://dx.doi.org/10.13039/100006085', 'name': None, 'children': []},
                              {'funder': 'http://dx.doi.org/10.13039/100000062', 'name': None, 'children': []},
                              {'funder': 'http://dx.doi.org/10.13039/100000050', 'name': None, 'children': []}],
                 'bottom': True,
                 'parents': [{'funder': 'http://dx.doi.org/10.13039/100000016', 'name': None, 'parent': []}],
                 'top': True}])
            for funder in funders:
                self.assertIsInstance(funder, Dict)
                self.assertIn('children', funder)
