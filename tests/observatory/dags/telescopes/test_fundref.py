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

import logging
import os
import shutil
import unittest
import xml.etree.ElementTree as ET
from typing import List, Dict
from unittest.mock import patch

import pendulum
import vcr
from click.testing import CliRunner

from observatory.dags.telescopes.fundref import (
    FundrefRelease,
    FundrefTelescope,
    add_funders_relationships,
    extract_release,
    list_releases,
    parse_fundref_registry_rdf,
    recursive_funders,
    transform_release,
    download_release
)
from observatory.platform.utils.config_utils import telescope_path, SubFolder
from observatory.platform.utils.data_utils import _hash_file
from observatory.platform.utils.test_utils import gzip_file_crc
from tests.observatory.config import test_fixtures_path


class TestFundref(unittest.TestCase):
    """ Tests for the functions used by the fundref telescope """

    def __init__(self, *args, **kwargs):
        """ Constructor which sets up variables used by tests.

        :param args: arguments.
        :param kwargs: keyword arguments.
        """

        super(TestFundref, self).__init__(*args, **kwargs)

        # FundRef releases list
        self.list_fundref_releases_hash = 'a7cf8190dcbda7992e3ae839ebab9f95'

        # FundRef test release
        self.fundref_test_path = os.path.join(test_fixtures_path(), 'telescopes', 'fundref.tar.gz')
        self.fundref_test_url = FundrefTelescope.TELESCOPE_DEBUG_URL
        self.fundref_nested_element = ET.Element(
            '{http://data.crossref.org/fundingdata/xml/schema/grant/grant-1.2/}country',
            attrib={'{http://www.w3.org/1999/02/22-rdf-syntax-ns#}resource': 'http://sws.geonames.org/6252001/'})
        self.fundref_nested_element.tail = '\n      '
        self.fundref_test_date = pendulum.datetime(year=3000, month=1, day=1)
        self.fundref_test_download_file_name = 'fundref_3000_01_01.tar.gz'
        self.fundref_test_decompress_file_name = 'fundref_3000_01_01.rdf'
        self.fundref_test_transform_file_name = 'fundref_3000_01_01.jsonl.gz'
        self.fundref_test_download_hash = 'c9c61c5053208752e8926f159d58b101'
        self.fundref_test_decompress_hash = 'ed14c816d89b4334675bd11514f9cac2'
        self.fundref_test_transform_hash = '60ac8e56'
        self.start_date = pendulum.datetime(year=2014, month=3, day=1)
        self.end_date = pendulum.datetime(year=2020, month=1, day=15)

        # Turn logging to warning because vcr prints too much at info level
        logging.basicConfig()
        logging.getLogger().setLevel(logging.WARNING)

    @patch('observatory.platform.utils.config_utils.airflow.models.Variable.get')
    def test_list_releases(self, mock_variable_get):
        """ Test that list releases returns a dictionary with releases.

        :return: None.
        """

        # Mock data variable
        data_path = 'data'
        mock_variable_get.return_value = data_path
        cassette_path = os.path.join(test_fixtures_path(),
                                     'vcr_cassettes',
                                     'list_fundref_releases.yaml')

        with CliRunner().isolated_filesystem():
            with vcr.use_cassette(cassette_path):
                releases = list_releases(self.start_date, self.end_date)
                self.assertIsInstance(releases, List)
                for release in releases:
                    self.assertIsInstance(release, FundrefRelease)
                self.assertEqual(39, len(releases))

    @patch('observatory.platform.utils.config_utils.airflow.models.Variable.get')
    def test_download_release(self, mock_variable_get):
        # Mock data variable
        data_path = 'data'
        mock_variable_get.return_value = data_path
        cassette_path = os.path.join(test_fixtures_path(),
                                     'vcr_cassettes',
                                     'fundref_download_release.yaml')
        url = 'https://gitlab.com/crossref/open_funder_registry/-/archive/v1.30/open_funder_registry-v1.30.tar.gz'
        date = pendulum.datetime(year=2020, month=1, day=14, hour=14, minute=51, second=51)
        expected_crc = '87235a1d'

        with CliRunner().isolated_filesystem():
            with vcr.use_cassette(cassette_path):
                release = FundrefRelease(url, date)
                path = download_release(release)
                self.assertEqual(expected_crc, gzip_file_crc(path))

    @patch('observatory.platform.utils.config_utils.airflow.models.Variable.get')
    def test_filepath_download(self, mock_variable_get):
        """ Test that path of downloaded file is correct for given url.

        :param home_mock: Mock observatory home path
        :return: None.
        """

        # Mock data variable
        data_path = 'data'
        mock_variable_get.return_value = data_path

        with CliRunner().isolated_filesystem():
            release = FundrefRelease(self.fundref_test_url, self.fundref_test_date)
            file_path_download = release.get_filepath(SubFolder.downloaded)
            path = telescope_path(SubFolder.downloaded, FundrefTelescope.DAG_ID)
            self.assertEqual(os.path.join(path, self.fundref_test_download_file_name), file_path_download)

    @patch('observatory.platform.utils.config_utils.airflow.models.Variable.get')
    def test_filepath_extract(self, mock_variable_get):
        """ Test that path of decompressed/extracted file is correct for given url.

        :param home_mock: Mock observatory home path
        :return: None.
        """

        # Mock data variable
        data_path = 'data'
        mock_variable_get.return_value = data_path

        with CliRunner().isolated_filesystem():
            release = FundrefRelease(self.fundref_test_url, self.fundref_test_date)
            file_path_extract = release.get_filepath(SubFolder.extracted)
            path = telescope_path(SubFolder.extracted, FundrefTelescope.DAG_ID)
            self.assertEqual(os.path.join(path, self.fundref_test_decompress_file_name), file_path_extract)

    @patch('observatory.platform.utils.config_utils.airflow.models.Variable.get')
    def test_filepath_transform(self, mock_variable_get):
        """ Test that path of transformed file is correct for given url.

        :param home_mock: Mock observatory home path
        :return: None.
        """

        # Mock data variable
        data_path = 'data'
        mock_variable_get.return_value = data_path

        with CliRunner().isolated_filesystem():
            release = FundrefRelease(self.fundref_test_url, self.fundref_test_date)
            file_path_transform = release.filepath_transform
            path = telescope_path(SubFolder.transformed, FundrefTelescope.DAG_ID)
            self.assertEqual(os.path.join(path, self.fundref_test_transform_file_name), file_path_transform)

    @patch('observatory.platform.utils.config_utils.airflow.models.Variable.get')
    def test_download_release_date(self, mock_variable_get):
        """ Test that the test url contains the correct date.

        :return: None.
        """

        # Mock data variable
        data_path = 'data'
        mock_variable_get.return_value = data_path

        with CliRunner().isolated_filesystem():
            release = FundrefRelease(self.fundref_test_url, self.fundref_test_date)
            self.assertEqual(self.fundref_test_date, release.date)

    @patch('observatory.platform.utils.config_utils.airflow.models.Variable.get')
    def test_extract_release(self, mock_variable_get):
        """ Test that the release is decompressed as expected.

        :return: None.
        """

        # Mock data variable
        data_path = 'data'
        mock_variable_get.return_value = data_path

        with CliRunner().isolated_filesystem():
            release = FundrefRelease(self.fundref_test_url, self.fundref_test_date)
            # 'download' release
            shutil.copyfile(self.fundref_test_path, release.filepath_download)

            decompress_file_path = extract_release(release)
            decompress_file_name = os.path.basename(decompress_file_path)

            self.assertTrue(os.path.exists(decompress_file_path))
            self.assertEqual(self.fundref_test_decompress_file_name, decompress_file_name)
            self.assertEqual(self.fundref_test_decompress_hash, _hash_file(decompress_file_path, algorithm='md5'))

    @patch('observatory.platform.utils.config_utils.airflow.models.Variable.get')
    def test_transform_release(self, mock_variable_get):
        """ Test that the release is transformed as expected.

        :return: None.
        """

        # Mock data variable
        data_path = 'data'
        mock_variable_get.return_value = data_path

        with CliRunner().isolated_filesystem():
            release = FundrefRelease(self.fundref_test_url, self.fundref_test_date)
            # 'download' release
            shutil.copyfile(self.fundref_test_path, release.filepath_download)
            # decompress
            extract_release(release)
            # transform
            transform_file_path = transform_release(release)
            transform_file_name = os.path.basename(transform_file_path)

            self.assertTrue(os.path.exists(transform_file_path))
            self.assertEqual(self.fundref_test_transform_file_name, transform_file_name)
            self.assertEqual(self.fundref_test_transform_hash, gzip_file_crc(transform_file_path))

    @patch('observatory.platform.utils.config_utils.airflow.models.Variable.get')
    def test_parse_fundref_registry_rdf(self, mock_variable_get):
        """ Test that correct funders list and dictionary are returned when parsing funders registry.

        :return: None.
        """

        # Mock data variable
        data_path = 'data'
        mock_variable_get.return_value = data_path

        with CliRunner().isolated_filesystem():
            release = FundrefRelease(self.fundref_test_url, self.fundref_test_date)
            # 'download' release
            shutil.copyfile(self.fundref_test_path, release.filepath_download)
            # decompress release
            decompress_file_path = extract_release(release)
            # parse FundRef registry
            funders, funders_by_key = parse_fundref_registry_rdf(decompress_file_path)

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
                 'country': None, 'country_code': 'usa', 'state': None, 'tax_id': None, 'continuation_of': [],
                 'renamed_as': [], 'replaces': [], 'affil_with': [], 'merged_with': [], 'incorporated_into': [],
                 'is_replaced_by': [], 'incorporates': [], 'split_into': [], 'status': None, 'merger_of': [],
                 'split_from': None, 'formly_known_as': None, 'notation': None},
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
                 'funding_body_sub_type': 'gov', 'region': 'Americas', 'country': None, 'country_code': 'usa',
                 'state': None, 'tax_id': None, 'continuation_of': [], 'renamed_as': [], 'replaces': [],
                 'affil_with': [], 'merged_with': [], 'incorporated_into': [], 'is_replaced_by': [], 'incorporates': [],
                 'split_into': [], 'status': None, 'merger_of': [], 'split_from': None, 'formly_known_as': None,
                 'notation': None}])
            self.assertIsInstance(funders_by_key, Dict)
            self.assertEqual(23411, len(funders_by_key))
            for key in funders_by_key:
                self.assertTrue(key.startswith('http://dx.doi.org/10.13039/'))
                self.assertIsInstance(funders_by_key[key], Dict)

    @patch('observatory.platform.utils.config_utils.airflow.models.Variable.get')
    def test_recursive_funders(self, mock_variable_get):
        """ Test that correct children/parents list and depth are returned.

        :return:
        """

        # Mock data variable
        data_path = 'data'
        mock_variable_get.return_value = data_path

        with CliRunner().isolated_filesystem():
            release = FundrefRelease(self.fundref_test_url, self.fundref_test_date)
            # 'download' release
            shutil.copyfile(self.fundref_test_path, release.filepath_download)
            # decompress release
            decompress_file_path = extract_release(release)
            # parse FundRef registry
            funders, funders_by_key = parse_fundref_registry_rdf(decompress_file_path)
            # iterate through funders recursively
            children, returned_depth = recursive_funders(funders_by_key, funders[1], 0, 'narrower', [])

            self.assertIsInstance(children, List)
            self.assertEqual(returned_depth, 1)
            for child in children:
                self.assertIn('children', child)

            parents, returned_depth = recursive_funders(funders_by_key, funders[1], 0, 'broader', [])
            self.assertIsInstance(parents, List)
            self.assertEqual(returned_depth, 1)
            for parent in parents:
                self.assertIn('parent', parent)

    @patch('observatory.platform.utils.config_utils.airflow.models.Variable.get')
    def test_add_funders_relationships(self, mock_variable_get):
        """ Test that children are added to each funder dictionary.

        :return: None.
        """

        # Mock data variable
        data_path = 'data'
        mock_variable_get.return_value = data_path

        with CliRunner().isolated_filesystem():
            release = FundrefRelease(self.fundref_test_url, self.fundref_test_date)
            # 'download' release
            shutil.copyfile(self.fundref_test_path, release.filepath_download)
            # decompress release
            decompress_file_path = extract_release(release)
            # parse FundRef registry
            funders, funders_by_key = parse_fundref_registry_rdf(decompress_file_path)
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
                 'country': None, 'country_code': 'usa', 'state': None, 'tax_id': None, 'continuation_of': [],
                 'renamed_as': [], 'replaces': [], 'affil_with': [], 'merged_with': [], 'incorporated_into': [],
                 'is_replaced_by': [], 'incorporates': [], 'split_into': [], 'status': None, 'merger_of': [],
                 'split_from': None, 'formly_known_as': None, 'notation': None,
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
                 'funding_body_sub_type': 'gov', 'region': 'Americas', 'country': None, 'country_code': 'usa',
                 'state': None, 'tax_id': None, 'continuation_of': [], 'renamed_as': [], 'replaces': [],
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
