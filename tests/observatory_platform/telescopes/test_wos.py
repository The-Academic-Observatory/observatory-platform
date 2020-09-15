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

# Author: Tuan Chien


import calendar
import json
import logging
import pendulum
import os
import unittest
import vcr
import xmltodict

from airflow.models import Connection
from click.testing import CliRunner
from collections import OrderedDict
from pathlib import Path
from wos import WosClient
from observatory_platform.telescopes.wos import (
    WosTelescope,
    WosJsonParser,
    WosUtility,
    write_pickled_xml_to_json,
)
from observatory_platform.utils.data_utils import _hash_file
from observatory_platform.utils.test_utils import gzip_file_crc

from tests.observatory_platform.config import test_fixtures_path


# class TestRecordWosVcr(unittest.TestCase):
#     """ A test that records the WoS response into a vcr cassette. This is in lieu of creating a separate program to
#     do the recording. Comment/uncomment as needed.
#     """
#
#     def record(self, client, inst_id, period):
#         query = build_query(inst_id, period)
#         make_query(client, query)
#
#     def test_record(self):
#         ts = pendulum.now().date().isoformat()
#         params = dict()
#         period = (pendulum.date(2019, 7, 1), pendulum.date(2019, 7, 31))
#
#         with vcr.use_cassette(f'/tmp/wos_{period[0].isoformat()}_{period[1].isoformat()}.yaml'):
#             with WosClient('login', 'password') as client:
#                 self.record(client, 'Curtin University', period)


class TestWosParse(unittest.TestCase):
    """ Test the various wos parsing utilities. """

    def __init__(self, *args, **kwargs):
        """ Constructor which sets up variables used by tests.

        :param args: arguments.
        :param kwargs: keyword arguments.
        """

        super(TestWosParse, self).__init__(*args, **kwargs)

        # Paths
        self.telescopes_path = os.path.join(test_fixtures_path(), 'telescopes')
        self.work_dir = '.'
        self.wos_2019_07_01_json_path = os.path.join(self.telescopes_path, 'curtin-2019-07-01.json')
        with open(self.wos_2019_07_01_json_path, 'r') as f:
            self.data = json.load(f)
        self.harvest_datetime = pendulum.now().isoformat()
        self.release_date = pendulum.date(2019, 1, 1).isoformat()

    def test_get_identifiers(self):
        """ Extract identifiers """
        data = self.data[0]
        identifiers = WosJsonParser.get_identifiers(data)
        self.assertEqual(len(identifiers), 10)
        self.assertEqual(identifiers['uid'], 'WOS:000471362100020')
        self.assertEqual(identifiers['issn'], '0951-8339')
        self.assertEqual(identifiers['eissn'], '1873-4170')
        self.assertEqual(identifiers['doi'], '10.1016/j.marstruc.2019.05.004')

    def test_get_pub_info(self):
        """ Extract publication info """
        data = self.data[0]
        pub_info = WosJsonParser.get_pub_info(data)
        self.assertEqual(pub_info['sort_date'], '2019-07-01')
        self.assertEqual(pub_info['pub_type'], 'Journal')
        self.assertEqual(pub_info['page_count'], 19)
        self.assertEqual(pub_info['source'], 'MARINE STRUCTURES')
        self.assertEqual(pub_info['doc_type'], 'Article')
        self.assertEqual(pub_info['publisher'], 'ELSEVIER SCI LTD')
        self.assertEqual(pub_info['publisher_city'], 'OXFORD')

    def test_get_title(self):
        """ Extract title """
        data = self.data[0]
        title = WosJsonParser.get_title(data)
        truth = 'Three-dimensional vortex-induced vibration of a circular cylinder at subcritical Reynolds numbers with low-R-e correction'
        self.assertEqual(title, truth)

    def test_get_names(self):
        """ Extract name information, e.g. authors """
        data = self.data[0]
        names = WosJsonParser.get_names(data)
        self.assertEqual(len(names), 3)

        entry = names[0]
        self.assertEqual(entry['seq_no'], 1)
        self.assertEqual(entry['role'], 'author')
        self.assertEqual(entry['first_name'], 'Hamid Matin')
        self.assertEqual(entry['last_name'], 'Nikoo')
        self.assertEqual(entry['wos_standard'], 'Nikoo, HM')
        self.assertEqual(entry['daisng_id'], '6771760')
        self.assertEqual(entry['full_name'], 'Nikoo, Hamid Matin')
        self.assertEqual(entry['orcid'], '0000-0002-6977-5520')
        self.assertEqual(entry['r_id'], None)

        entry = names[1]
        self.assertEqual(entry['seq_no'], 2)
        self.assertEqual(entry['role'], 'author')
        self.assertEqual(entry['first_name'], 'Kaiming')
        self.assertEqual(entry['last_name'], 'Bi')
        self.assertEqual(entry['wos_standard'], 'Bi, K')
        self.assertEqual(entry['daisng_id'], '810034')
        self.assertEqual(entry['full_name'], 'Bi, Kaiming')
        self.assertEqual(entry['orcid'], '0000-0002-5702-6119')
        self.assertEqual(entry['r_id'], 'H-7824-2015')

        entry = names[2]
        self.assertEqual(entry['seq_no'], 3)
        self.assertEqual(entry['role'], 'author')
        self.assertEqual(entry['first_name'], 'Hong')
        self.assertEqual(entry['last_name'], 'Hao')
        self.assertEqual(entry['wos_standard'], 'Hao, H')
        self.assertEqual(entry['daisng_id'], '20357')
        self.assertEqual(entry['full_name'], 'Hao, Hong')
        self.assertEqual(entry['orcid'], '0000-0001-7509-8653')
        self.assertEqual(entry['r_id'], 'D-6540-2013')

    def test_get_languages(self):
        """ Extract language information """
        data = self.data[0]
        languages = WosJsonParser.get_languages(data)
        self.assertEqual(len(languages), 1)
        self.assertEqual(languages[0]['type'], 'primary')
        self.assertEqual(languages[0]['name'], 'English')

    def test_get_refcount(self):
        """ Extract reference count """
        data = self.data[0]
        refs = WosJsonParser.get_refcount(data)
        self.assertEqual(refs, 47)

    def test_get_abstract(self):
        """ Extract the abstract """
        data = self.data[0]
        abstract = WosJsonParser.get_abstract(data)
        self.assertEqual(len(abstract),1)
        head = abstract[0][0:38]
        truth = "Reynolds-Averaged Navier-Stokes (RANS)"
        self.assertEqual(head, truth)
        self.assertEqual(len(abstract[0]), 1363)

    def test_get_keyword(self):
        """ Extract keywords and keywords plus if available """
        data = self.data[0]
        keywords = WosJsonParser.get_keyword(data)
        self.assertEqual(len(keywords), 15)
        word_list = ['3-D VIV', 'SST K - omega', 'low-R-e correction', 'CFD', 'FSI', 'VERY-LOW MASS',
                     'NUMERICAL-SIMULATION', 'RIGID CYLINDER', 'VIV', 'DYNAMICS', 'FLOW', 'UNIFORM', 'REDUCE',
                     'FORCES', 'RISER']
        self.assertListEqual(keywords, word_list)

    def test_get_conference(self):
        """ Extract conference name """
        data = self.data[216]
        conf = WosJsonParser.get_conference(data)
        name = "Annual Scientific Meeting of the Australian-and-New Zealand-Association-of-Neurologists (ANZAN)"
        self.assertEqual(len(conf), 1)
        self.assertEqual(conf[0]['name'], name)
        self.assertEqual(conf[0]['id'], 329942)


    def test_get_fund_ack(self):
        """ Extract funding information """
        data = self.data[0]
        fund_ack = WosJsonParser.get_fund_ack(data)
        truth = 'The authors would like to acknowledge the support from Australian Research Council Discovery Early Career Researcher Award (DECRA) DE150100195 for carrying out this research.'
        self.assertEqual(len(fund_ack['text']), 1)
        self.assertEqual(fund_ack['text'][0], truth)
        self.assertEqual(len(fund_ack['grants']), 1)
        self.assertEqual(fund_ack['grants'][0]['agency'], 'Australian Research Council Discovery Early Career Researcher Award (DECRA)')
        self.assertEqual(len(fund_ack['grants'][0]['ids']), 1)
        self.assertEqual(fund_ack['grants'][0]['ids'][0], 'DE150100195')

        data = self.data[216]
        fund_ack = WosJsonParser.get_fund_ack(data)
        self.assertEqual(fund_ack, {'text': [], 'grants' : []})

    def test_get_categories(self):
        """ Extract WoS categories """
        data = self.data[0]
        categories = WosJsonParser.get_categories(data)
        self.assertEqual(len(categories['headings']), 1)
        self.assertEqual(len(categories['subheadings']), 1)
        self.assertEqual(len(categories['subjects']), 3)

        self.assertEqual(categories['headings'][0], 'Science & Technology')
        self.assertEqual(categories['subheadings'][0], 'Technology')

        self.assertDictEqual(categories['subjects'][0], {'ascatype': 'traditional', 'code':'IL', 'text':'Engineering, Marine'})
        self.assertDictEqual(categories['subjects'][1], {'ascatype':'traditional', 'code': 'IM', 'text': 'Engineering, Civil'})
        self.assertDictEqual(categories['subjects'][2], {'ascatype': 'extended', 'code': None, 'text': 'Engineering'})

    def test_get_orgs(self):
        """ Extract Wos organisations """
        data = self.data[0]
        orgs = WosJsonParser.get_orgs(data)
        self.assertEqual(len(orgs), 1)
        self.assertEqual(orgs[0]['city'], 'Bentley')
        self.assertEqual(orgs[0]['state'], 'WA')
        self.assertEqual(orgs[0]['country'], 'Australia')
        self.assertEqual(orgs[0]['org_name'], 'Curtin University')
        self.assertEqual(len(orgs[0]['suborgs']), 2)
        self.assertEqual(orgs[0]['suborgs'][0], 'Ctr Infrastruct Monitoring & Protect')
        self.assertEqual(orgs[0]['suborgs'][1], 'Sch Civil & Mech Engn')
        self.assertEqual(len(orgs[0]['names']), 3)
        self.assertEqual(orgs[0]['names'][0]['first_name'], 'Hamid Matin')
        self.assertEqual(orgs[0]['names'][0]['last_name'], 'Nikoo')
        self.assertEqual(orgs[0]['names'][0]['daisng_id'], '6771760')
        self.assertEqual(orgs[0]['names'][0]['full_name'], 'Nikoo, Hamid Matin')
        self.assertEqual(orgs[0]['names'][0]['wos_standard'], 'Nikoo, HM')
        self.assertEqual(orgs[0]['names'][1]['first_name'], 'Kaiming')
        self.assertEqual(orgs[0]['names'][1]['last_name'], 'Bi')
        self.assertEqual(orgs[0]['names'][1]['daisng_id'], '810034')
        self.assertEqual(orgs[0]['names'][1]['full_name'], 'Bi, Kaiming')
        self.assertEqual(orgs[0]['names'][1]['wos_standard'], 'Bi, K')
        self.assertEqual(orgs[0]['names'][2]['first_name'], 'Hong')
        self.assertEqual(orgs[0]['names'][2]['last_name'], 'Hao')
        self.assertEqual(orgs[0]['names'][2]['daisng_id'], '20357')
        self.assertEqual(orgs[0]['names'][2]['full_name'], 'Hao, Hong')
        self.assertEqual(orgs[0]['names'][2]['wos_standard'], 'Hao, H')

    def test_parse_json(self):
        """ Test whether the json file can be parsed into fields correctly. """

        self.assertEqual(len(self.data), 429)
        entry = self.data[0]
        entry = WosJsonParser.parse_json(entry, self.harvest_datetime, self.release_date)
        self.assertEqual(entry['harvest_datetime'], self.harvest_datetime)
        self.assertEqual(entry['release_date'], self.release_date)
        self.assertEqual(entry['identifiers']['uid'], 'WOS:000471362100020')
        self.assertEqual(entry['pub_info']['pub_type'], 'Journal')
        self.assertEqual(entry['title'], 'Three-dimensional vortex-induced vibration of a circular cylinder at subcritical Reynolds numbers with low-R-e correction')
        self.assertEqual(entry['names'][0]['first_name'], 'Hamid Matin')
        self.assertEqual(entry['languages'][0]['name'], 'English')
        self.assertEqual(entry['ref_count'], 47)
        self.assertEqual(len(entry['abstract'][0]), 1363)
        self.assertEqual(len(entry['keywords']), 15)
        self.assertEqual(len(entry['conferences']), 0)
        self.assertEqual(entry['fund_ack']['grants'][0]['ids'][0], 'DE150100195')
        self.assertEqual(entry['categories']['headings'][0], 'Science & Technology')
        self.assertEqual(len(entry['orgs']), 1)


class TestWos(unittest.TestCase):
    """Test the WosTelescope."""

    def __init__(self, *args, **kwargs):
        """ Constructor which sets up variables used by tests.

        :param args: arguments.
        :param kwargs: keyword arguments.
        """

        super(TestWos, self).__init__(*args, **kwargs)

        logging.basicConfig()
        logging.getLogger().setLevel(logging.WARNING)

        # Connection details
        self.conn = Connection()
        self.conn.conn_id = 'wos_curtin'
        self.conn.login = 'test'
        self.conn.password = 'test'
        self.conn.extra = '{\r\n "start_date": "2019-07-01",\r\n "id": "Curtin University" \r\n}'

        # Paths
        self.vcr_cassettes_path = os.path.join(test_fixtures_path(), 'vcr_cassettes')
        self.work_dir = '.'

        # Wos Snapshot 2019-07-01 to 2019-07-31
        self.wos_2019_07_01_path = os.path.join(self.vcr_cassettes_path, 'wos_2019-07-01.yaml')
        self.wos_2019_07_01_expected_hash = 'e380d64610b05bac49f1ce70bd442050'
        self.wos_2019_07_01_json_hash = '638e66049e0147bbd8fbbf9699adc1ca'

    def test_download_wos_snapshot(self):
        """ Test whether we can successfully download and save a snapshot. """

        with CliRunner().isolated_filesystem():
            with vcr.use_cassette(self.wos_2019_07_01_path):
                dag_start = pendulum.date(2019, 7, 31)
                files = WosUtility.download_wos_snapshot('.', self.conn, dag_start, 'sequential')
                # Check that returned downloads has correct length
                self.assertEqual(1, len(files))

                # Check that file has expected hash
                file_path = files[0]

                self.assertGreater(Path(self.wos_2019_07_01_path).stat().st_size, 500000)
                self.assertGreater(Path(file_path).stat().st_size, 500000)

                self.assertTrue(os.path.exists(file_path))
                self.assertEqual(self.wos_2019_07_01_expected_hash, _hash_file(file_path, algorithm='md5'))

    def test_transform_xml(self):
        """ Test whether we can transform xml to json correctly. """

        with CliRunner().isolated_filesystem():
            with vcr.use_cassette(self.wos_2019_07_01_path):
                dag_start = pendulum.date(2019, 7, 31)
                files = WosUtility.download_wos_snapshot('.', self.conn, dag_start, 'sequential')
                json_file_list = write_pickled_xml_to_json(files, WosUtility.parse_query)
                self.assertEqual(len(files), len(json_file_list))
                json_file = json_file_list[0]
                self.assertEqual(self.wos_2019_07_01_json_hash, _hash_file(json_file, algorithm='md5'))
