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


import json
import logging
import unittest
from unittest.mock import patch

import pendulum
from airflow.models import Connection
from click.testing import CliRunner

from academic_observatory_workflows.config import test_fixtures_folder
from academic_observatory_workflows.workflows.web_of_science_telescope import WosJsonParser, WosUtility


# class TestRecordWosVcr(unittest.TestCase):
#     """ A test that records the WoS response into a vcr cassette. This is in lieu of creating a separate program to
#     do the recording. Comment/uncomment as needed.
#     """
#
#     def record(self, client, inst_id, period):
#         query = WosUtility.build_query(inst_id, period)
#         WosUtility.make_query(client, query)
#
#     def test_record(self):
#         period = pendulum.Period(pendulum.date(1965, 10, 1), pendulum.date(1965, 10, 1))
#
#         with vcr.use_cassette(f'/tmp/wos_cassette.yaml'):
#             with WosClient('login', 'password') as client:
#                 self.record(client, ['Curtin University'], period)


class TestWosParse(unittest.TestCase):
    """Test the various wos parsing utilities."""

    def __init__(self, *args, **kwargs):
        """Constructor which sets up variables used by tests.

        :param args: arguments.
        :param kwargs: keyword arguments.
        """

        super(TestWosParse, self).__init__(*args, **kwargs)

        # Paths
        self.work_dir = "../../../tests/observatory/dags/workflows"
        self.wos_2020_10_01_json_path = test_fixtures_folder("web_of_science", "wos-2020-10-01.json")
        with open(self.wos_2020_10_01_json_path, "r") as f:
            self.data = json.load(f)
        self.harvest_datetime = pendulum.now().isoformat()
        self.release_date = pendulum.date(2020, 10, 1).isoformat()

    def test_get_identifiers(self):
        """Extract identifiers"""
        data = self.data[0]
        identifiers = WosJsonParser.get_identifiers(data)
        self.assertEqual(len(identifiers), 10)
        self.assertEqual(identifiers["uid"], "WOS:000000000000000")
        self.assertEqual(identifiers["issn"], "0000-0000")
        self.assertEqual(identifiers["eissn"], "0000-0000")
        self.assertEqual(identifiers["doi"], "10.0000/j.gaz.2020.01.001")

    def test_get_pub_info(self):
        """Extract publication info"""
        data = self.data[0]
        pub_info = WosJsonParser.get_pub_info(data)
        self.assertEqual(pub_info["sort_date"], "2020-01-01")
        self.assertEqual(pub_info["pub_type"], "Journal")
        self.assertEqual(pub_info["page_count"], 2)
        self.assertEqual(pub_info["source"], "JUPITER GAZETTE")
        self.assertEqual(pub_info["doc_type"], "Article")
        self.assertEqual(pub_info["publisher"], "JUPITER PUBLISHING LTD")
        self.assertEqual(pub_info["publisher_city"], "SPRINGFIELD")

    def test_get_title(self):
        """Extract title"""
        data = self.data[0]
        title = WosJsonParser.get_title(data)
        truth = (
            "The habitats of endangered hypnotoads on the southern oceans of Europa: a Ophiophagus hannah perspective"
        )
        self.assertEqual(title, truth)

    def test_get_names(self):
        """Extract name information, e.g. authors"""
        data = self.data[0]
        names = WosJsonParser.get_names(data)
        self.assertEqual(len(names), 3)

        entry = names[0]
        self.assertEqual(entry["seq_no"], 1)
        self.assertEqual(entry["role"], "author")
        self.assertEqual(entry["first_name"], "Big Eaty")
        self.assertEqual(entry["last_name"], "Snake")
        self.assertEqual(entry["wos_standard"], "Snake, BE")
        self.assertEqual(entry["daisng_id"], "101010")
        self.assertEqual(entry["full_name"], "Snake, Big Eaty")
        self.assertEqual(entry["orcid"], "0000-0000-0000-0001")
        self.assertEqual(entry["r_id"], "D-0000-2000")

        entry = names[1]
        self.assertEqual(entry["seq_no"], 2)
        self.assertEqual(entry["role"], "author")
        self.assertEqual(entry["first_name"], "Hypno")
        self.assertEqual(entry["last_name"], "Toad")
        self.assertEqual(entry["wos_standard"], "Toad, H")
        self.assertEqual(entry["daisng_id"], "100000")
        self.assertEqual(entry["full_name"], "Toad, Hypno")
        self.assertEqual(entry["orcid"], "0000-0000-0000-0002")
        self.assertEqual(entry["r_id"], "H-0000-2001")

        entry = names[2]
        self.assertEqual(entry["seq_no"], 3)
        self.assertEqual(entry["role"], "author")
        self.assertEqual(entry["first_name"], "Great")
        self.assertEqual(entry["last_name"], "Historian")
        self.assertEqual(entry["wos_standard"], "Historian, G")
        self.assertEqual(entry["daisng_id"], "200000")
        self.assertEqual(entry["full_name"], "Historian, Great")
        self.assertEqual(entry["orcid"], "0000-0000-0000-0003")
        self.assertEqual(entry["r_id"], None)

    def test_get_languages(self):
        """Extract language information"""
        data = self.data[0]
        languages = WosJsonParser.get_languages(data)
        self.assertEqual(len(languages), 1)
        self.assertEqual(languages[0]["type"], "primary")
        self.assertEqual(languages[0]["name"], "Mindwaves")

    def test_get_refcount(self):
        """Extract reference count"""
        data = self.data[0]
        refs = WosJsonParser.get_refcount(data)
        self.assertEqual(refs, 10000)

    def test_get_abstract(self):
        """Extract the abstract"""
        data = self.data[0]
        abstract = WosJsonParser.get_abstract(data)
        self.assertEqual(len(abstract), 1)
        head = abstract[0][0:38]
        truth = "Jupiter hypnotoads lead mysterious liv"
        self.assertEqual(head, truth)
        self.assertEqual(len(abstract[0]), 169)

    def test_get_keyword(self):
        """Extract keywords and keywords plus if available"""
        data = self.data[0]
        keywords = WosJsonParser.get_keyword(data)
        self.assertEqual(len(keywords), 15)
        word_list = [
            "Jupiter",
            "Toads",
            "Snakes",
            "JPT",
            "JPS",
            "WORD1",
            "WORD2",
            "WORD3",
            "WORD4",
            "WORD5",
            "WORD6",
            "WORD7",
            "WORD8",
            "WORD9",
            "WORD0",
        ]
        self.assertListEqual(keywords, word_list)

    def test_get_conference(self):
        """Extract conference name"""
        data = self.data[0]
        conf = WosJsonParser.get_conference(data)
        name = "Annual Jupiter Meeting of the Minds"
        self.assertEqual(len(conf), 1)
        self.assertEqual(conf[0]["name"], name)
        self.assertEqual(conf[0]["id"], 12345)

    def test_get_fund_ack(self):
        """Extract funding information"""
        data = self.data[0]
        fund_ack = WosJsonParser.get_fund_ack(data)
        truth = "The authors would like to thank all life in the universe for not making us extinct yet."
        self.assertEqual(len(fund_ack["text"]), 1)
        self.assertEqual(fund_ack["text"][0], truth)
        self.assertEqual(len(fund_ack["grants"]), 1)
        self.assertEqual(fund_ack["grants"][0]["agency"], "Jupiter research council")
        self.assertEqual(len(fund_ack["grants"][0]["ids"]), 1)
        self.assertEqual(fund_ack["grants"][0]["ids"][0], "JP00000000HT1")

    def test_get_categories(self):
        """Extract WoS categories"""
        data = self.data[0]
        categories = WosJsonParser.get_categories(data)
        self.assertEqual(len(categories["headings"]), 1)
        self.assertEqual(len(categories["subheadings"]), 1)
        self.assertEqual(len(categories["subjects"]), 3)

        self.assertEqual(categories["headings"][0], "Hynology")
        self.assertEqual(categories["subheadings"][0], "Zoology")

        self.assertDictEqual(
            categories["subjects"][0], {"ascatype": "traditional", "code": "XX", "text": "Jupiter Toads"}
        )
        self.assertDictEqual(
            categories["subjects"][1], {"ascatype": "traditional", "code": "X", "text": "Jupiter life"}
        )
        self.assertDictEqual(
            categories["subjects"][2], {"ascatype": "extended", "code": None, "text": "Jupiter Science"}
        )

    def test_get_orgs(self):
        """Extract Wos organisations"""
        data = self.data[0]
        orgs = WosJsonParser.get_orgs(data)
        self.assertEqual(len(orgs), 1)
        self.assertEqual(orgs[0]["city"], "Springfield")
        self.assertEqual(orgs[0]["state"], "SF")
        self.assertEqual(orgs[0]["country"], "Jupiter")
        self.assertEqual(orgs[0]["org_name"], "Generic University")
        self.assertEqual(len(orgs[0]["suborgs"]), 2)
        self.assertEqual(orgs[0]["suborgs"][0], "Centre of Excellence for Extraterrestrial Telepathic Studies")
        self.assertEqual(orgs[0]["suborgs"][1], "Zoology")
        self.assertEqual(len(orgs[0]["names"]), 3)
        self.assertEqual(orgs[0]["names"][0]["first_name"], "Big Eaty")
        self.assertEqual(orgs[0]["names"][0]["last_name"], "Snake")
        self.assertEqual(orgs[0]["names"][0]["daisng_id"], "101010")
        self.assertEqual(orgs[0]["names"][0]["full_name"], "Snake, Big Eaty")
        self.assertEqual(orgs[0]["names"][0]["wos_standard"], "Snake, BE")
        self.assertEqual(orgs[0]["names"][1]["first_name"], "Hypno")
        self.assertEqual(orgs[0]["names"][1]["last_name"], "Toad")
        self.assertEqual(orgs[0]["names"][1]["daisng_id"], "100000")
        self.assertEqual(orgs[0]["names"][1]["full_name"], "Toad, Hypno")
        self.assertEqual(orgs[0]["names"][1]["wos_standard"], "Toad, H")
        self.assertEqual(orgs[0]["names"][2]["first_name"], "Great")
        self.assertEqual(orgs[0]["names"][2]["last_name"], "Historian")
        self.assertEqual(orgs[0]["names"][2]["daisng_id"], "200000")
        self.assertEqual(orgs[0]["names"][2]["full_name"], "Historian, Great")
        self.assertEqual(orgs[0]["names"][2]["wos_standard"], "Historian, G")

    def test_parse_json(self):
        """Test whether the json file can be parsed into fields correctly."""

        self.assertEqual(len(self.data), 1)
        entry = self.data[0]

        wos_inst_id = ["Generic University"]
        entry = WosJsonParser.parse_json(entry, self.harvest_datetime, self.release_date, wos_inst_id)
        self.assertEqual(entry["harvest_datetime"], self.harvest_datetime)
        self.assertEqual(entry["release_date"], self.release_date)
        self.assertEqual(entry["identifiers"]["uid"], "WOS:000000000000000")
        self.assertEqual(entry["pub_info"]["pub_type"], "Journal")
        self.assertEqual(
            entry["title"],
            "The habitats of endangered hypnotoads on the southern oceans of Europa: a Ophiophagus hannah perspective",
        )
        self.assertEqual(entry["names"][0]["first_name"], "Big Eaty")
        self.assertEqual(entry["languages"][0]["name"], "Mindwaves")
        self.assertEqual(entry["ref_count"], 10000)
        self.assertEqual(len(entry["abstract"][0]), 169)
        self.assertEqual(len(entry["keywords"]), 15)
        self.assertEqual(len(entry["conferences"]), 1)
        self.assertEqual(entry["fund_ack"]["grants"][0]["ids"][0], "JP00000000HT1")
        self.assertEqual(entry["categories"]["headings"][0], "Hynology")
        self.assertEqual(len(entry["orgs"]), 1)


class TestWos(unittest.TestCase):
    """Test the WosTelescope."""

    def __init__(self, *args, **kwargs):
        """Constructor which sets up variables used by tests.

        :param args: arguments.
        :param kwargs: keyword arguments.
        """

        super(TestWos, self).__init__(*args, **kwargs)

        logging.basicConfig()
        logging.getLogger().setLevel(logging.WARNING)

        # Connection details
        self.conn = Connection()
        self.conn.conn_id = "wos_curtin"
        self.conn.login = "test"
        self.conn.password = "test"
        self.conn.extra = '{\r\n "start_date": "2019-07-01",\r\n "id": "Curtin University" \r\n}'

        # Paths
        self.work_dir = "../../../tests/observatory/dags/workflows"

    @patch("academic_observatory_workflows.workflows.web_of_science_telescope.WosClient")
    @patch("academic_observatory_workflows.workflows.web_of_science_telescope.WosUtility.make_query", return_value=[""])
    def test_download_wos_snapshot(self, mock_query, mock_client):
        """Test whether we can successfully download and save a snapshot."""

        with CliRunner().isolated_filesystem():
            dag_start = pendulum.datetime(2019, 11, 1)
            wos_inst_id = ["Curtin University"]
            files = WosUtility.download_wos_snapshot(".", self.conn, wos_inst_id, dag_start, "sequential")
            self.assertEqual(5, len(files))
            self.assertEqual(mock_query.call_count, 5)
            self.assertEqual(mock_client.call_count, 1)
