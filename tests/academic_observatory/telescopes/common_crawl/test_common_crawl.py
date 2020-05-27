# Copyright 2019 Curtin University
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

# Author: James Diprose

import datetime
import filecmp
import gzip
import os
import shutil
import unittest

from academic_observatory.telescopes.common_crawl import WarcIndex, PageInfo, InstitutionIndex, \
    WarcIndexInfo, common_crawl_path, common_crawl_serialize_custom_types, save_page_infos
from academic_observatory.utils import test_data_dir
from academic_observatory.utils.config_utils import observatory_home


class TestCommonCrawl(unittest.TestCase):

    def test_common_crawl_path(self):
        path = common_crawl_path()
        self.assertTrue(os.path.exists(path))

    def test_common_crawl_serialize_custom_types(self):
        # datetime.datetime
        datetime_instance = datetime.datetime(year=2020, month=1, day=1, hour=1, minute=2, second=3, microsecond=0,
                                              tzinfo=datetime.timezone.utc)
        datetime_actual = common_crawl_serialize_custom_types(datetime_instance)
        datetime_expected = '2020-01-01T01:02:03+00:00'
        self.assertEqual(datetime_actual, datetime_expected)

        # datetime.date
        date_instance = datetime.date(year=2020, month=1, day=1)
        date_actual = common_crawl_serialize_custom_types(date_instance)
        date_expected = '2020-01-01'
        self.assertEqual(date_actual, date_expected)

        # non supported type
        with self.assertRaises(TypeError):
            common_crawl_serialize_custom_types(WarcIndex('', '', 0, 0))

    def test_save_page_infos(self):
        # Create page_infos List
        fetch_month = datetime.date(year=2019, month=8, day=1)
        institution_index = InstitutionIndex.from_dict({"grid_id": "grid.1032.0",
                                                        "institute_name": "Curtin University",
                                                        "institute_type": "Education",
                                                        "institute_url": "http://www.curtin.edu.au/",
                                                        "institute_country_code": "AU"})
        warc_index = WarcIndex.from_dict(
            {"url": "https://cbs.curtin.edu.au/index.cfm?objectId=32573652-B92A-DD42-3829C962C16E51D1",
             "warc_filename": "crawl-data/CC-MAIN-2019-35/segments/1566027330968.54/crawldiagnostics/CC-MAIN-20190826042816-20190826064816-00158.warc.gz",
             "warc_record_offset": 13734848, "warc_record_length": 819})
        warc_index_info = WarcIndexInfo.from_dict({"fetch_month": fetch_month,
                                                   "url_surtkey": "au,edu,curtin,cbs)/index.cfm?objectid=32573652-b92a-dd42-3829c962c16e51d1",
                                                   "url": "https://cbs.curtin.edu.au/index.cfm?objectId=32573652-B92A-DD42-3829C962C16E51D1",
                                                   "url_host_name": "cbs.curtin.edu.au", "url_host_tld": "au",
                                                   "url_host_2nd_last_part": "edu",
                                                   "url_host_3rd_last_part": "curtin", "url_host_4th_last_part": "cbs",
                                                   "url_host_5th_last_part": None,
                                                   "url_host_registry_suffix": "edu.au",
                                                   "url_host_registered_domain": "curtin.edu.au",
                                                   "url_host_private_suffix": "edu.au",
                                                   "url_host_private_domain": "curtin.edu.au", "url_protocol": "https",
                                                   "url_port": None, "url_path": "/index.cfm",
                                                   "url_query": "objectId=32573652-B92A-DD42-3829C962C16E51D1",
                                                   "fetch_time": "2019-08-26T05:26:28+00:00", "fetch_status": 301,
                                                   "content_digest": "QTMNMIW5GEUJUF55ED2BVP5BQRZECYFD",
                                                   "content_mime_type": "text/html",
                                                   "content_mime_detected": "text/html", "content_charset": None,
                                                   "content_languages": None})
        page_info = PageInfo(institution_index, warc_index, warc_index_info)
        page_infos = [page_info]

        # Create other parameters
        output_path = observatory_home('tests')
        table_name = 'test_table'
        grid_id = 'grid.1032.0'
        start_time = datetime.datetime(year=2020, month=1, day=1, hour=1, minute=0,
                                       second=0, microsecond=0, tzinfo=datetime.timezone.utc)
        batch_number = 0

        # Save page info results
        save_path = save_page_infos(page_infos, output_path, table_name, start_time, grid_id, fetch_month, batch_number)

        # Extract gzipped file
        extracted_path = save_path[:-5]
        with gzip.open(save_path, 'rb') as file_gzip:
            with open(extracted_path, 'wb') as file_extracted:
                shutil.copyfileobj(file_gzip, file_extracted)

        # Path for test file
        test_data_dir_ = test_data_dir(__file__)
        cc_content_test_path = os.path.join(test_data_dir_, 'common_crawl', 'cc_content_test.json')

        # Check that the generated and expected files match
        self.assertTrue(filecmp.cmp(extracted_path, cc_content_test_path))
        os.remove(save_path)
        os.remove(extracted_path)
