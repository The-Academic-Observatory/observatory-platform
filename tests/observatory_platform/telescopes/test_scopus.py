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


import pendulum
import unittest
import unittest.mock as mock

from queue import Queue
from click.testing import CliRunner
from unittest.mock import patch

from observatory_platform.telescopes.scopus import (
    ScopusRelease,
    ScopusUtility,
    ScopusUtilWorker,
    ScopusJsonParser,
    ScopusClient
)

from observatory_platform.utils.telescope_utils import (
    build_schedule,
    SchedulePeriod,
)


class TestScopusRelease(unittest.TestCase):
    """ Test the ScopusRelease class. """

    @patch('observatory_platform.telescopes.scopus.telescope_path', return_value='test')
    def test_init(self, mock_target):
        """ Test initialisation. """

        obj = ScopusRelease(inst_id='inst_id', scopus_inst_id=['scopus_inst_id'],
                            release_date=pendulum.date(1984, 1, 1),
                            start_date=pendulum.date(2000, 5, 1),
                            end_date=pendulum.date(2000, 1, 1), project_id='project_id',
                            download_bucket_name='download_bucket', transform_bucket_name='transform_bucket',
                            data_location='data_location', schema_ver='schema_ver', view='standard')

        self.assertEqual(obj.inst_id, 'inst_id')
        self.assertEqual(obj.scopus_inst_id[0], 'scopus_inst_id')
        self.assertEqual(obj.release_date, pendulum.date(1984, 1, 1))
        self.assertEqual(obj.start_date, pendulum.date(2000, 5, 1))
        self.assertEqual(obj.end_date, pendulum.date(2000, 1, 1))
        self.assertEqual(obj.project_id, 'project_id')
        self.assertEqual(obj.download_path, 'test')
        self.assertEqual(obj.transform_path, 'test')
        self.assertEqual(obj.telescope_path, 'telescopes/scopus/1984-01-01')
        self.assertEqual(obj.download_bucket_name, 'download_bucket')
        self.assertEqual(obj.transform_bucket_name, 'transform_bucket')
        self.assertEqual(obj.data_location, 'data_location')
        self.assertEqual(obj.schema_ver, 'schema_ver')
        self.assertEqual(obj.view, 'standard')
        self.assertEqual(mock_target.call_count, 2)

class TestScopusUtilWorker(unittest.TestCase):
    """ Test the ScopusUtilWorker class. """

    def test_init(self):
        """ Test the constructor. """

        now = pendulum.now('UTC')
        client = ScopusClient('api_key', 'standard')
        client_id = 1
        worker = ScopusUtilWorker(client_id, client, now, 20000)
        self.assertEqual(now, worker.quota_reset_date)
        self.assertEqual(client_id, worker.client_id)
        self.assertTrue(isinstance(worker.client, ScopusClient))


class TestScopusUtility(unittest.TestCase):
    """ Test the SCOPUS utility class. """

    def __init__(self, *args, **kwargs):
        super(TestScopusUtility, self).__init__(*args, **kwargs)
        self.scopus_1990_09_01_path = '/tmp/scopus_curtin_1990_09_01.yml'
        self.reset_past = pendulum.datetime(2019, 1, 1, 11, 11, 11)
        self.reset_future = pendulum.datetime(9999, 9, 29, 1, 11, 11)
        self.conn = 'test'
        self.api_key1 = "test_key1"
        self.api_key2 = "test_key2"
        self.scopus_inst_id = ["60031226"]  # Curtin University
        self.workers = [ScopusUtilWorker(1, ScopusClient(self.api_key1, 'standard'), self.reset_past, 20000),
                        ScopusUtilWorker(2, ScopusClient(self.api_key2, 'standard'), self.reset_past, 20000)]
        self.conn = 'scopus_curtin'
        self.workdir = '.'
        self.schedule = build_schedule(pendulum.date(1990, 5, 1), pendulum.date(1990, 9, 1))

    def test_build_query(self):
        """ Test query builder. """

        scopus_inst_id = ["test1", "test2"]
        period = SchedulePeriod(pendulum.date(2018, 10, 1), pendulum.date(2019, 2, 1))
        query = ScopusUtility.build_query(scopus_inst_id, period)
        query_truth = '(AF-ID(test1) OR AF-ID(test2)) AND PUBDATETXT("October 2018" or "November 2018" or "December 2018" or "January 2019" or "February 2019")'
        self.assertEqual(query, query_truth)

    @patch('observatory_platform.telescopes.scopus.sleep')
    def test_sleep_if_needed(self, sleep_mock):
        """ Test sleep calculation. """
        ScopusUtility.sleep_if_needed(self.reset_past, self.conn)
        sleep_mock.assert_not_called()

        ScopusUtility.sleep_if_needed(self.reset_future, self.conn)
        sleep_mock.assert_called_once()

    def test_qe_worker_maintenace(self):
        """ Test quota exceeded worker maintenance. """

        workerq = Queue()
        qe_workers = [ScopusUtilWorker(1, ScopusClient('test', 'standard'), self.reset_past, 20000)]
        qe_workers = ScopusUtility.qe_worker_maintenance(qe_workers, workerq, 'test')
        self.assertEqual(workerq.qsize(), 1)
        self.assertEqual(len(qe_workers), 0)

        workerq = Queue()
        qe_workers = [ScopusUtilWorker(1, ScopusClient('test', 'standard'), self.reset_future, 20000)]
        qe_workers = ScopusUtility.qe_worker_maintenance(qe_workers, workerq, 'test')
        self.assertEqual(workerq.qsize(), 0)
        self.assertEqual(len(qe_workers), 1)

    def test_update_reset_date(self):
        """ Test updating of reset date. """

        reset_date = pendulum.now('UTC')
        error_msg = f'{ScopusClient.QUOTA_EXCEED_ERROR_PREFIX}9999999999999'
        worker = ScopusUtilWorker(1, ScopusClient('test'), reset_date, 20000)

        new_reset = ScopusUtility.update_reset_date(self.conn, error_msg, worker, reset_date)
        self.assertTrue(new_reset > reset_date)

        error_msg = f'{ScopusClient.QUOTA_EXCEED_ERROR_PREFIX}0'
        new_reset = ScopusUtility.update_reset_date(self.conn, error_msg, worker, reset_date)
        self.assertEqual(new_reset, pendulum.datetime(1970, 1, 1, 0, 0, 0))

    @patch('observatory_platform.telescopes.scopus.ScopusUtility.make_query', return_value=('', 0))
    def test_download_scopus_period(self, mock_make_query):
        """ Test downloading of a period. Mocks out actual api call. """

        with CliRunner().isolated_filesystem():
            period = SchedulePeriod(pendulum.date(1990, 9, 1), pendulum.date(1990, 9, 30))
            worker = ScopusUtilWorker(1, ScopusClient(self.api_key1), pendulum.now('UTC'), 20000)
            save_file = ScopusUtility.download_scopus_period(worker, self.conn, period, self.scopus_inst_id,
                                                             self.workdir)
            self.assertEqual(mock_make_query.call_count, 1)
            self.assertNotEqual(save_file, '')

    @patch('observatory_platform.telescopes.scopus.ScopusUtilWorker.QUEUE_WAIT_TIME', 1)
    @patch('observatory_platform.telescopes.scopus.ScopusUtility.make_query', return_value=('', 0))
    def test_sequential(self, mock_make_query):
        """ Sequential download test. """

        now = pendulum.now('UTC')
        workers = list()

        workers.append(ScopusUtilWorker(1, ScopusClient(self.api_key1), now, 20000))
        workers.append(ScopusUtilWorker(2, ScopusClient(self.api_key2), now, 20000))

        taskq = Queue()
        for period in self.schedule:
            taskq.put(period)
        download_path = '/tmp'

        def side_effect(*args):
            side_effect.count += 1
            if side_effect.count <= 1:
                raise Exception(f'{ScopusClient.QUOTA_EXCEED_ERROR_PREFIX}1601433684000')
            return mock.DEFAULT
        side_effect.count = 0
        mock_make_query.side_effect = side_effect

        with CliRunner().isolated_filesystem():
            saved_files = ScopusUtility.download_sequential(workers, taskq, self.conn, self.scopus_inst_id,
                                                            download_path)
            self.assertEqual(mock_make_query.call_count, 6)
            self.assertEqual(len(saved_files), 5)

    @patch('observatory_platform.telescopes.scopus.ScopusUtilWorker.QUEUE_WAIT_TIME', 1)
    @patch('observatory_platform.telescopes.scopus.ScopusUtility.make_query', return_value=('', 0))
    def test_parallel(self, mock_make_query):
        """ Parallel download test. """

        taskq = Queue()
        for period in self.schedule:
            taskq.put(period)
        download_path = '/tmp'

        def side_effect(*args):
            side_effect.count += 1
            if side_effect.count <= 4:
                raise Exception(f'{ScopusClient.QUOTA_EXCEED_ERROR_PREFIX}1601433684000')
            return mock.DEFAULT
        side_effect.count = 0
        mock_make_query.side_effect = side_effect

        with CliRunner().isolated_filesystem():
            saved_files = ScopusUtility.download_parallel(self.workers, taskq, self.conn, self.scopus_inst_id,
                                                          download_path)
            self.assertEqual(mock_make_query.call_count, 9)
            self.assertEqual(len(saved_files), 5)

    @patch('observatory_platform.telescopes.scopus.ScopusUtilWorker.QUEUE_WAIT_TIME', 1)
    @patch('observatory_platform.telescopes.scopus.telescope_path', return_value='test')
    @patch('observatory_platform.telescopes.scopus.ScopusUtility.make_query', return_value=('', 0))
    def test_download_snapshot(self, mock_make_query, mock_telepath):
        """ Download snapshot test. """

        release = ScopusRelease(inst_id='curtin', scopus_inst_id=['60031226'],
                                release_date=pendulum.date(2020, 1, 1),
                                start_date=pendulum.date(1990, 5, 1),
                                end_date=pendulum.date(1990, 9, 1), project_id='project_id',
                                download_bucket_name='download_bucket', transform_bucket_name='transform_bucket',
                                data_location='data_location', schema_ver='schema_ver', view='standard')
        self.assertEqual(mock_telepath.call_count, 2)

        with CliRunner().isolated_filesystem():
            api_keys = [{"key": self.api_key1}, {"key": self.api_key2}]
            saved_files = ScopusUtility.download_snapshot(api_keys, release, 'sequential')
            self.assertEqual(mock_make_query.call_count, 5)
            self.assertEqual(len(saved_files), 5)
            saved_files = ScopusUtility.download_snapshot(api_keys, release, 'parallel')
            self.assertEqual(mock_make_query.call_count, 10)
            self.assertEqual(len(saved_files), 5)


class TestScopusJsonParser(unittest.TestCase):
    """ Test parsing facilities. """

    def __init__(self, *args, **kwargs):
        super(TestScopusJsonParser, self).__init__(*args, **kwargs)
        self.scopus_inst_id = ["60031226"]  # Curtin University

        self.data = {
            "dc:identifier": "scopusid",
            "eid": "testid",
            "dc:title": "arttitle",
            "prism:aggregationType": "source",
            "subtypeDescription": "typedesc",
            "citedby-count": "345",
            "prism:publicationName": "pubname",
            "prism:isbn": "isbn",
            "prism:issn": "issn",
            "prism:eIssn": "eissn",
            "prism:coverDate": "2010-12-01",
            "prism:doi": "doi",
            "pii": "pii",
            "pubmed-id": "med",
            "orcid": "orcid",
            "dc:creator": "firstauth",
            "source-id": "1000",
            "openaccess": "1",
            "openaccessFlag": False,
            "affiliation": [
                {
                    "affilname": "aname",
                    "affiliation-city": "acity",
                    "affiliation-country": "country",
                    "afid": "id",
                    "name-variant": "variant",
                }
            ],
            "author": [
                {
                    "authid": "id",
                    "orcid": "id",
                    "authname": "name",
                    "given-name": "first",
                    "surname": "last",
                    "initials": "mj",
                    "afid": "id"
                }
            ],
            "dc:description": "abstract",
            "authkeywords": ["words"],
            "article-number": "artno",
            "fund-acr": "acr",
            "fund-no": "no",
            "fund-sponsor": "sponsor"
        }

    def test_get_affiliations(self):
        """ Test get affiliations """

        affil = ScopusJsonParser.get_affiliations({})
        self.assertEqual(len(affil), 0)

        affil = ScopusJsonParser.get_affiliations(self.data)
        self.assertEqual(len(affil), 1)
        af = affil[0]
        self.assertEqual(af['name'], 'aname')
        self.assertEqual(af['city'], 'acity')
        self.assertEqual(af['country'], 'country')
        self.assertEqual(af['id'], 'id')
        self.assertEqual(af['name_variant'], 'variant')

    def test_get_authors(self):
        """ Test get authors """

        author = ScopusJsonParser.get_authors({})
        self.assertEqual(len(author), 0)

        author = ScopusJsonParser.get_authors(self.data)
        self.assertEqual(len(author), 1)
        au = author[0]
        self.assertEqual(au['authid'], "id")
        self.assertEqual(au['orcid'], "id")
        self.assertEqual(au['full_name'], "name")
        self.assertEqual(au['first_name'], "first")
        self.assertEqual(au['last_name'], "last")
        self.assertEqual(au['initials'], "mj")
        self.assertEqual(au['afid'], "id")

    def test_parse_json(self):
        """ Test the parser. """

        harvest_datetime = pendulum.now('UTC').isoformat()
        release_date = "2018-01-01"
        entry = ScopusJsonParser.parse_json(self.data, harvest_datetime, release_date, self.scopus_inst_id)
        self.assertEqual(entry['harvest_datetime'], harvest_datetime)
        self.assertEqual(entry['release_date'], release_date)
        self.assertEqual(entry['title'], 'arttitle')
        self.assertEqual(entry['identifier'], 'scopusid')
        self.assertEqual(entry['creator'], 'firstauth')
        self.assertEqual(entry['publication_name'], 'pubname')
        self.assertEqual(entry['cover_date'], '2010-12-01')
        self.assertEqual(entry['doi'], 'doi')
        self.assertEqual(entry['eissn'], 'eissn')
        self.assertEqual(entry['issn'], 'issn')
        self.assertEqual(entry['isbn'], 'isbn')
        self.assertEqual(entry['aggregation_type'], 'source')
        self.assertEqual(entry['pubmed_id'], 'med')
        self.assertEqual(entry['pii'], 'pii')
        self.assertEqual(entry['eid'], 'testid')
        self.assertEqual(entry['subtype_description'], 'typedesc')
        self.assertEqual(entry['open_access'], 1)
        self.assertEqual(entry['open_access_flag'], False)
        self.assertEqual(entry['citedby_count'], 345)
        self.assertEqual(entry['source_id'], 1000)
        self.assertEqual(entry['orcid'], 'orcid')

        self.assertEqual(len(entry['affiliations']), 1)
        af = entry['affiliations'][0]
        self.assertEqual(af['name'], 'aname')
        self.assertEqual(af['city'], 'acity')
        self.assertEqual(af['country'], 'country')
        self.assertEqual(af['id'], 'id')
        self.assertEqual(af['name_variant'], 'variant')

        self.assertEqual(entry['abstract'], 'abstract')
        self.assertEqual(entry['article_number'], 'artno')
        self.assertEqual(entry['fund_agency_ac'], 'acr')
        self.assertEqual(entry['fund_agency_id'], 'no')
        self.assertEqual(entry['fund_agency_name'], 'sponsor')

        words = entry['keywords']
        self.assertEqual(len(words), 1)
        self.assertEqual(words[0], "words")

        authors = entry['authors']
        self.assertEqual(len(authors), 1)
        au = authors[0]
        self.assertEqual(au['authid'], "id")
        self.assertEqual(au['orcid'], "id")
        self.assertEqual(au['full_name'], "name")
        self.assertEqual(au['first_name'], "first")
        self.assertEqual(au['last_name'], "last")
        self.assertEqual(au['initials'], "mj")
        self.assertEqual(au['afid'], "id")

        self.assertEqual(len(entry['institution_ids']), 1)
        self.assertEqual(entry['institution_ids'], self.scopus_inst_id)
