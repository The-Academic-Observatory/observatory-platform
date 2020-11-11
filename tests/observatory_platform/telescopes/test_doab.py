import pathlib
import logging
import os
import shutil
import unittest
from collections import OrderedDict
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import patch

import vcr
import pendulum
from airflow.exceptions import AirflowException
from click.testing import CliRunner


from observatory_platform.telescopes.doab import (
    DoabRelease,
    DoabTelescope,
    create_release,
    change_keys,
    convert,
    download,
    download_oai_pmh,
    download_csv,
    extract_entries,
    transform)

from observatory_platform.utils.data_utils import _hash_file
from tests.observatory_platform.config import test_fixtures_path
import observatory_platform.telescopes.doab


class TestDoab(unittest.TestCase):
    """ Tests for the functions used by the orcid telescope """

    def __init__(self, *args, **kwargs):
        """ Constructor which sets up variables used by tests.
        :param args: arguments.
        :param kwargs: keyword arguments.
        """

        super(TestDoab, self).__init__(*args, **kwargs)

        self.start_date = pendulum.parse('2020-07-25 11:48:23.795099+00:00')
        self.end_date = pendulum.instance(datetime.strptime('2020-08-17 00:00:05.041842+00:00', '%Y-%m-%d %H:%M:%S.%f%z'))

        logging.basicConfig()

    def test_create_release(self):
        """ Test creating a release """
        with CliRunner().isolated_filesystem():
            # first release
            release = create_release(self.start_date, self.end_date, DoabTelescope.telescope, True)
            self.assertIsInstance(release, DoabRelease)

            # later release
            release = create_release(self.start_date, self.end_date, DoabTelescope.telescope, False)
            self.assertIsInstance(release, DoabRelease)

    @patch('observatory_platform.telescopes.doab.extract_entries')
    @patch('observatory_platform.utils.config_utils.AirflowVariable.get')
    def test_download_oai_pmh(self, mock_variable_get, mock_extract_entries):
        """
        Test outputs of downloading oai-pmh data
        """
        with CliRunner().isolated_filesystem():
            mock_variable_get.return_value = 'data'
            # first release
            release = DoabRelease(self.start_date, self.end_date, DoabTelescope.telescope)

            # with token file -> success is False
            # create token file, mock failed request
            mock_extract_entries.return_value = (False, 'test_cursor', None)
            with self.assertRaises(AirflowException):
                success = download_oai_pmh(release)
            # check that token file is created
            self.assertTrue(os.path.isfile(release.token_path))
            # delete cursor file
            pathlib.Path(release.token_path).unlink()

            # with existing download file, without cursor file -> success is True
            # create download file
            with open(release.oai_pmh_path, 'w') as f:
                f.write('')
            success = download_oai_pmh(release)
            # check that success is true
            self.assertTrue(success)
            # delete download file
            pathlib.Path(release.oai_pmh_path).unlink()

            # without existing downloads, without cursor file -> success is True
            mock_extract_entries.return_value = (True, None, '42')
            success = download_oai_pmh(release)
            # check that success is true
            self.assertTrue(success)

            mock_extract_entries.return_value = (True, None, '0')
            success = download_oai_pmh(release)
            # check that success is true
            self.assertFalse(success)

    @patch('requests.Session.get')
    @patch('observatory_platform.utils.config_utils.AirflowVariable.get')
    def test_download_csv(self, mock_variable_get, mock_requests_get):
        """ Test outputs of downloading the csv data """
        download_csv_hash = '3ac02641cf41c7283e5aef8a851e0ae8'
        with CliRunner().isolated_filesystem():
            mock_variable_get.return_value = 'data'

            release = DoabRelease(self.start_date, self.end_date, DoabTelescope.telescope)
            mock_requests_get.return_value = SimpleNamespace(status_code=200, content=b'csv_file')
            success = download_csv(release)
            self.assertTrue(success)
            self.assertEqual(download_csv_hash, _hash_file(release.csv_path, algorithm='md5'))

            mock_requests_get.return_value = SimpleNamespace(status_code=404, text='error')
            with self.assertRaises(AirflowException):
                success = download_csv(release)

    def test_extract_entries(self):
        """ Test extracting oai-pmh entries from a url using a cassette """
        tmp_url = 'https://www.doabooks.org/oai?verb=ListRecords&metadataPrefix=oai_dc&from=2020-09-01&until=2020-09-03'
        entries_response_path = os.path.join(test_fixtures_path(), 'vcr_cassettes', 'doab_entries.yaml')
        entries_response_hash = '6bdd8bfcd5f52824f39ff8a53fd40ee8'
        entries_file_hash = '9ecd838180fe877a10b74d8532dbd9bc'

        with CliRunner().isolated_filesystem():
            entries_path = 'events.json'

            with vcr.use_cassette(entries_response_path):
                success, next_token, total_entries = extract_entries(tmp_url, entries_path)
                self.assertTrue(success)
                self.assertEqual(None, next_token)
                self.assertEqual('152', total_entries)

                # check file hash of response
                self.assertEqual(entries_response_hash, _hash_file(entries_response_path, algorithm='md5'))

                # check file hash of corresponding entries
                self.assertTrue(os.path.isfile(entries_path))
                self.assertEqual(entries_file_hash, _hash_file(entries_path, algorithm='md5'))

    @patch('observatory_platform.telescopes.doab.download_csv')
    @patch('observatory_platform.telescopes.doab.download_oai_pmh')
    @patch('observatory_platform.utils.config_utils.AirflowVariable.get')
    def test_download(self, mock_variable_get, mock_download_oai_pmh, mock_download_csv):
        """ Test output of downloading both oai-pmh and csv data """
        with CliRunner().isolated_filesystem():
            mock_variable_get.return_value = 'data'
            release = DoabRelease(self.start_date, self.end_date, DoabTelescope.telescope)

            mock_download_oai_pmh.return_value = True
            mock_download_csv.return_value = True
            success = download(release)
            self.assertTrue(success)

            mock_download_oai_pmh.return_value = False
            mock_download_csv.return_value = True
            success = download(release)
            self.assertFalse(success)

            mock_download_oai_pmh.return_value = True
            mock_download_csv.return_value = False
            success = download(release)
            self.assertFalse(success)

            mock_download_oai_pmh.return_value = False
            mock_download_csv.return_value = False
            success = download(release)
            self.assertFalse(success)

    @patch('observatory_platform.utils.config_utils.AirflowVariable.get')
    def test_transform(self, mock_variable_get):
        """ Test resulting file from transform function """
        transform_hash = '21e69fecd8d7781c54933217b020b207'
        with CliRunner().isolated_filesystem():
            mock_variable_get.return_value = 'data'

            release = DoabRelease(self.start_date, self.end_date, DoabTelescope.telescope)
            oai_pmh_path = os.path.join(test_fixtures_path(), 'telescopes', 'doab_oai_pmh.json')
            csv_path = os.path.join(test_fixtures_path(), 'telescopes', 'doab_csv.csv')
            shutil.copyfile(oai_pmh_path, release.oai_pmh_path)
            shutil.copyfile(csv_path, release.csv_path)

            transform(release)
            self.assertTrue(os.path.isfile(release.transform_path))
            self.assertEqual(transform_hash, _hash_file(release.transform_path, algorithm='md5'))

    def test_convert(self):
        """ Test string convert function """
        string = convert('dc:test-identifier url')
        self.assertEqual('test_identifier_url', string)

        string = convert('@xmlns:xsi')
        self.assertEqual('xsi', string)

    def test_change_keys(self):
        """ Test changing keys of downloaded results """
        csv_entries = {'9783830941798': {'ISSN': '', 'Volume': '', 'Pages': '120', 'Series title': '',
                                         'Added on date': '2020-04-28 16:16:05', 'Subjects': 'LB5-3640; L7-991'}}
        observatory_platform.telescopes.doab.csv_entries = csv_entries
        record_dict = [OrderedDict([('header',
                                    OrderedDict([('identifier', 'oai:doab-books:45024'),
                                                 ('datestamp', '2020-09-02T09:51:56Z'),
                                                 ('setSpec', ['Social_Sciences', 'publisher_1487'])])),
                                   ('metadata',
                                    OrderedDict([('oai_dc:dc',
                                                  OrderedDict([('@xmlns:dc', 'http://purl.org/dc/elements/1.1/'),
                                                               ('@xmlns:oai_dc', 'http://www.openarchives.org/OAI/2.0/oai_dc/'),
                                                               ('@xmlns:xsi', 'http://www.w3.org/2001/XMLSchema-instance'),
                                                               ('@xsi:schemaLocation', 'http://www.openarchives.org/OAI/2.0/oai_dc/ http://www.openarchives.org/OAI/2.0/oai_dc.xsd'),
                                                               ('dc:title', 'Hybrid environments for universities'),
                                                               ('dc:identifier', ['https://www.waxmann.com/buch4179', 'https://www.doabooks.org/doab?func=search&query=rid:45024', 'ISBN: 9783830941798', 'DOI: https://doi.org/10.31244/9783830991793']),
                                                               ('dc:creator', ['Nestler, Jonathan', 'Nenonen, Suvi', 'Loidl-Reisch, Cordula', 'Gothe, Kerstin', 'den Heijer, Alexandra', 'Liedtke, Bettina', 'Ninnemann, Katja', 'Tieva, Åse', 'Wallenborg, Christian']),
                                                               ('dc:language', 'English'),
                                                               ('dc:description', 'Long description'),
                                                               ('dc:subject', ['University', 'architecture', 'Educational Technology', 'Urban planning', 'Campus', 'Hochschule', 'Hochschulplanung', 'Stadtplanung', 'Universität', 'Entwicklung']),
                                                               ('dc:publisher', 'Waxmann Verlag'),
                                                               ('dc:date', '2020'),
                                                               ('dc:type', 'book'),
                                                               ('dc:rights',
                                                                'https://creativecommons.org/licenses/by-nc-nd/4.0/legalcode')]))]))])]

        updated_dict = change_keys(record_dict, convert)
        expected_dict = [OrderedDict([('header', OrderedDict([('identifier', 'oai:doab-books:45024'), ('datestamp', '2020-09-02T09:51:56Z'), ('setspec', ['Social_Sciences', 'publisher_1487'])])), ('metadata', OrderedDict([('title', 'Hybrid environments for universities'), ('identifier', ['https://www.waxmann.com/buch4179', 'https://www.doabooks.org/doab?func=search&query=rid:45024', 'ISBN: 9783830941798', 'DOI: https://doi.org/10.31244/9783830991793']), ('creator', ['Nestler, Jonathan', 'Nenonen, Suvi', 'Loidl-Reisch, Cordula', 'Gothe, Kerstin', 'den Heijer, Alexandra', 'Liedtke, Bettina', 'Ninnemann, Katja', 'Tieva, Åse', 'Wallenborg, Christian']), ('language', ['English']), ('description', 'Long description'), ('subject', ['University', 'architecture', 'Educational Technology', 'Urban planning', 'Campus', 'Hochschule', 'Hochschulplanung', 'Stadtplanung', 'Universität', 'Entwicklung']), ('publisher', 'Waxmann Verlag'), ('date', '2020'), ('type', 'book'), ('rights', 'https://creativecommons.org/licenses/by-nc-nd/4.0/legalcode')])), ('csv', {'issn': '', 'volume': '', 'pages': '120', 'series_title': '', 'added_on_date': '2020-04-28 16:16:05', 'subjects': ['LB5-3640', 'L7-991']})])]
        self.assertEqual(expected_dict, updated_dict)
