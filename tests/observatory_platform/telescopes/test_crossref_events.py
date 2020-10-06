import json
import logging
import os
import pathlib
import shutil
import unittest
from datetime import datetime
from unittest.mock import patch

import pendulum
import vcr
from airflow.exceptions import AirflowException
from click.testing import CliRunner

from observatory_platform.telescopes.crossref_events import (CrossrefEventsRelease,
                                                             CrossrefEventsTelescope,
                                                             change_keys,
                                                             convert,
                                                             download_events_batch,
                                                             download_release,
                                                             extract_events,
                                                             transform_release)
from observatory_platform.utils.config_utils import SubFolder, telescope_path
from observatory_platform.utils.data_utils import _hash_file
from tests.observatory_platform.config import test_fixtures_path


class TestCrossrefEvents(unittest.TestCase):
    """ Tests for the functions used by the crossref events telescope """

    def __init__(self, *args, **kwargs):
        """ Constructor which sets up variables used by tests.

        :param args: arguments.
        :param kwargs: keyword arguments.
        """

        super(TestCrossrefEvents, self).__init__(*args, **kwargs)

        self.prev_start_date = pendulum.parse('2020-01-02 11:48:23.795099+00:00')
        self.start_date = pendulum.instance(datetime.strptime('2020-01-03 03:16:49.041842+00:00', '%Y-%m-%d '
                                                                                                  '%H:%M:%S.%f%z'))

        # Turn logging to warning because vcr prints too much at info level
        logging.basicConfig()

    def test_extract_events(self):
        """
        Tests whether events are extracted appropriately from a temporary url which should return only 42 events.
        The number of events per row is set to 30, so it tests using this function recursively and returning a cursor.
        """
        tmp_url = 'https://api.eventdata.crossref.org/v1/events?mailto=aniek.roelofs@curtin.edu.au&source=reddit&from' \
                  '-collected-date=2020-01-02&until-collected-date=2020-01-02&rows=30'
        events_response_path = os.path.join(test_fixtures_path(), 'vcr_cassettes', 'crossref_events.yaml')
        events_response_hash = '22f0e10f7b09939ee2acf44d790d4f11'
        events_file_hash = '81b6c93643aa8f1283c473a567a6dc85'

        with CliRunner().isolated_filesystem():
            events_path = 'events.json'
            with vcr.use_cassette(events_response_path):
                success, next_cursor, total_events = extract_events(tmp_url, events_path)
                self.assertTrue(success)
                self.assertEqual(next_cursor, None)
                self.assertEqual(total_events, 42)

                # check file hash of response
                self.assertEqual(events_response_hash, _hash_file(events_response_path, algorithm='md5'))

                # check file hash of corresponding events
                self.assertTrue(os.path.isfile(events_path))
                self.assertEqual(events_file_hash, _hash_file(events_path, algorithm='md5'))

    def test_batch_dates(self):
        """
        Tests the lists of batch dates that result from splitting up a release period in batches of multiple periods.
        This is tested for a few different batch sizes.
        """
        with patch.object(CrossrefEventsTelescope, 'MAX_PROCESSES', 4):
            # 1 day only
            start_date = pendulum.parse('2020-01-02 11:48:23.795099+00:00')
            end_date = pendulum.parse('2020-01-02 23:48:23.795099+00:00')
            release = CrossrefEventsRelease(start_date, end_date)
            expected_batch_dates = [('2020-01-02', '2020-01-02')]
            self.assertEqual(release.batch_dates, expected_batch_dates)

            # exactly 2 days per batch
            start_date = pendulum.parse('2020-01-02 11:48:23.795099+00:00')
            end_date = pendulum.parse('2020-01-09 23:48:23.795099+00:00')
            release = CrossrefEventsRelease(start_date, end_date)
            expected_batch_dates = [('2020-01-02', '2020-01-03'), ('2020-01-04', '2020-01-05'),
                                    ('2020-01-06', '2020-01-07'), ('2020-01-08', '2020-01-09')]
            self.assertEqual(release.batch_dates, expected_batch_dates)

            # 1.5 day per batch
            start_date = pendulum.parse('2020-01-02 11:48:23.795099+00:00')
            end_date = pendulum.parse('2020-01-07 23:48:23.795099+00:00')
            release = CrossrefEventsRelease(start_date, end_date)
            expected_batch_dates = [('2020-01-02', '2020-01-03'), ('2020-01-04', '2020-01-05'),
                                    ('2020-01-06', '2020-01-07')]
            self.assertEqual(release.batch_dates, expected_batch_dates)

            # 2.25 day per batch
            start_date = pendulum.parse('2020-01-02 11:48:23.795099+00:00')
            end_date = pendulum.parse('2020-01-10 23:48:23.795099+00:00')
            release = CrossrefEventsRelease(start_date, end_date)
            expected_batch_dates = [('2020-01-02', '2020-01-03'), ('2020-01-04', '2020-01-05'),
                                    ('2020-01-06', '2020-01-07'), ('2020-01-08', '2020-01-10')]
            self.assertEqual(release.batch_dates, expected_batch_dates)

    def test_change_keys(self):
        """
        Test changing the keys of a nested dictionary, replacing '-' with '_'.
        """
        old_dict = {
            'key-1': {
                'key-2': 'value-2'
            },
            'key_3': {
                'key-4': 'value-4'
            },
            'key_5': 'value-5',
            'key_6': {
                'key_7': {
                    'key-8': 'value-8'
                }
            }
        }
        actual_new_dict = change_keys(old_dict, convert)
        expected_new_dict = {
            'key_1': {
                'key_2': 'value-2'
            },
            'key_3': {
                'key_4': 'value-4'
            },
            'key_5': 'value-5',
            'key_6': {
                'key_7': {
                    'key_8': 'value-8'
                }
            }
        }
        self.assertEqual(actual_new_dict, expected_new_dict)

    @patch('observatory_platform.utils.config_utils.airflow.models.Variable.get')
    def test_crossref_events_release(self, mock_variable_get):
        """
        Test initiating a CrossrefEventsRelease class, both for sequential/parallel download mode and
        first_release/later release.
        :param mock_variable_get: MagicMock instance of airflow's Variable.get() function
        """
        with CliRunner().isolated_filesystem():
            # set telescope data path variable
            mock_variable_get.return_value = 'data'

            with patch.object(CrossrefEventsTelescope, 'DOWNLOAD_MODE', 'sequential'):
                release = CrossrefEventsRelease(self.prev_start_date, self.start_date)
                # check number of batches / urls is 1
                self.assertEqual(len(release.urls), 1)
                # check that first batch also contains urls for edited/deleted events
                self.assertEqual(len(release.urls[0]), 3)

                release = CrossrefEventsRelease(self.prev_start_date, self.start_date, True)
                # check number of batches / urls is 1
                self.assertEqual(len(release.urls), 1)
                # check that first batch does not contain urls for edited/deleted events
                self.assertEqual(len(release.urls[0]), 1)

            with patch.object(CrossrefEventsTelescope, 'DOWNLOAD_MODE', 'parallel'):
                release = CrossrefEventsRelease(self.prev_start_date, self.start_date)
                # check number of batches / urls is 2
                self.assertEqual(len(release.urls), 2)
                # check that first batch also contains urls for edited/deleted events
                self.assertEqual(len(release.urls[0]), 3)

                release = CrossrefEventsRelease(self.prev_start_date, self.start_date, True)
                # check number of batches / urls is 2
                self.assertEqual(len(release.urls), 2)
                # check that first batch does not contain urls for edited/deleted events
                self.assertEqual(len(release.urls[0]), 1)

            with patch.object(CrossrefEventsTelescope, 'DOWNLOAD_MODE', 'error'):
                # check that invalid download mode raises exception
                with self.assertRaises(AirflowException):
                    CrossrefEventsRelease(self.prev_start_date, self.start_date)

    @patch('observatory_platform.utils.config_utils.airflow.models.Variable.get')
    def test_subdir(self, mock_variable_get):
        """
        Test the path to a subdirectory to store download/transformed events files.
        :param mock_variable_get: MagicMock instance of airflow's Variable.get() function
        """
        with CliRunner().isolated_filesystem():
            # set telescope data path variable
            mock_variable_get.return_value = 'data'
            release = CrossrefEventsRelease(self.prev_start_date, self.start_date)

            # download subdir
            actual_dir = release.subdir(SubFolder.downloaded)
            date_str = self.prev_start_date.strftime("%Y_%m_%d") + "-" + self.start_date.strftime("%Y_%m_%d")
            expected_dir = os.path.join(telescope_path(SubFolder.downloaded, CrossrefEventsTelescope.DAG_ID), date_str)
            self.assertEqual(actual_dir, expected_dir)

            # transform subdir
            actual_dir = release.subdir(SubFolder.transformed)
            date_str = self.prev_start_date.strftime("%Y_%m_%d") + "-" + self.start_date.strftime("%Y_%m_%d")
            expected_dir = os.path.join(telescope_path(SubFolder.transformed, CrossrefEventsTelescope.DAG_ID), date_str)
            self.assertEqual(actual_dir, expected_dir)

    @patch('observatory_platform.utils.config_utils.airflow.models.Variable.get')
    def test_download_path(self, mock_variable_get):
        """
        Test the file path for downloaded events and check if the parent directory exists.
        :param mock_variable_get: MagicMock instance of airflow's Variable.get() function
        """
        with CliRunner().isolated_filesystem():
            # set telescope data path variable
            mock_variable_get.return_value = 'data'
            release = CrossrefEventsRelease(self.prev_start_date, self.start_date)

            file_name = 'crossref_events'

            # get download path
            actual_path = release.get_path(SubFolder.downloaded, file_name)
            sub_dir = release.subdir(SubFolder.downloaded)
            expected_path = os.path.join(sub_dir, f"{file_name}.json")
            self.assertEqual(actual_path, expected_path)
            # check that subdir exists
            self.assertTrue(os.path.exists(os.path.dirname(actual_path)))

    @patch('observatory_platform.utils.config_utils.airflow.models.Variable.get')
    def test_transform_path(self, mock_variable_get):
        """
        Test the file path for downloaded events and check if the parent directory exists.
        :param mock_variable_get: MagicMock instance of airflow's Variable.get() function
        """
        with CliRunner().isolated_filesystem():
            # set telescope data path variable
            mock_variable_get.return_value = 'data'
            release = CrossrefEventsRelease(self.prev_start_date, self.start_date)

            file_name = 'crossref_events'

            # get download path
            actual_path = release.get_path(SubFolder.transformed, file_name)
            sub_dir = release.subdir(SubFolder.transformed)
            expected_path = os.path.join(sub_dir, f"{file_name}.json")
            self.assertEqual(actual_path, expected_path)
            # check that subdir exists
            self.assertTrue(os.path.exists(os.path.dirname(actual_path)))

    @patch('observatory_platform.utils.config_utils.airflow.models.Variable.get')
    def test_batch_path(self, mock_variable_get):
        """
        Test the file path for cursor and events file of a single batch for all 3 urls (events/edited/deleted).
        :param mock_variable_get: MagicMock instance of airflow's Variable.get() function
        """
        release = CrossrefEventsRelease(self.prev_start_date, self.start_date)
        for j, url in enumerate(release.urls[0]):
            # test extracted events path
            self.batch_path_test(mock_variable_get, True, j)
            # test cursor path
            self.batch_path_test(mock_variable_get, False, j)

    def batch_path_test(self, mock_variable_get: patch, cursor: bool, j: int):
        """
        Test the file path of a single batch for one of the urls.
        :param mock_variable_get: MagicMock instance of airflow's Variable.get() function
        :param cursor: Whether cursor or events file
        :param j: Determines which of the 3 urls (events/edited/deleted)
        """
        with CliRunner().isolated_filesystem():
            # set telescope data path variable
            mock_variable_get.return_value = 'data'
            release = CrossrefEventsRelease(self.prev_start_date, self.start_date)

            sub_dir = release.subdir(SubFolder.downloaded)
            i = 0
            # get start and end date of first batch
            batch_start_date = release.batch_dates[i][0]

            event_type = release.urls[i][j].split('?mailto')[0].split('/')[-1]
            if event_type == 'events':
                batch_end_date = release.batch_dates[i][1]
            else:
                batch_end_date = release.end_date.strftime("%Y-%m-%d")
            if cursor:
                # get path for events/edited/deleted of first batch
                actual_path = release.batch_path(release.urls[i][j], cursor)
                expected_path = os.path.join(sub_dir, f"{event_type}_{batch_start_date}_{batch_end_date}_cursor.json")
            else:
                # get path for events/edited/deleted of first batch
                actual_path = release.batch_path(release.urls[i][j])
                expected_path = os.path.join(sub_dir, f"{event_type}_{batch_start_date}_{batch_end_date}.json")
            self.assertEqual(actual_path, expected_path)
            self.assertTrue(os.path.exists(os.path.dirname(actual_path)))

    @patch('observatory_platform.utils.config_utils.airflow.models.Variable.get')
    def test_blob_name(self, mock_variable_get):
        """
        Test the blob name used to determine path in Google Cloud Storage
        :param mock_variable_get: MagicMock instance of airflow's Variable.get() function
        """
        with CliRunner().isolated_filesystem():
            # set telescope data path variable
            mock_variable_get.return_value = 'data'
            release = CrossrefEventsRelease(self.prev_start_date, self.start_date)

            date_str = self.prev_start_date.strftime("%Y_%m_%d") + "-" + self.start_date.strftime("%Y_%m_%d")
            file_name = f"{CrossrefEventsTelescope.DAG_ID}_{date_str}.json"

            # get blob name
            actual_path = release.blob_name
            expected_path = f'telescopes/{CrossrefEventsTelescope.DAG_ID}/{file_name}'
            self.assertEqual(actual_path, expected_path)

    @patch('observatory_platform.telescopes.crossref_events.extract_events')
    @patch('observatory_platform.utils.config_utils.airflow.models.Variable.get')
    @patch('observatory_platform.telescopes.crossref_events.CrossrefEventsTelescope.DOWNLOAD_MODE', 'sequential')
    def test_download_events_batch(self, mock_variable_get, mock_extract_events):
        """
        Test downloading one complete batch in different conditions. Mocking when an error has occurred (cursor file
        is present), mocking when all events were successfully downloaded (no cursor file, events file is present) and
        mocking a fresh start (no cursor file, no events file present)
        :param mock_variable_get: MagicMock instance of airflow's Variable.get() function
        :param mock_extract_events:
        """
        with CliRunner().isolated_filesystem():
            # set telescope data path variable
            mock_variable_get.return_value = 'data'
            release = CrossrefEventsRelease(self.prev_start_date, self.start_date)

            i = 0
            # with cursor file -> success is False
            # create cursor file, mock failed request
            mock_extract_events.return_value = (False, 'test_cursor', None)
            batch_results = download_events_batch(release, i)
            events_paths = []
            for url in release.urls[i]:
                events_paths.append(release.batch_path(url))
                cursor_path = release.batch_path(url, cursor=True)
                self.assertTrue(os.path.isfile(cursor_path))
                # delete cursor file
                pathlib.Path(cursor_path).unlink()
            for count, result in enumerate(batch_results):
                self.assertEqual(result[0], events_paths[count])
                # check that success is false
                self.assertFalse(result[1])

            # with existing events, without cursor file -> success is True
            # create event files
            for event_path in events_paths:
                with open(event_path, 'w') as f:
                    f.write('')
            batch_results = download_events_batch(release, i)
            for count, result in enumerate(batch_results):
                self.assertEqual(result[0], events_paths[count])
                # check that success is true
                self.assertTrue(result[1])
                # delete event file
                pathlib.Path(events_paths[count]).unlink()

            # without existing events, without cursor file -> success is True
            mock_extract_events.return_value = (True, None, 42)
            batch_results = download_events_batch(release, i)
            for count, result in enumerate(batch_results):
                self.assertEqual(result[0], events_paths[count])
                # check that success is true
                self.assertTrue(result[1])

    @patch('observatory_platform.telescopes.crossref_events.download_events_batch')
    @patch('observatory_platform.utils.config_utils.airflow.models.Variable.get')
    def test_download_release(self, mock_variable_get, mock_download_events_batch):
        """
        Test downloading a complete release in different conditions. Testing while using different download modes and
        testing for both first time downloading a release and later.
        :param mock_variable_get: MagicMock instance of airflow's Variable.get() function
        :param mock_download_events_batch:
        """
        # use sequential download mode
        with patch.object(CrossrefEventsTelescope, 'DOWNLOAD_MODE', 'sequential'):
            # test for both first_release False and True
            self.download_release_tests(mock_download_events_batch, mock_variable_get, False)
            self.download_release_tests(mock_download_events_batch, mock_variable_get, True)

        # use parallel download mode
        with patch.object(CrossrefEventsTelescope, 'DOWNLOAD_MODE', 'parallel'):
            # test for both first_release False and True
            self.download_release_tests(mock_download_events_batch, mock_variable_get, False)
            self.download_release_tests(mock_download_events_batch, mock_variable_get, True)

    def download_release_tests(self, mock_download_events_batch: patch, mock_variable_get: patch, first_release: bool):
        """
        Test downloading a single release.
        :param mock_download_events_batch:
        :param mock_variable_get: MagicMock instance of airflow's Variable.get() function
        :param first_release:
        """
        with CliRunner().isolated_filesystem():
            # set telescope data path variable
            mock_variable_get.return_value = 'data'
            release = CrossrefEventsRelease(self.prev_start_date, self.start_date, first_release)

            i = 0
            # collect batch results
            download_events_batch_results = []
            for url in release.urls[i]:
                event_path = release.batch_path(url)
                download_events_batch_results.append((event_path, True))
                # create empty event files
                with open(event_path, 'a') as f:
                    f.write(json.dumps([]) + '\n')

            # all batches success, empty event files -> don't continue DAG
            mock_download_events_batch.return_value = download_events_batch_results
            continue_dag = download_release(release)
            self.assertFalse(continue_dag)

            # all batches success, non-empty event files -> continue DAG
            for result in download_events_batch_results:
                event_path = result[0]
                with open(event_path, 'w') as f:
                    json.dump([{
                                   'test': 'test'
                               }], f)
            continue_dag = download_release(release)
            self.assertTrue(continue_dag)

            # one failed batch -> raise airflow exception
            download_events_batch_results = []
            failed = True
            for url in release.urls[i]:
                event_path = release.batch_path(url)
                if failed:
                    download_events_batch_results.append((event_path, False))
                    failed = False
                else:
                    download_events_batch_results.append((event_path, True))
            mock_download_events_batch.return_value = download_events_batch_results
            with self.assertRaises(AirflowException):
                download_release(release)

    @patch('observatory_platform.utils.config_utils.airflow.models.Variable.get')
    def test_transform_release(self, mock_variable_get):
        """
        Test transforming a release, using a test download file it checks whether the transformed file is as expected.
        :param mock_variable_get: MagicMock instance of airflow's Variable.get() function
        """
        download_release_path = os.path.join(test_fixtures_path(), 'telescopes', 'crossref_events.json')
        transform_release_hash = '898c605bb10840d5b5c1f097e32df457'

        with CliRunner().isolated_filesystem():
            # set telescope data path variable
            mock_variable_get.return_value = 'data'
            release = CrossrefEventsRelease(self.prev_start_date, self.start_date)

            # copy test release file to download path
            shutil.copy(download_release_path, release.download_path)

            # transform release
            transform_release(release)

            self.assertTrue(os.path.isfile(release.transform_path))
            self.assertEqual(transform_release_hash, _hash_file(release.transform_path, algorithm='md5'))
