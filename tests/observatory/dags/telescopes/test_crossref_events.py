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

import os
from datetime import timedelta
from unittest.mock import patch

import pendulum
import vcr
from airflow.exceptions import AirflowException
from click.testing import CliRunner
from observatory.dags.telescopes.crossref_events import CrossrefEventsRelease, CrossrefEventsTelescope, download_events
from observatory.platform.utils.template_utils import blob_name, table_ids_from_path
from observatory.platform.utils.test_utils import ObservatoryEnvironment, ObservatoryTestCase, module_file_path, \
    test_fixtures_path
from requests.exceptions import RetryError


class TestCrossrefEvents(ObservatoryTestCase):
    """ Tests for the Crossref Events telescope """

    def __init__(self, *args, **kwargs):
        """ Constructor which sets up variables used by tests.
        :param args: arguments.
        :param kwargs: keyword arguments.
        """
        super(TestCrossrefEvents, self).__init__(*args, **kwargs)
        self.project_id = os.getenv('TEST_GCP_PROJECT_ID')
        self.data_location = os.getenv('TEST_GCP_DATA_LOCATION')

        self.first_execution_date = pendulum.datetime(year=2018, month=5, day=16)
        self.first_cassette = test_fixtures_path('vcr_cassettes', 'crossref_events', 'crossref_events1.csv')
        self.first_download_hash = 'c8e09147d7e1b2247093f950371fd33d'
        self.first_transform_hash = 'df579b88'

        self.second_execution_date = pendulum.datetime(year=2018, month=6, day=1)
        self.second_cassette = test_fixtures_path('vcr_cassettes', 'crossref_events', 'crossref_events2.csv')
        self.second_download_hash = 'a981bd130a2bbd10322b7d5b1a87d578'
        self.second_transform_hash = '536c220b'

        # additional tests setup
        self.start_date = pendulum.Pendulum(2020, 1, 1)
        self.end_date = pendulum.Pendulum(2020, 2, 1)
        self.release = CrossrefEventsRelease(CrossrefEventsTelescope.DAG_ID, self.start_date, self.end_date,
                                             False, 'mailto', 'parallel', 21)

    def test_dag_structure(self):
        """ Test that the Crossref Events DAG has the correct structure.
        :return: None
        """

        dag = CrossrefEventsTelescope().make_dag()
        self.assert_dag_structure({
            'check_dependencies': ['get_release_info'],
            'get_release_info': ['download'],
            'download': ['upload_downloaded'],
            'upload_downloaded': ['transform'],
            'transform': ['upload_transformed'],
            'upload_transformed': ['bq_load_partition'],
            'bq_load_partition': ['bq_delete_old'],
            'bq_delete_old': ['bq_append_new'],
            'bq_append_new': ['cleanup'],
            'cleanup': []
        }, dag)

    def test_dag_load(self):
        """ Test that the Crossref Events DAG can be loaded from a DAG bag.
        :return: None
        """

        with ObservatoryEnvironment().create():
            dag_file = os.path.join(module_file_path('observatory.dags.dags'), 'crossref_events.py')
            self.assert_dag_load('crossref_events', dag_file)

    def test_telescope(self):
        """ Test the Crossref Events telescope end to end.
        :return: None.
        """
        # Setup Observatory environment
        env = ObservatoryEnvironment(self.project_id, self.data_location)
        dataset_id = env.add_dataset()

        # Setup Telescope
        telescope = CrossrefEventsTelescope(dataset_id=dataset_id)
        telescope.download_mode = 'sequential'
        dag = telescope.make_dag()

        # Create the Observatory environment and run tests
        with env.create():
            # first run
            with env.create_dag_run(dag, self.first_execution_date):
                # Test that all dependencies are specified: no error should be thrown
                env.run_task(telescope.check_dependencies.__name__)

                # Test list releases task with files available
                ti = env.run_task(telescope.get_release_info.__name__)
                start_date, end_date, first_release = ti.xcom_pull(key=CrossrefEventsTelescope.RELEASE_INFO,
                                                                   task_ids=telescope.get_release_info.__name__,
                                                                   include_prior_dates=False)
                self.assertEqual(start_date, dag.default_args['start_date'])
                self.assertEqual(end_date, pendulum.today('UTC') - timedelta(days=1))
                self.assertTrue(first_release)

                # use release info for other tasks
                release = CrossrefEventsRelease(telescope.dag_id, start_date, end_date, first_release,
                                                telescope.mailto, telescope.download_mode, telescope.max_processes)

                # Test download task
                with vcr.use_cassette(self.first_cassette):
                    env.run_task(telescope.download.__name__)

                self.assertEqual(1, len(release.download_files))
                download_path = release.download_files[0]
                self.assert_file_integrity(download_path, self.first_download_hash, 'md5')

                # Test that file uploaded
                env.run_task(telescope.upload_downloaded.__name__)
                self.assert_blob_integrity(env.download_bucket, blob_name(download_path), download_path)

                # Test that file transformed
                env.run_task(telescope.transform.__name__)

                self.assertEqual(1, len(release.transform_files))
                transform_path = release.transform_files[0]
                self.assert_file_integrity(transform_path, self.first_transform_hash, 'gzip_crc')

                # Test that transformed file uploaded
                env.run_task(telescope.upload_transformed.__name__)
                self.assert_blob_integrity(env.transform_bucket, blob_name(transform_path), transform_path)

                # Test that load partition task is skipped for the first release
                ti = env.run_task(telescope.bq_load_partition.__name__)
                self.assertEqual(ti.state, 'skipped')

                # Test delete old task is in success state, without doing anything
                ti = env.run_task(telescope.bq_delete_old.__name__)
                self.assertEqual(ti.state, 'success')

                # Test append new creates table
                env.run_task(telescope.bq_append_new.__name__)
                main_table_id, partition_table_id = table_ids_from_path(transform_path)
                table_id = f'{self.project_id}.{telescope.dataset_id}.{main_table_id}'
                expected_rows = 18
                self.assert_table_integrity(table_id, expected_rows)

                # Test that all telescope data deleted
                download_folder, extract_folder, transform_folder = release.download_folder, release.extract_folder, \
                                                                    release.transform_folder
                env.run_task(telescope.cleanup.__name__)
                self.assert_cleanup(download_folder, extract_folder, transform_folder)

            # second run
            with env.create_dag_run(dag, self.second_execution_date):
                # Test that all dependencies are specified: no error should be thrown
                env.run_task(telescope.check_dependencies.__name__)

                # Test list releases task with files available
                ti = env.run_task(telescope.get_release_info.__name__)
                start_date, end_date, first_release = ti.xcom_pull(key=CrossrefEventsTelescope.RELEASE_INFO,
                                                                   task_ids=telescope.get_release_info.__name__,
                                                                   include_prior_dates=False)
                self.assertEqual(start_date, release.end_date)
                self.assertEqual(end_date, pendulum.today('UTC') - timedelta(days=1))
                self.assertFalse(first_release)

                # use release info for other tasks
                release = CrossrefEventsRelease(telescope.dag_id, start_date, end_date, first_release,
                                                telescope.mailto, telescope.download_mode, telescope.max_processes)

                # Test download task
                with vcr.use_cassette(self.second_cassette):
                    env.run_task(telescope.download.__name__)

                self.assertEqual(1, len(release.download_files))
                download_path = release.download_files[0]
                self.assert_file_integrity(download_path, self.second_download_hash, 'md5')

                # Test that file uploaded
                env.run_task(telescope.upload_downloaded.__name__)
                self.assert_blob_integrity(env.download_bucket, blob_name(download_path), download_path)

                # Test that file transformed
                env.run_task(telescope.transform.__name__)

                self.assertEqual(1, len(release.transform_files))
                transform_path = release.transform_files[0]
                self.assert_file_integrity(transform_path, self.second_transform_hash, 'gzip_crc')

                # Test that transformed file uploaded
                env.run_task(telescope.upload_transformed.__name__)
                self.assert_blob_integrity(env.transform_bucket, blob_name(transform_path), transform_path)

                # Test that load partition task creates partition
                env.run_task(telescope.bq_load_partition.__name__)
                main_table_id, partition_table_id = table_ids_from_path(transform_path)
                table_id = f'{self.project_id}.{telescope.dataset_id}.{partition_table_id}${pendulum.today().strftime("%Y%m%d")}'
                expected_rows = 12
                self.assert_table_integrity(table_id, expected_rows)

                # Test task deleted rows from main table
                env.run_task(telescope.bq_delete_old.__name__)
                table_id = f'{self.project_id}.{telescope.dataset_id}.{main_table_id}'
                expected_rows = 10
                self.assert_table_integrity(table_id, expected_rows)

                # Test append new adds rows to table
                env.run_task(telescope.bq_append_new.__name__)
                table_id = f'{self.project_id}.{telescope.dataset_id}.{main_table_id}'
                expected_rows = 22
                self.assert_table_integrity(table_id, expected_rows)

                # Test that all telescope data deleted
                download_folder, extract_folder, transform_folder = release.download_folder, release.extract_folder, \
                                                                    release.transform_folder
                env.run_task(telescope.cleanup.__name__)
                self.assert_cleanup(download_folder, extract_folder, transform_folder)

    def test_batch_dates(self):
        """ Test the batch_dates property of release
        :return: None.
        """
        # Test different download modes
        self.release.max_processes = 2
        self.release.download_mode = 'sequential'
        batches = self.release.batch_dates
        self.assertEqual(self.start_date.strftime("%Y-%m-%d"), batches[0][0])
        self.assertEqual(self.end_date.strftime("%Y-%m-%d"), batches[-1][1])
        self.assertListEqual([('2020-01-01', '2020-02-01')], batches)

        self.release.download_mode = 'error'
        with self.assertRaises(AirflowException):
            batches = self.release.batch_dates

        # Test different number of max processes with parallel download mode
        self.release.download_mode = 'parallel'
        self.release.max_processes = 1
        batches = self.release.batch_dates
        self.assertEqual(self.start_date.strftime("%Y-%m-%d"), batches[0][0])
        self.assertEqual(self.end_date.strftime("%Y-%m-%d"), batches[-1][1])
        self.assertListEqual([('2020-01-01', '2020-02-01')], batches)

        self.release.max_processes = 2
        batches = self.release.batch_dates
        self.assertEqual(self.start_date.strftime("%Y-%m-%d"), batches[0][0])
        self.assertEqual(self.end_date.strftime("%Y-%m-%d"), batches[-1][1])
        self.assertListEqual([('2020-01-01', '2020-01-16'), ('2020-01-17', '2020-02-01')], batches)

        self.release.max_processes = 8
        batches = self.release.batch_dates
        self.assertEqual(self.start_date.strftime("%Y-%m-%d"), batches[0][0])
        self.assertEqual(self.end_date.strftime("%Y-%m-%d"), batches[-1][1])
        self.assertListEqual([('2020-01-01', '2020-01-04'), ('2020-01-05', '2020-01-08'), ('2020-01-09', '2020-01-12'),
                              ('2020-01-13', '2020-01-16'), ('2020-01-17', '2020-01-20'), ('2020-01-21', '2020-01-24'),
                              ('2020-01-25', '2020-01-28'), ('2020-01-29', '2020-02-01')], batches)

    def test_urls(self):
        """ Test the urls property of release
        :return: None.
        """
        events_url = 'https://api.eventdata.crossref.org/v1/events?mailto={mail_to}' \
                     '&from-collected-date={start_date}&until-collected-date={end_date}&rows=1000'
        edited_url = 'https://api.eventdata.crossref.org/v1/events/edited?' \
                     'mailto={mail_to}&from-updated-date={start_date}' \
                     '&until-updated-date={end_date}&rows=1000'
        deleted_url = 'https://api.eventdata.crossref.org/v1/events/deleted?' \
                      'mailto={mail_to}&from-updated-date={start_date}' \
                      '&until-updated-date={end_date}&rows=1000'

        self.release.download_mode = 'parallel'
        self.release.first_release = True
        urls = self.release.urls
        for i, url_batch in enumerate(urls):
            start_date = self.release.batch_dates[i][0]
            end_date = self.release.batch_dates[i][1]
            expected_list = [events_url.format(mail_to=self.release.mailto, start_date=start_date, end_date=end_date)]
            self.assertEqual(expected_list, url_batch)

        self.release.first_release = False
        urls = self.release.urls
        for i, url_batch in enumerate(urls):
            start_date = self.release.batch_dates[i][0]
            end_date = self.release.batch_dates[i][1]
            expected_list = [events_url.format(mail_to=self.release.mailto, start_date=start_date, end_date=end_date),
                             edited_url.format(mail_to=self.release.mailto, start_date=start_date, end_date=end_date),
                             deleted_url.format(mail_to=self.release.mailto, start_date=start_date, end_date=end_date)]
            self.assertEqual(expected_list, url_batch)

    @patch.object(CrossrefEventsRelease, 'download_events_batch')
    @patch("observatory.platform.utils.template_utils.AirflowVariable.get")
    def test_download(self, mock_variable_get, mock_download_batch):
        """ Test the download method of the release
        :return: None.
        """
        mock_variable_get.return_value = "data"
        self.release.download_mode = 'parallel'
        no_workers = min(self.release.max_processes, len(self.release.urls))

        with CliRunner().isolated_filesystem():
            events_path = 'events.json'

            # Test no failed files, but empty
            with open(events_path, 'w') as f:
                f.write('[]\n')
            mock_download_batch.return_value = [(events_path, True),
                                                (events_path, True),
                                                (events_path, True)]
            success = self.release.download()
            self.assertFalse(success)
            self.assertEqual(no_workers, mock_download_batch.call_count)

            # Test no failed files, not empty
            mock_download_batch.reset_mock()
            with open(events_path, 'w') as f:
                f.write("[{'test': 'test'}]\n")
            mock_download_batch.return_value = [(events_path, True),
                                                (events_path, True),
                                                (events_path, True)]
            success = self.release.download()
            self.assertTrue(success)
            self.assertEqual(no_workers, mock_download_batch.call_count)

            # Test failed files
            mock_download_batch.return_value = [(events_path, True),
                                                (events_path, False),
                                                (events_path, True)]
            with self.assertRaises(AirflowException):
                self.release.download()

    @patch('observatory.dags.telescopes.crossref_events.retry_session')
    def test_extract_events(self, mock_retry_session):
        """ Test extract_events function with unsuccessful response and retry error
        :return: None.
        """
        with CliRunner().isolated_filesystem():
            events_path = 'events.json'

            # Test unsuccesful status code
            mock_retry_session().get.return_value.status_code = 400
            with self.assertRaises(ConnectionError):
                download_events('url', events_path)

            # Test retry error
            mock_retry_session.side_effect = RetryError()
            success, next_cursor, total_events = download_events('url', events_path)
            self.assertFalse(success)
            self.assertIsNone(next_cursor)
            self.assertIsNone(total_events)

    @patch('observatory.dags.telescopes.crossref_events.download_events')
    @patch("observatory.platform.utils.template_utils.AirflowVariable.get")
    def test_download_events_batch(self, mock_variable_get, mock_download_events):
        """ Test download_events_batch function
        :return: None.
        """
        mock_variable_get.return_value = os.path.join(os.getcwd(), "data")
        self.release.first_release = True
        batch_number = 0
        url = self.release.urls[batch_number][0]
        with CliRunner().isolated_filesystem():
            events_path = self.release.batch_path(url)
            cursor_path = self.release.batch_path(url, cursor=True)

            # Test with existing cursor path
            with open(cursor_path, 'w') as f:
                f.write('cursor')
            mock_download_events.return_value = (True, None, 10)
            results = self.release.download_events_batch(batch_number)
            self.assertEqual([(events_path, True)], results)
            self.assertFalse(os.path.exists(cursor_path))
            mock_download_events.assert_called_once_with(url, events_path, 'cursor')

            # Test with no existing previous files
            mock_download_events.reset_mock()
            mock_download_events.return_value = (True, None, 10)
            results = self.release.download_events_batch(batch_number)
            self.assertEqual([(events_path, True)], results)
            mock_download_events.assert_called_once_with(url, events_path)

            # Test with events path from previous successful attempt
            mock_download_events.reset_mock()
            with open(events_path, 'w') as f:
                f.write('events')
            results = self.release.download_events_batch(batch_number)
            self.assertEqual([(events_path, True)], results)
            mock_download_events.assert_not_called()
            os.remove(events_path)

            # Test unsuccesful download_events
            mock_download_events.reset_mock()
            mock_download_events.return_value = (False, 'next_cursor', None)
            results = self.release.download_events_batch(batch_number)
            expected_hash = 'be9a53c4476102bd3576ba96c20542c9'
            self.assertEqual([(events_path, False)], results)
            self.assert_file_integrity(cursor_path, expected_hash, 'md5')
