import io
import logging
import os
import shutil
import unittest
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import PropertyMock, patch

import pendulum
from airflow.exceptions import AirflowException
from botocore.client import BaseClient
from click.testing import CliRunner

from observatory_platform.telescopes.orcid import OrcidRelease, \
    OrcidTelescope, \
    bq_append_from_file, \
    bq_append_from_partition, \
    bq_delete_old_records, \
    bq_load_partition, \
    change_keys, \
    cleanup_dirs, \
    convert, \
    download_records, \
    transfer_records, \
    transform_records, \
    write_boto_config, \
    write_modified_record_prefixes
from observatory_platform.utils.config_utils import SubFolder, find_schema, schema_path, telescope_path
from observatory_platform.utils.data_utils import _hash_file
from observatory_platform.utils.gc_utils import aws_to_google_cloud_storage_transfer, storage_bucket_exists
from observatory_platform.utils.gc_utils import bigquery_partitioned_table_id
from tests.observatory_platform.config import test_fixtures_path


class TestOrcid(unittest.TestCase):
    """ Tests for the functions used by the orcid telescope """

    def __init__(self, *args, **kwargs):
        """ Constructor which sets up variables used by tests.
        :param args: arguments.
        :param kwargs: keyword arguments.
        """

        super(TestOrcid, self).__init__(*args, **kwargs)

        self.start_date = pendulum.parse('2020-08-11 11:48:23.795099+00:00')
        # self.start_date = pendulum.parse('2020-10-05 11:48:23.795099+00:00')
        self.end_date = pendulum.instance(datetime.strptime('2020-08-17 00:00:05.041842+00:00', '%Y-%m-%d '
                                                                                                '%H:%M:%S.%f%z'))
        # fake keys generated with canarytokens.org
        self.aws_access_key_id = 'AKIAXYZDQCEN3MXYPAZU'
        self.aws_secret_access_key = 'hYt3jGZLS46JiWe5/2H2vfhGHw0UD4Gox1rio8fV'
        # self.end_date = pendulum.instance(datetime.strptime('2020-10-07 03:16:49.041842+00:00', '%Y-%m-%d '
        #                                                                                           '%H:%M:%S.%f%z'))
        # Turn logging to warning because vcr prints too much at info level
        logging.basicConfig()

    # @patch('botocore.response.StreamingBody.read')
    @patch('botocore.client.BaseClient._make_api_call')
    @patch('observatory_platform.utils.config_utils.AirflowVariable.get')
    def test_write_modified_record_prefixes(self, mock_variable_get, mock_api_call):
        modified_records_hash = '46e2733588b17dc5c441ff99de3fa03e'
        gc_download_bucket = 'orcid_sync'
        with CliRunner().isolated_filesystem():
            # set telescope data path variable
            mock_variable_get.side_effect = side_effect
            lambda_file_path = os.path.join(test_fixtures_path(), 'telescopes', 'orcid_last_modified.csv.tar')
            mock_api_call.return_value = {
                'Body': open(lambda_file_path, 'rb')
            }

            # # s3 = botocore.session.get_session().create_client('s3')
            # stubber = Stubber(s3_resource)
            # response = {'Body': 'Mock'}
            # stubber.add_response('get_object', response)
            # with stubber:
            #     mock_boto_client.return_value = s3_resource

            # with open(os.path.join(test_fixtures_path(), 'telescopes', 'orcid_last_modified.csv.tar'), 'rb') as f:
            #     mock_read.return_value = f.read()

            release = OrcidRelease(self.start_date, self.end_date)
            modified_records_count = write_modified_record_prefixes(self.aws_access_key_id, self.aws_secret_access_key,
                                                                    gc_download_bucket, release)
            self.assertEqual(6, modified_records_count)
            self.assertTrue(os.path.isfile(release.modified_records_path))
            self.assertEqual(modified_records_hash, _hash_file(release.modified_records_path, algorithm='md5'))

    @patch('observatory_platform.utils.config_utils.AirflowVariable.get')
    def test_write_boto_config(self, mock_variable_get):
        boto_config_file_hash = '915c0ebc6a2eebe12e85fc6fae35e02a'
        with CliRunner().isolated_filesystem():
            mock_variable_get.side_effect = side_effect

            boto_config_path = write_boto_config(self.aws_access_key_id, self.aws_secret_access_key)
            self.assertTrue(os.path.isfile(boto_config_path))
            self.assertEqual(boto_config_file_hash, _hash_file(boto_config_path, algorithm='md5'))

    @patch('observatory_platform.telescopes.orcid.write_modified_record_prefixes')
    @patch('observatory_platform.telescopes.orcid.args_list')
    @patch('airflow.hooks.base_hook.BaseHook.get_connection')
    @patch('observatory_platform.telescopes.orcid.Variable.get')
    @patch('observatory_platform.utils.config_utils.AirflowVariable.get')
    def test_download_records(self, mock_variable_get, mock_variable_telescope_get, mock_get_connection,
                              mock_args_list,  mock_write_records):
        with CliRunner().isolated_filesystem():
            mock_variable_get.side_effect = side_effect
            mock_variable_telescope_get.side_effect = side_effect
            mock_get_connection.return_value = SimpleNamespace(login=self.aws_access_key_id,
                                                               password=self.aws_secret_access_key)

            # test stdout when downloading records of first release
            release = OrcidRelease(self.start_date, self.end_date, True)
            mock_args_list.return_value = ['echo', 'first_release']
            with patch('sys.stdout', new_callable=io.StringIO) as mock_stdout:
                download_records(release)
                self.assertEqual('first_release', mock_stdout.getvalue().split('\n')[-2])

            release = OrcidRelease(self.start_date, self.end_date, False)
            with open(release.modified_records_path, 'w') as f:
                f.write("gs://orcid_bucket/001/0000-0003-1168-8001.xml\ngs://orcid_bucket/002/0000-0003-1168-8002.xml"
                        "\n")

            # test stdout when downloading records of later release and correct number of modified records count
            release.modified_records_count = 2
            mock_write_records.return_value = 2
            mock_args_list.return_value = ['cat', '-']
            with patch('sys.stdout', new_callable=io.StringIO) as mock_stdout:
                download_records(release)
                self.assertEqual('gs://orcid_bucket/001/0000-0003-1168-8001.xml',
                                 mock_stdout.getvalue().split('\n')[-3])
                self.assertEqual('gs://orcid_bucket/002/0000-0003-1168-8002.xml',
                                 mock_stdout.getvalue().split('\n')[-2])

            # test airflow exception of later release and incorrect number of modified records count
            release.modified_records_count = 3
            mock_write_records.return_value = 2
            mock_args_list.return_value = ['cat', '-']
            with self.assertRaises(AirflowException):
                download_records(release)

    def test_convert(self):
        actual_key = convert('unit:@test-convert')
        self.assertEqual('test_convert', actual_key)

    def test_change_keys(self):
        old_dict = {
            '@xmlns:internal': 'value-1',
            'common:orcid-identifier': {
                'common:uri': 'value-2'
            },
            'external-identifier:external-identifiers': {
                '@put-code': 'value-3',
                'common:source': {
                    'common:source-name': 'value-4',
                    'common:source-client-id': 'value-5'
                }
            }
        }
        actual_new_dict = change_keys(old_dict, convert)
        expected_new_dict = {
            'orcid_identifier': {
                'uri': 'value-2'
            },
            'external_identifiers': {
                'put_code': 'value-3',
                'source': {
                    'source_name': 'value-4',
                    'source_client_id': 'value-5'
                }
            }
        }
        self.assertEqual(expected_new_dict, actual_new_dict)

    @patch('observatory_platform.telescopes.orcid.OrcidRelease.download_dir', new_callable=PropertyMock)
    @patch('observatory_platform.utils.config_utils.AirflowVariable.get')
    def test_transform_records(self, mock_variable_get, mock_download_dir):
        transform_hash = '25604802e68bda8eeffc2886d234a0ff'
        with CliRunner().isolated_filesystem():
            mock_variable_get.side_effect = side_effect
            download_dir = os.path.join(os.getcwd(), 'data/telescopes/download')
            mock_download_dir.return_value = download_dir

            # make download dir and add two record files
            os.makedirs(download_dir)
            os.makedirs(os.path.join(download_dir, '060'))
            os.makedirs(os.path.join(download_dir, '061'))
            orcid_record_path = os.path.join(test_fixtures_path(), 'telescopes', 'orcid_record.xml')
            shutil.copyfile(orcid_record_path, os.path.join(download_dir, '060', '0000-0003-0776-6060.xml'))
            shutil.copyfile(orcid_record_path, os.path.join(download_dir, '061', '0000-0003-0776-6061.xml'))
            # make file in download directory that should be ignored
            with open(os.path.join(download_dir, 'other_file.txt'), 'w') as f:
                f.write('test')

            release = OrcidRelease(self.start_date, self.end_date)
            transform_records(release)

            self.assertTrue(os.path.isfile(release.transform_path))
            self.assertEqual(transform_hash, _hash_file(release.transform_path, algorithm='md5'))

    @patch('observatory_platform.telescopes.orcid.load_bigquery_table')
    @patch('observatory_platform.telescopes.orcid.Variable.get')
    @patch('observatory_platform.utils.config_utils.AirflowVariable.get')
    def test_bq_load_partition(self, mock_variable_get, mock_variable_telescope_get, mock_load_bq_table):
        with CliRunner().isolated_filesystem():
            mock_variable_get.side_effect = side_effect
            mock_variable_telescope_get.side_effect = side_effect
            release = OrcidRelease(self.start_date, self.end_date)

            # create expected arguments with which load bq table should be called with
            uri = f"gs://{side_effect('transform_bucket_name')}/{release.blob_name}"
            table_id = bigquery_partitioned_table_id(OrcidTelescope.PARTITION_TABLE_ID, release.end_date)
            schema_file_path = find_schema(schema_path('telescopes'), OrcidTelescope.DAG_ID, release.end_date,
                                           ver='2.0')

            bq_load_partition(release)
            mock_load_bq_table.assert_called_with(uri, OrcidTelescope.DAG_ID, side_effect('data_location'), table_id,
                                                  schema_file_path, 'NEWLINE_DELIMITED_JSON', partition=True,
                                                  require_partition_filter=False)

    @patch('observatory_platform.telescopes.orcid.run_bigquery_query')
    def test_bq_delete_old_records(self, mock_run_query):
        start_date = "2020-12-01"
        end_date = "2020-12-08"

        bq_delete_old_records(start_date, end_date)

        expected_query = "\n\nMERGE\n  orcid.orcid M\nUSING\n  (SELECT common_orcid_identifier.common_path, " \
                         "history_history.common_last_modified_date FROM orcid.orcid_partitions WHERE _PARTITIONDATE " \
                         f"BETWEEN '{start_date}' AND '{end_date}') P\nON\n  M.common_orcid_identifier.common_path = " \
                         "P.common_orcid_identifier.common_path\nWHEN MATCHED AND " \
                         "M.history_history.common_last_modified_date <= P.history_history.common_last_modified_date " \
                         "OR M.history_history.common_last_modified_date is null THEN\n  DELETE"
        mock_run_query.assert_called_with(expected_query)

    @patch('observatory_platform.telescopes.orcid.copy_bigquery_table')
    @patch('observatory_platform.telescopes.orcid.create_bigquery_dataset')
    @patch('observatory_platform.telescopes.orcid.Variable.get')
    @patch('observatory_platform.utils.config_utils.AirflowVariable.get')
    def test_bq_append_from_partition(self, mock_variable_get, mock_variable_telescope_get, mock_create_bq_dataset, mock_copy_bq_table):
        with CliRunner().isolated_filesystem():
            mock_variable_get.side_effect = side_effect
            mock_variable_telescope_get.side_effect = side_effect
            release = OrcidRelease(self.start_date, self.end_date)

            bq_append_from_partition(release, pendulum.parse("2020-12-01"), pendulum.parse("2020-12-08"))
            mock_create_bq_dataset.assert_called_once()
            source_table_ids = ['workflows-dev.orcid.orcid_partitions$20201201',
                                'workflows-dev.orcid.orcid_partitions$20201202',
                                'workflows-dev.orcid.orcid_partitions$20201203',
                                'workflows-dev.orcid.orcid_partitions$20201204',
                                'workflows-dev.orcid.orcid_partitions$20201205',
                                'workflows-dev.orcid.orcid_partitions$20201206',
                                'workflows-dev.orcid.orcid_partitions$20201207',
                                'workflows-dev.orcid.orcid_partitions$20201208']
            mock_copy_bq_table.assert_called_with(source_table_ids, OrcidTelescope.MAIN_TABLE_ID, side_effect(
                'data_location'), 'WRITE_APPEND')

    @patch('observatory_platform.telescopes.orcid.load_bigquery_table')
    @patch('observatory_platform.telescopes.orcid.create_bigquery_dataset')
    @patch('observatory_platform.telescopes.orcid.Variable.get')
    @patch('observatory_platform.utils.config_utils.AirflowVariable.get')
    def test_bq_append_from_file(self, mock_variable_get, mock_variable_telescope_get, mock_create_bq_dataset,
                                 mock_load_bq_table):
        with CliRunner().isolated_filesystem():
            mock_variable_get.side_effect = side_effect
            mock_variable_telescope_get.side_effect = side_effect
            release = OrcidRelease(self.start_date, self.end_date)

            # create expected arguments with which load bq table should be called with
            uri = f"gs://{side_effect('transform_bucket_name')}/{release.blob_name}"
            table_id = OrcidTelescope.MAIN_TABLE_ID
            schema_file_path = find_schema(schema_path('telescopes'), OrcidTelescope.DAG_ID, release.end_date,
                                           ver='2.0')

            bq_append_from_file(release)
            mock_create_bq_dataset.assert_called_once()
            mock_load_bq_table.assert_called_with(uri, OrcidTelescope.DATASET_ID, side_effect('data_location'),
                                                  table_id, schema_file_path, 'NEWLINE_DELIMITED_JSON',
                                                  write_disposition='WRITE_APPEND')

    @patch('observatory_platform.telescopes.orcid.logging.warning')
    @patch('observatory_platform.utils.config_utils.AirflowVariable.get')
    def test_cleanup_dirs(self, mock_variable_get, mock_logging_warning):
        with CliRunner().isolated_filesystem():
            mock_variable_get.side_effect = side_effect
            release = OrcidRelease(self.start_date, self.end_date)

            download_dir = release.subdir(SubFolder.downloaded)
            transform_dir = release.subdir(SubFolder.transformed)
            os.makedirs(download_dir)
            os.makedirs(transform_dir)

            cleanup_dirs(release)
            self.assertFalse(os.path.exists(download_dir))
            self.assertFalse(os.path.exists(transform_dir))
            mock_logging_warning.assert_not_called()

            cleanup_dirs(release)
            self.assertEqual(2, mock_logging_warning.call_count)

    @patch('observatory_platform.utils.config_utils.AirflowVariable.get')
    def test_orcid_release(self, mock_variable_get):
        with CliRunner().isolated_filesystem():
            mock_variable_get.side_effect = side_effect

            release = OrcidRelease(self.start_date, self.end_date, True)
            self.assertTrue(release.first_release)
            self.assertEqual(self.start_date, release.start_date)
            self.assertEqual(self.end_date, release.end_date)
            self.assertEqual(0, release.modified_records_count)
            self.assertIsInstance(release.lambda_file_path, str)
            self.assertIsInstance(release.modified_records_path, str)
            self.assertIsInstance(release.continuation_token_path, str)
            self.assertIsInstance(release.download_dir, str)
            self.assertIsInstance(release.transform_path, str)
            # self.assertIsInstance(release.blob_dir_download, str)
            self.assertIsInstance(release.blob_name, str)

            release = OrcidRelease(self.start_date, self.end_date, False)
            self.assertFalse(release.first_release)

    @patch('observatory_platform.utils.config_utils.AirflowVariable.get')
    def test_lambda_file_path(self, mock_variable_get):
        with CliRunner().isolated_filesystem():
            mock_variable_get.side_effect = side_effect
            release = OrcidRelease(self.start_date, self.end_date)

            actual_path = release.lambda_file_path
            expected_path = release.get_path(SubFolder.downloaded, 'last_modified.csv.tar')
            self.assertEqual(expected_path, actual_path)

    @patch('observatory_platform.utils.config_utils.AirflowVariable.get')
    def test_modified_records_path(self, mock_variable_get):
        with CliRunner().isolated_filesystem():
            mock_variable_get.side_effect = side_effect
            release = OrcidRelease(self.start_date, self.end_date)

            actual_path = release.modified_records_path
            expected_path = release.get_path(SubFolder.downloaded, 'modified_records.txt')
            self.assertEqual(expected_path, actual_path)

    @patch('observatory_platform.utils.config_utils.AirflowVariable.get')
    def test_continuation_token_path(self, mock_variable_get):
        with CliRunner().isolated_filesystem():
            mock_variable_get.side_effect = side_effect
            release = OrcidRelease(self.start_date, self.end_date)

            actual_path = release.continuation_token_path
            expected_path = release.get_path(SubFolder.downloaded, 'continuation_token.txt')
            self.assertEqual(expected_path, actual_path)

    @patch('observatory_platform.utils.config_utils.AirflowVariable.get')
    def test_download_dir(self, mock_variable_get):
        with CliRunner().isolated_filesystem():
            mock_variable_get.side_effect = side_effect
            release = OrcidRelease(self.start_date, self.end_date)

            actual_path = release.download_dir
            expected_path = release.subdir(SubFolder.downloaded)
            self.assertEqual(expected_path, actual_path)
            self.assertTrue(os.path.exists(actual_path))

    @patch('observatory_platform.utils.config_utils.AirflowVariable.get')
    def test_transform_path(self, mock_variable_get):
        with CliRunner().isolated_filesystem():
            mock_variable_get.side_effect = side_effect
            release = OrcidRelease(self.start_date, self.end_date)

            actual_path = release.transform_path
            date_str = release.start_date.strftime("%Y_%m_%d") + "-" + release.end_date.strftime("%Y_%m_%d")
            expected_path = release.get_path(SubFolder.transformed, f"{OrcidTelescope.DAG_ID}_{date_str}.json")
            self.assertEqual(expected_path, actual_path)

    # @patch('observatory_platform.utils.config_utils.AirflowVariable.get')
    # def test_blob_dir_download(self, mock_variable_get):
    #     with CliRunner().isolated_filesystem():
    #         mock_variable_get.side_effect = side_effect
    #         release = OrcidRelease(self.start_date, self.end_date)
    #
    #         actual_blob_name = release.blob_dir_download
    #         date_str = release.start_date.strftime("%Y_%m_%d") + "-" + release.end_date.strftime("%Y_%m_%d")
    #         expected_blob_name = f'telescopes/{OrcidTelescope.DAG_ID}/{date_str}'
    #         self.assertEqual(expected_blob_name, actual_blob_name)

    @patch('observatory_platform.utils.config_utils.AirflowVariable.get')
    def test_blob_name(self, mock_variable_get):
        with CliRunner().isolated_filesystem():
            mock_variable_get.side_effect = side_effect
            release = OrcidRelease(self.start_date, self.end_date)

            actual_blob_name = release.blob_name
            date_str = release.start_date.strftime("%Y_%m_%d") + "-" + release.end_date.strftime("%Y_%m_%d")
            expected_blob_name = f'telescopes/{OrcidTelescope.DAG_ID}/{OrcidTelescope.DAG_ID}_{date_str}.json'
            self.assertEqual(expected_blob_name, actual_blob_name)

    @patch('observatory_platform.utils.config_utils.AirflowVariable.get')
    def test_subdir(self, mock_variable_get):
        with CliRunner().isolated_filesystem():
            mock_variable_get.side_effect = side_effect
            release = OrcidRelease(self.start_date, self.end_date)

            date_str = release.start_date.strftime("%Y_%m_%d") + "-" + release.end_date.strftime("%Y_%m_%d")
            for subdir in [SubFolder.downloaded, SubFolder.transformed]:
                actual_dir = release.subdir(subdir)
                expected_dir = os.path.join(telescope_path(subdir, OrcidTelescope.DAG_ID), date_str)
                self.assertEqual(expected_dir, actual_dir)

    @patch('observatory_platform.utils.config_utils.AirflowVariable.get')
    def test_get_path(self, mock_variable_get):
        with CliRunner().isolated_filesystem():
            mock_variable_get.side_effect = side_effect
            release = OrcidRelease(self.start_date, self.end_date)

            for subdir in [SubFolder.downloaded, SubFolder.transformed]:
                actual_path = release.get_path(subdir, "test.json")
                expected_path = os.path.join(release.subdir(subdir), "test.json")
                self.assertEqual(expected_path, actual_path)
                self.assertTrue(os.path.exists(release.subdir(subdir)))

    # @patch('observatory_platform.utils.config_utils.AirflowVariable.get')
    # def test_transfer_all_records(self, mock_variable_get):
    #     with CliRunner().isolated_filesystem():
    #         mock_variable_get.side_effect = side_effect
    #
    #         gc_project_id = 'workflows-dev'
    #         gc_bucket = 'orcid_sync'
    #
    #         get_all_record_prefixes(aws_access_key_id, aws_secret_access_key, self.start_date)

    # def test_google_transfer(self):
    #     os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/Users/284060a/keys/gcp/workflows-dev-ead352480b42.json"
    #
    #     storage_bucket_exists('orcid_sync')
    #
    #     aws_bucket = 'test-orcid-data'
    #
    #     include_prefixes = []
    #     gc_project_id = 'workflows-dev'
    #     gc_bucket = 'orcid_sync'
    #     # aws_to_google_cloud_storage_transfer(aws_access_key_id, aws_secret_access_key,
    #     #                                      'v2.0test', [],
    #     #                                      gc_project_id, gc_bucket, "test",
    #     #                                      self.start_date,
    #     #                                      self.end_date)
    #     # aws_to_google_cloud_storage_transfer(aws_access_key_id, aws_secret_access_key, aws_bucket, include_prefixes,
    #     #                                      gc_project_id, gc_bucket, "test", last_modified_since=datetime(2020,10,
    #     #                                                                                                     12,3,00),
    #     #                                      last_modified_before=datetime.utcnow())
    #
    #     aws_bucket = 'v2.0-summaries'
    #
    #     aws_to_google_cloud_storage_transfer(aws_access_key_id, aws_secret_access_key, OrcidTelescope.SUMMARIES_BUCKET,
    #                                          ['060/0000-0003-0776-6060'], gc_project_id, gc_bucket, "test",
    #                                          self.start_date, self.end_date)
    #
    # @patch('airflow.hooks.base_hook.BaseHook.get_connection')
    # @patch('observatory_platform.utils.config_utils.airflow.models.Variable.get')
    # def test_transfer_records(self, mock_variable_get, mock_get_connection):
    #     with CliRunner().isolated_filesystem():
    #         # set telescope data path variable
    #         # mock_variable_get.return_value = 'data'
    #         mock_variable_get.side_effect = side_effect
    #
    #         mock_get_connection.return_value = SimpleNamespace(login=aws_access_key_id, password=aws_secret_access_key)
    #
    #         # transfer_records(self.start_date, True)
    #         transfer_records(self.start_date, False)


def side_effect(arg):
    values = {
        'project_id': 'workflows-dev',
        'orcid_bucket_name': 'orcid_sync',
        'download_bucket_name': 'workflows-dev-910922156021-dev',
        'transform_bucket_name': 'transform-bucket',
        'data_path': 'data',
        'data_location': 'US'
    }
    return values[arg]


def mock_make_api_call(self, operation_name, **kwargs):
    if operation_name == 'get_object':
        parsed_response = {
            'Body': 'Mock'
        }
        return parsed_response
    return BaseClient._make_api_call(self, operation_name, kwargs)
