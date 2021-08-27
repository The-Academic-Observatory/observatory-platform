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

# Author: James Diprose

import glob
import os
import unittest
from typing import List
from zipfile import ZipFile

import natsort
import pendulum
from click.testing import CliRunner
from google.cloud import bigquery
from google.cloud import storage
from google.cloud.storage import Blob

from academic_observatory_workflows.config import test_fixtures_folder
from academic_observatory_workflows.workflows.mag_telescope import (
    db_load_mag_release,
    list_mag_release_files,
    transform_mag_file,
    transform_mag_release,
)
from observatory.platform.utils.file_utils import _hash_file
from observatory.platform.utils.gc_utils import upload_files_to_cloud_storage
from observatory.platform.utils.test_utils import random_id


def extract_mag_release(file_path: str, unzip_path: str):
    """Extract a MAG release.

    :param file_path: the path to the archive to unzip.
    :param unzip_path: the path to unzip the files into. If the zip is of a folder, then the folder will be unzipped
    into this path.
    :return: None.
    """

    with ZipFile(file_path) as zip_file:
        zip_file.extractall(unzip_path)


class TestMagTelescope(unittest.TestCase):
    """ Tests for the functions used by the MAG telescope """

    def __init__(self, *args, **kwargs):
        """Constructor which sets up variables used by tests.

        :param args: arguments.
        :param kwargs: keyword arguments.
        """

        super(TestMagTelescope, self).__init__(*args, **kwargs)
        self.gc_project_id: str = os.getenv("TEST_GCP_PROJECT_ID")
        self.gc_bucket_name: str = os.getenv("TEST_GCP_BUCKET_NAME")
        self.gc_data_location: str = os.getenv("TEST_GCP_DATA_LOCATION")

        self.data_path = test_fixtures_folder("mag", "mag-2020-05-21.zip")
        self.release_date = pendulum.datetime(year=2020, month=5, day=21)
        self.release_folder = "mag-2020-05-21"
        self.extracted_folder = "extracted"
        self.transformed_folder = "transformed"
        self.release_folder = "mag-2020-05-21"
        self.extracted_folder = "extracted"
        self.transformed_folder = "transformed"
        self.folders = ["advanced", "mag", "nlp", "samples"]
        self.sub_folders = [
            ("Authors.txt_aes_tmp_2020-10-11_02-01-24", "Authors.txt"),
            ("PaperExtendedAttributes.txt_aes_tmp_2020-10-11_02-02-00", "PaperExtendedAttributes.txt"),
            ("PaperUrls.txt_aes_tmp_2020-10-11_02-01-43", "PaperUrls.txt"),
        ]
        self.advanced = [
            "EntityRelatedEntities.txt",
            "FieldOfStudyChildren.txt",
            "FieldOfStudyExtendedAttributes.txt",
            "FieldsOfStudy.txt",
            "PaperFieldsOfStudy.txt",
            "PaperRecommendations.txt",
            "RelatedFieldOfStudy.txt",
        ]
        self.mag = [
            "Affiliations.txt",
            "Authors.txt",
            "ConferenceInstances.txt",
            "ConferenceSeries.txt",
            "Journals.txt",
            "PaperAuthorAffiliations.txt",
            "PaperExtendedAttributes.txt",
            "PaperReferences.txt",
            "PaperUrls.txt",
            "Papers.txt",
        ]
        self.nlp = [
            "PaperAbstractsInvertedIndex.txt.1",
            "PaperAbstractsInvertedIndex.txt.2",
            "PaperCitationContexts.txt",
        ]
        self.samples = [
            "CreateDatabase.usql",
            "CreateFunctions.usql",
            "HIndexDatabricksSample.py",
            "ReadMe.pdf",
            "ReleaseNote.txt",
        ]

    def test_list_mag_release_files(self):
        """Test that list_mag_release_files lists all files in the MAG releases folder.
        :return: None.
        """

        with CliRunner().isolated_filesystem():
            # Make MAG folders
            folders = []
            for _, folder in enumerate(self.folders):
                path = os.path.join(self.release_folder, folder)
                os.makedirs(path, exist_ok=True)
                folders.append(path)

            # Make mag sub folders
            for sub_folder, sub_file in self.sub_folders:
                sub_folder_path = os.path.join(self.release_folder, "mag", sub_folder)
                os.makedirs(sub_folder_path, exist_ok=True)
                sub_file_path = os.path.join(sub_folder_path, sub_file)
                open(sub_file_path, "a").close()

            # advanced files
            expected_files = []
            for file_name in self.advanced:
                path = os.path.join(folders[0], file_name)
                open(path, "a").close()
                expected_files.append(path)

            # mag files
            for file_name in self.mag:
                path = os.path.join(folders[1], file_name)
                open(path, "a").close()
                expected_files.append(path)

            # nlp files
            for file_name in self.nlp:
                path = os.path.join(folders[2], file_name)
                open(path, "a").close()
                expected_files.append(path)

            # sample files
            for file_name in self.samples:
                path = os.path.join(folders[3], file_name)
                open(path, "a").close()

            # List MAG releases and check that output is as expected
            files = list_mag_release_files(self.release_folder)
            actual_files = [str(f) for f in files]
            self.assertListEqual(expected_files, actual_files)

        # Check that this function works with a folder of transformed files
        with CliRunner().isolated_filesystem():
            with CliRunner().isolated_filesystem():
                # Make MAG files
                file_names = self.advanced + self.mag + self.nlp
                expected_files = []
                os.makedirs(self.release_folder, exist_ok=True)
                for file_name in file_names:
                    path = os.path.join(self.release_folder, file_name)
                    open(path, "a").close()
                    expected_files.append(path)

                expected_files = sorted(expected_files)

                # List MAG releases and check that output is as expected
                files = list_mag_release_files(self.release_folder)
                actual_files = [str(f) for f in files]
                self.assertListEqual(expected_files, actual_files)

    def test_transform_mag_file(self):
        """Tests that transform_mag_file transforms a single file correctly.

        :return: None.
        """

        with CliRunner().isolated_filesystem():
            # Extract release zip file into folder
            extract_mag_release(self.data_path, self.extracted_folder)

            # Make input and output paths
            input_file_path = os.path.join(self.extracted_folder, self.release_folder, "mag", "Affiliations.txt")
            output_file_path = os.path.join(self.transformed_folder, self.release_folder, "mag")
            os.makedirs(output_file_path)
            output_file_path = os.path.join(output_file_path, "Affiliations.txt")

            # Transform file and check result
            result = transform_mag_file(input_file_path, output_file_path)
            self.assertTrue(result)
            expected_file_hash = "5570569e573a517587d3d11ec00eebf9"
            self.assertEqual(expected_file_hash, _hash_file(output_file_path, algorithm="md5"))

    def test_transform_mag_release(self):
        """Tests that transform_mag_release transforms an entire MAG release.

        :return: None.
        """

        with CliRunner().isolated_filesystem():
            # Make expected files
            expected_files = []
            for file in self.advanced + self.mag + self.nlp:
                expected_files.append(os.path.join(self.transformed_folder, self.release_folder, file))
            expected_files = natsort.natsorted(expected_files)

            # Extract release zip file into folder
            extract_mag_release(self.data_path, self.extracted_folder)

            # Transform release
            input_release_path = os.path.join(self.extracted_folder, self.release_folder)
            output_release_path = os.path.join(self.transformed_folder, self.release_folder)
            os.makedirs(output_release_path)
            result = transform_mag_release(input_release_path, output_release_path)

            # Test that the expected files exist
            self.assertTrue(result)
            actual_files = glob.glob(os.path.join(output_release_path, "**"))
            actual_files = natsort.natsorted(actual_files)
            self.assertEqual(expected_files, actual_files)

    def test_bq_load_mag_release(self):
        """Tests that db_load_mag_release successfully loads a MAG release into BigQuery.

        :return: None.
        """

        with CliRunner().isolated_filesystem():
            # Extract release zip file into folder
            extract_mag_release(self.data_path, self.extracted_folder)

            # Transform release
            input_release_path = os.path.join(self.extracted_folder, self.release_folder)
            output_release_path = os.path.join(self.transformed_folder, self.release_folder)
            os.makedirs(output_release_path)
            result = transform_mag_release(input_release_path, output_release_path)
            self.assertTrue(result)

            # Upload to cloud storage
            base_folder = random_id()
            print(f"base_folder: {base_folder}")
            release_path = f"{base_folder}/{self.release_folder}"
            posix_paths = list_mag_release_files(output_release_path)
            file_paths = [str(path) for path in posix_paths]
            blob_names = [f"{release_path}/{path.name}" for path in posix_paths]

            # Create random dataset id
            client = bigquery.Client()
            dataset_id = random_id()
            try:
                # Upload files to cloud storage
                result = upload_files_to_cloud_storage(self.gc_bucket_name, blob_names, file_paths)
                self.assertTrue(result)

                # Load release into BigQuery
                result = db_load_mag_release(
                    self.gc_project_id,
                    self.gc_bucket_name,
                    self.gc_data_location,
                    release_path,
                    self.release_date,
                    dataset_id=dataset_id,
                )

                # Check that all tables have loaded
                self.assertTrue(result)

                # Check that PaperAbstractsInvertedIndex has 100 rows, since it was loaded from two tables with 50
                # rows each
                table: bigquery.Table = client.get_table(f"{dataset_id}.PaperAbstractsInvertedIndex20200521")
                expected_num_rows = 100
                self.assertEqual(expected_num_rows, table.num_rows)
            finally:
                # Cleanup
                client.delete_dataset(dataset_id, delete_contents=True, not_found_ok=True)

                # Delete all blobs
                storage_client = storage.Client()
                bucket = storage_client.get_bucket(self.gc_bucket_name)
                blobs: List[Blob] = list(bucket.list_blobs(prefix=base_folder))
                for blob in blobs:
                    blob.delete()
