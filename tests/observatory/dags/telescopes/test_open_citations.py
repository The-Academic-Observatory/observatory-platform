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

import os
import unittest
from unittest.mock import patch

import pendulum
import vcr

from observatory.dags.telescopes.open_citations import (list_open_citations_releases,
                                                        fetch_open_citations_versions,
                                                        OpenCitationsRelease, File)
from tests.observatory.test_utils import test_fixtures_path


class TestOpenCitations(unittest.TestCase):
    """ Tests for the functions used by the Open Citations workflow """

    def setUp(self) -> None:
        self.list_open_citations_releases_path = os.path.join(test_fixtures_path(),
                                                              'vcr_cassettes',
                                                              'list_open_citations_releases.yaml')
        self.fetch_open_citations_versions_path = os.path.join(test_fixtures_path(),
                                                               'vcr_cassettes',
                                                               'fetch_open_citations_versions.yaml')

    @patch('observatory.platform.utils.config_utils.airflow.models.Variable.get')
    def test_open_citations_release(self, mock_variable_get):
        """ Test that the getter properties in OpenCitationsRelease work as expected.

        :return: None.
        """

        # Mock data variable
        data_path = 'data'
        mock_variable_get.return_value = data_path

        release_date = pendulum.datetime(year=2018, month=7, day=5)
        file_name = 'data.csv.zip'
        file = File(file_name, '', '')
        release = OpenCitationsRelease(release_date, [file])
        self.assertEqual(release.release_name, 'open_citations_2018_07_05')
        self.assertEqual(release.download_path, 'data/telescopes/download/open_citations/open_citations_2018_07_05')
        self.assertEqual(release.extract_path, 'data/telescopes/extract/open_citations/open_citations_2018_07_05')
        self.assertEqual(release.transformed_blob_path, 'telescopes/open_citations/open_citations_2018_07_05/*.csv')
        self.assertEqual(file.parent, release)
        self.assertEqual(file.download_blob_name, f'telescopes/open_citations/open_citations_2018_07_05/{file_name}')

    def test_list_open_citations_releases(self):
        """ Test that the list_open_citations_releases function returns the correct results.

        :return: None.
        """

        with vcr.use_cassette(self.list_open_citations_releases_path):
            # No start date or end date
            expected_release_dates = [pendulum.datetime(year=2018, month=7, day=5),
                                      pendulum.datetime(year=2018, month=7, day=13),
                                      pendulum.datetime(year=2018, month=11, day=12),
                                      pendulum.datetime(year=2020, month=1, day=21),
                                      pendulum.datetime(year=2020, month=3, day=23),
                                      pendulum.datetime(year=2020, month=5, day=13),
                                      pendulum.datetime(year=2020, month=7, day=4),
                                      pendulum.datetime(year=2020, month=9, day=7)]
            releases = list_open_citations_releases()
            self.assertEqual(len(releases), 8)
            for expected_date, release in zip(expected_release_dates, releases):
                self.assertEqual(expected_date, release.release_date.date())

            # Start date
            expected_release_dates = [pendulum.datetime(year=2020, month=7, day=4),
                                      pendulum.datetime(year=2020, month=9, day=7)]
            releases = list_open_citations_releases(start_date=pendulum.datetime(year=2020, month=7, day=1))
            self.assertEqual(len(releases), 2)
            for expected_date, release in zip(expected_release_dates, releases):
                self.assertEqual(expected_date, release.release_date.date())

            # End date
            expected_release_dates = [pendulum.datetime(year=2018, month=7, day=5),
                                      pendulum.datetime(year=2018, month=7, day=13)]
            releases = list_open_citations_releases(end_date=pendulum.datetime(year=2018, month=7, day=15))
            self.assertEqual(len(releases), 2)
            for expected_date, release in zip(expected_release_dates, releases):
                self.assertEqual(expected_date, release.release_date.date())

            # Start date and end date
            expected_release_dates = [pendulum.datetime(year=2018, month=11, day=12),
                                      pendulum.datetime(year=2020, month=1, day=21),
                                      pendulum.datetime(year=2020, month=3, day=23)]
            releases = list_open_citations_releases(start_date=pendulum.datetime(year=2018, month=11, day=1),
                                                    end_date=pendulum.datetime(year=2020, month=4, day=1))
            self.assertEqual(len(releases), 3)
            for expected_date, release in zip(expected_release_dates, releases):
                self.assertEqual(expected_date, release.release_date.date())

    def test_fetch_open_citations_versions(self):
        """ Test that the fetch_open_citations_versions function returns the correct results.

        :return: None.
        """

        with vcr.use_cassette(self.fetch_open_citations_versions_path):
            expected_versions = [
                {
                    'url': 'https://api.figshare.com/v2/articles/6741422/versions/1',
                    'version': 1
                },
                {
                    'url': 'https://api.figshare.com/v2/articles/6741422/versions/2',
                    'version': 2
                },
                {
                    'url': 'https://api.figshare.com/v2/articles/6741422/versions/3',
                    'version': 3
                },
                {
                    'url': 'https://api.figshare.com/v2/articles/6741422/versions/4',
                    'version': 4
                },
                {
                    'url': 'https://api.figshare.com/v2/articles/6741422/versions/5',
                    'version': 5
                },
                {
                    'url': 'https://api.figshare.com/v2/articles/6741422/versions/6',
                    'version': 6
                },
                {
                    'url': 'https://api.figshare.com/v2/articles/6741422/versions/7',
                    'version': 7
                },
                {
                    'url': 'https://api.figshare.com/v2/articles/6741422/versions/8',
                    'version': 8
                }
            ]
            versions = fetch_open_citations_versions()
            self.assertEqual(len(versions), 8)
            for expected_version, version in zip(expected_versions, versions):
                self.assertEqual(expected_version, version)
