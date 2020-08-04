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

# Author: Richard Hosking

import unittest

from observatory_platform.workflows.doi import db_extend_grid_with_iso3166_and_home_repos, db_aggregate_crossref_events, \
                                               db_aggregate_mag, db_compute_oa_colours_from_unpaywall, db_join_crossref_with_funders, \
                                               db_aggregate_open_citations, db_aggregate_wos, db_aggregate_scopus, db_create_dois_table


class TestDoiWorkflow(unittest.TestCase):
    """ Tests for the functions used by the Doi workflow """

    def __init__(self, *args, **kwargs):
        """ Constructor which sets up variables used by tests.

        :param args: arguments.
        :param kwargs: keyword arguments.
        """

        self.project = ""
        self.location = ""
        self.grid_release = ""
        self.mag_release = ""
        self.unpaywall_release = ""
        self.crossref_release = ""
        self.fundref_release = ""
        self.open_citations_release = ""

    
    def test_extend_grid_with_iso3166_and_home_repo(self):
        """ Tests the SQL and helper function for extending the GRID dataset with iso3166 and home repositories

        :return: None.
        """

        db_extend_grid_with_iso3166_and_home_repos()
    
    def test_aggregate_crossref_events(self):
        """ Tests the SQL and helper function for 

        :return: None.
        """

        db_aggregate_crossref_events()

    
    def test_aggregate_mag(self):
        """ Tests the SQL and helper function for 

        :return: None.
        """

        db_aggregate_mag()

    
    def test_compute_oa_colours_from_unpaywall(self):
        """ Tests the SQL and helper function for 

        :return: None.
        """

        db_compute_oa_colours_from_unpaywall()

    def test_join_crossref_with_funders(self):
        """ Tests the SQL and helper function for 

        :return: None.
        """

        db_join_crossref_with_funders()

    def test_aggregate_open_citations(self):
        """ Tests the SQL and helper function for 

        :return: None.
        """

        db_aggregate_open_citations()

    def test_aggregate_wos(self):
        """ Tests the SQL and helper function for 

        :return: None.
        """

        db_aggregate_wos()

    def test_aggregate_scopus(self):
        """ Tests the SQL and helper function for 

        :return: None.
        """

        db_aggregate_scopus()

    def test_create_dois_table(self):
        """ Tests the SQL and helper function for 

        :return: None.
        """

        db_create_dois_table()

    