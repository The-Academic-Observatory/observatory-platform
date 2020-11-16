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

# Author: Cameron Neylon

from pathlib import Path

import pydata_google_auth

from tests.observatory.test_utils import test_fixtures_path
from observatory.reports.chart_utils import calculate_confidence_interval
from observatory.reports.tables import (
    InstitutionOpenAccessTable,
    InstitutionFundersTable,
    InstitutionOutputsTable,
    InstitutionCitationsTable
)


def update_chart_test_data():
    """ Use the current tables definition to get up to date test data if necessary"""

    scopes = [
        'https://www.googleapis.com/auth/cloud-platform',
        'https://www.googleapis.com/auth/drive',
    ]

    credentials = pydata_google_auth.get_user_credentials(
        scopes,
    )

    scope = """AND
  id in (SELECT id from `open-knowledge-publications.institutional_oa_evaluation_2020.named_grids_in_scope`)
"""
    # Set the arguments up for table production
    args = [credentials, 'coki-scratch-space', scope, 2018, (2016, 2020)]
    openaccess = InstitutionOpenAccessTable(*args)
    calculate_confidence_interval(openaccess.df, ['percent_oa'])
    funders = InstitutionFundersTable(*args)
    output_types = InstitutionOutputsTable(*args)
    citations = InstitutionCitationsTable(*args)

    fixtures_path = Path(test_fixtures_path())
    openaccess.df.to_csv(fixtures_path / 'reports' / 'test_oa_data.csv')
    funders.df.to_csv(fixtures_path / 'reports' / 'test_funding_data.csv')
    output_types.df.to_csv(fixtures_path / 'reports' / 'test_outputs_data.csv')
    citations.df.to_csv(fixtures_path / 'reports' / 'test_citations_data.csv')


if __name__ == '__main__':
    update_chart_test_data()
