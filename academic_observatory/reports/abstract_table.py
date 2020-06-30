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

# Author: Cameron Neylon & Richard Hosking

import pandas as pd

from academic_observatory.reports import defaults
from academic_observatory.reports import chart_utils


class AbstractObservatoryTable:
    """Abstract Base Class for Tables
    """

    sql_template = ''
    bq_table = ''

    def __init__(self,
                 credentials=None,
                 project_id: str = defaults.project_id,
                 scope: str = defaults.scope,
                 focus_year: int = None,
                 year_range: tuple = None,
                 collect_and_run: bool = True,
                 **kwargs):
        """Initialisation function
        """

        if not credentials:
            credentials = report_utils.get_gcp_credentials()
        self.credentials = credentials
        self.project_id = project_id
        self.scope = scope
        self.focus_year = focus_year
        self.year_range = year_range

        if collect_and_run:
            self.collect_data()
            self.clean_data()

    def __repr__(self):
        """Return string for the object
        """

        return self.df

    def format_sql(self):
        """Format SQL data
        """

        sql = self.sql_template.format(bq_table=self.bq_table,
                                       year_range=self.year_range,
                                       focus_year=self.focus_year,
                                       scope=self.scope)
        return sql

    def collect_data(self,
                     dialect='standard',
                     verbose=False):
        """Get the data from BigQuery
        """

        self.df = pd.io.gbq.read_gbq(self.format_sql(),
                                     project_id=self.project_id,
                                     credentials=self.credentials,
                                     dialect=dialect,
                                     verbose=verbose)
        self.clean_data()
        return self.df

    def clean_data(self):
        chart_utils.clean_geo_names(self.df)
        chart_utils.nice_column_names(self.df)
