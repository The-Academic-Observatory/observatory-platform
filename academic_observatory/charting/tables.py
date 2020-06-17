# Copyright 2019 Curtin University
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

# Author: Cameron Neylon & Richard Hosing 

import pandas as pd
import pydata_google_auth

from utils import chart_utils

class GenericOpenAccessTable(AbstractObservatoryTable):
    sql_template = """
SELECT
  table.id as id,
  table.name as name,
  table.country as country,
  table.country_code as country_code,
  table.region as region,
  table.subregion as subregion,
  years.published_year as published_year,
  years.metrics.total as total,
  years.metrics.oa as oa,
  years.metrics.green as green,
  years.metrics.gold as gold,
  years.metrics.hybrid as hybrid,
  years.metrics.green_only as green_only,
  years.metrics.gold_just_doaj as gold_just_doaj,
  years.metrics.percent_OA as percent_oa,
  years.metrics.percent_green as percent_green,
  years.metrics.percent_gold as percent_gold,
  years.metrics.percent_gold_doaj as percent_gold_just_doaj,
  years.metrics.percent_green_only as percent_green_only,
  years.metrics.percent_hybrid as percent_hybrid,
  years.metrics.percent_bronze as percent_bronze
FROM `{bq_table}` as table,
    UNNEST(years) as years
WHERE
    years.published_year > {year_range[0]} and
    years.published_year < {year_range[1]} {scope}
"""


class GenericOutputsTable(AbstractObservatoryTable):
    sql_template = """
SELECT
  table.id as id,
  table.name as name,
  table.country as country,
  table.country_code as country_code,
  table.region as region,
  table.subregion as subregion,
  years.published_year as published_year,
  years.metrics.total as total,   
  type.output_type as type,
  type.total as count,
  type.oa as oa,
  type.green as green,
  type.gold as gold,
  type.hybrid as hybrid   
FROM `{bq_table}` as table,
  UNNEST(years) as years,
  UNNEST(years.output_types) as type
WHERE
    years.published_year > {year_range[0]} and
    years.published_year < {year_range[1]} and
    type.total > 5 {scope}
"""

    def clean_data(self):
        helpers.calculate_percentages(self.df,
                                      ['oa', 'green', 'gold'],
                                      'total')
        helpers.clean_output_type_names(self.df)
        super().clean_data()


class GenericCollaborationsTable(AbstractObservatoryTable):
    sql_template = """
SELECT
  table.id as id,
  years.published_year as published_year,
  years.metrics.total as total,
  collabs.id as collab_id,
  collabs.count as count,
  collabs.name as name,
  collabs.country as country,
  collabs.region as region,
  collabs.coordinates as coordinates
FROM `{bq_table}` as table,
  UNNEST(years) as years,
  UNNEST(years.collaborations) as collabs
WHERE
    years.published_year > {year_range[0]} and
    years.published_year < {year_range[1]} and
    collabs.count > 5 {scope}
"""


class GenericPublishersTable(AbstractObservatoryTable):
    sql_template = """
SELECT
  table.id as id,
  table.name as name,
  table.country as country,
  table.country_code as country_code,
  table.region as region,
  table.subregion as subregion,
  years.published_year as published_year,
  years.metrics.total as total,
  publishers.publisher as publisher,
  publishers.count as count,
  publishers.oa as oa,
  publishers.gold as gold,
  publishers.green as green
FROM `{bq_table}` as table,
  UNNEST(years) as years,
  UNNEST(years.publishers) as publishers
WHERE
    years.published_year > {year_range[0]} and
    years.published_year < {year_range[1]} and
    publishers.count > 50 {scope}
"""

    def clean_data(self):
        helpers.calculate_percentages(self.df,
                                      ['oa', 'green', 'gold'],
                                      'count')
        super().clean_data()


class GenericJournalsTable(AbstractObservatoryTable):
    sql_template = """
SELECT
  table.id as id,
  table.name as name,
  table.country as country,
  table.country_code as country_code,
  table.region as region,
  table.subregion as subregion,
  years.published_year as published_year,
  years.metrics.total as total,
  journals.journal as journal,
  journals.count as count,
  journals.oa as oa,
  journals.gold as gold,
  journals.green as green
FROM `{bq_table}` as table,
  UNNEST(years) as years,
  UNNEST(years.journals) as journals
WHERE
    years.published_year > {year_range[0]} and
    years.published_year < {year_range[1]} and
    journals.count > 5 {scope}
"""

    def clean_data(self):
        helpers.calculate_percentages(self.df,
                                      ['oa', 'green', 'gold'],
                                      'count')
        super().clean_data()


class GenericFundersTable(AbstractObservatoryTable):
    sql_template = """
SELECT
  table.id as id,
  table.name as name,
  table.country as country,
  table.country_code as country_code,
  table.region as region,
  table.subregion as subregion,
  years.published_year as published_year,
  years.metrics.total as total,
  funders.name as funder,
  funders.count as count,
  funders.oa as oa,
  funders.gold as gold,
  funders.green as green
FROM `{bq_table}` as table,
  UNNEST(years) as years,
  UNNEST(years.funders) as funders
WHERE
    years.published_year > {year_range[0]} and
    years.published_year < {year_range[1]} and
    funders.count > 20 {scope}
"""

    def clean_data(self):
        helpers.calculate_percentages(self.df,
                                      ['oa', 'green', 'gold'],
                                      'count')
        super().clean_data()


class GenericCitationsTable(AbstractObservatoryTable):
    sql_template = """
SELECT
  table.id as id,
  table.name as name,
  table.country as country,
  table.country_code as country_code,
  table.region as region,
  table.subregion as subregion,
  years.published_year as published_year,
  years.metrics.total as total,
  years.metrics.oa as oa,
  years.metrics.gold as gold,
  years.metrics.green as green,
  years.metrics.hybrid as hybrid,
  years.metrics.total_citations as total_citations,
  years.metrics.articles_with_citations as cited_articles,
  IF(oa_cites.status, oa_cites.citations, NULL) as oa_citations,
  IF(gold_cites.status, gold_cites.citations, NULL) as gold_citations,
  IF(green_cites.status, green_cites.citations, NULL) as green_citations,
  IF(hybrid_cites.status, hybrid_cites.citations, NULL) as hybrid_citations
FROM `{bq_table}` as table,
  UNNEST(years) as years,
  UNNEST(years.citations.oa) as oa_cites,
  UNNEST(years.citations.gold) as gold_cites,
  UNNEST(years.citations.green) as green_cites,
  UNNEST(years.citations.hybrid) as hybrid_cites
WHERE
    years.published_year > {year_range[0]} and
    years.published_year < {year_range[1]} and
    oa_cites.status is True and  
    gold_cites.status is True and
    green_cites.status is True and
    hybrid_cites.status is True {scope}
"""

    def clean_data(self):
        self.df['citations_per_output'] = self.df.total_citations / \
            self.df.total
        self.df['citations_per_oa_output'] = self.df.oa_citations / \
            self.df.oa
        self.df['oa_citation_advantage'] = self.df.citations_per_oa_output / \
            self.df.citations_per_output
        super().clean_data()


class InstitutionReportTable(object):
    bq_table = 'academic-observatory-sandbox.institution.institutions_latest'


class InstitutionOpenAccessTable(InstitutionReportTable, GenericOpenAccessTable):
    pass


class InstitutionOutputsTable(InstitutionReportTable, GenericOutputsTable):
    pass


class InstitutionCitationsTable(InstitutionReportTable, GenericCitationsTable):
    pass


class InstitutionPublishersTable(InstitutionReportTable, GenericPublishersTable):
    pass


class InstitutionJournalsTable(InstitutionReportTable, GenericJournalsTable):
    pass


class InstitutionCollaborationsTable(InstitutionReportTable, GenericCollaborationsTable):
    pass


class InstitutionFundersTable(InstitutionReportTable, GenericFundersTable):
    pass


class FunderReportTable(object):
    bq_table = 'academic-observatory-sandbox.funder.funders_latest'


class FunderOpenAccessTable(FunderReportTable, GenericOpenAccessTable):
    pass


class FunderOutputsTable(FunderReportTable, GenericOutputsTable):
    pass


class FunderCitationsTable(FunderReportTable, GenericCitationsTable):
    pass


class FunderPublishersTable(FunderReportTable, GenericPublishersTable):
    pass


class FunderJournalsTable(FunderReportTable, GenericJournalsTable):
    pass


class FunderCollaborationsTable(FunderReportTable, GenericCollaborationsTable):
    pass


class FunderFundersTable(FunderReportTable, GenericFundersTable):
    pass


class CountryReportTable(object):
    bq_table = 'academic-observatory-sandbox.country.countries_latest'


class CountryOpenAccessTable(CountryReportTable, GenericOpenAccessTable):
    pass


class CountryOutputsTable(CountryReportTable, GenericOutputsTable):
    pass


class CountryCitationsTable(CountryReportTable, GenericCitationsTable):
    pass


class CountryPublishersTable(CountryReportTable, GenericPublishersTable):
    pass


class CountryJournalsTable(CountryReportTable, GenericJournalsTable):
    pass


class CountryCollaborationsTable(CountryReportTable, GenericCollaborationsTable):
    pass


class CountryFundersTable(CountryReportTable, GenericFundersTable):
    pass


class GroupReportTable(object):
    bq_table = 'academic-observatory-sandbox.group.groups_latest'


class GroupOpenAccessTable(GroupReportTable, GenericOpenAccessTable):
    pass


class GroupOutputsTable(GroupReportTable, GenericOutputsTable):
    pass


class GroupCitationsTable(GroupReportTable, GenericCitationsTable):
    pass


class GroupPublishersTable(GroupReportTable, GenericPublishersTable):
    pass


class GroupJournalsTable(GroupReportTable, GenericJournalsTable):
    pass


class GroupCollaborationsTable(GroupReportTable, GenericCollaborationsTable):
    pass


class GroupFundersTable(GroupReportTable, GenericFundersTable):
    pass