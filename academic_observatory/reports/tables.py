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

from academic_observatory.reports import chart_utils
from .abstract_table import AbstractObservatoryTable


class GenericOpenAccessTable(AbstractObservatoryTable):
    """Generic table class for OpenAccess
    """

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
    """Generic table class for Outputs
    """

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
        """Clean data
        """

        chart_utils.calculate_percentages(self.df,
                                          ['oa', 'green', 'gold'],
                                          'total')
        chart_utils.clean_output_type_names(self.df)
        super().clean_data()


class GenericCollaborationsTable(AbstractObservatoryTable):
    """Generic table class for Collaborations
    """

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
    """Generic table class for Publishers
    """

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
        """Clean data
        """

        chart_utils.calculate_percentages(self.df,
                                          ['oa', 'green', 'gold'],
                                          'count')
        super().clean_data()


class GenericJournalsTable(AbstractObservatoryTable):
    """Generic table class for Journals
    """

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
        """Clean data
        """

        chart_utils.calculate_percentages(self.df,
                                          ['oa', 'green', 'gold'],
                                          'count')
        super().clean_data()


class GenericFundersTable(AbstractObservatoryTable):
    """Generic table class for Funders
    """

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
        """Clean data
        """

        chart_utils.calculate_percentages(self.df,
                                          ['oa', 'green', 'gold'],
                                          'count')
        super().clean_data()


class GenericCitationsTable(AbstractObservatoryTable):
    """Generic table class for Citations
    """

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
        """Clean data
        """

        self.df['citations_per_output'] = self.df.total_citations / self.df.total
        self.df['citations_per_oa_output'] = self.df.oa_citations / self.df.oa
        self.df['oa_citation_advantage'] = self.df.citations_per_oa_output / self.df.citations_per_output
        super().clean_data()


class InstitutionReportTable(object):
    """Table class for Institution Reports
    """

    bq_table = 'academic-observatory-sandbox.institution.institutions_latest'


class InstitutionOpenAccessTable(InstitutionReportTable, GenericOpenAccessTable):
    """Table class for Institution Open Access
    """

    pass


class InstitutionOutputsTable(InstitutionReportTable, GenericOutputsTable):
    """Table class for Institution Outputs
    """

    pass


class InstitutionCitationsTable(InstitutionReportTable, GenericCitationsTable):
    """Table class for Institution Citations
    """

    pass


class InstitutionPublishersTable(InstitutionReportTable, GenericPublishersTable):
    """Table class for Institution Publishers
    """

    pass


class InstitutionJournalsTable(InstitutionReportTable, GenericJournalsTable):
    """Table class for Institution Journals
    """

    pass


class InstitutionCollaborationsTable(InstitutionReportTable, GenericCollaborationsTable):
    """Table class for Institution Collaborations
    """

    pass


class InstitutionFundersTable(InstitutionReportTable, GenericFundersTable):
    """Table class for Institution Funders
    """

    pass


class FunderReportTable(object):
    """Table class for Funders Reports
    """

    bq_table = 'academic-observatory-sandbox.funder.funders_latest'


class FunderOpenAccessTable(FunderReportTable, GenericOpenAccessTable):
    """Table class for Funders Open Access
    """

    pass


class FunderOutputsTable(FunderReportTable, GenericOutputsTable):
    """Table class for Funders Outputs
    """

    pass


class FunderCitationsTable(FunderReportTable, GenericCitationsTable):
    """Table class for Funders Citations
    """

    pass


class FunderPublishersTable(FunderReportTable, GenericPublishersTable):
    """Table class for Funders Publishers
    """

    pass


class FunderJournalsTable(FunderReportTable, GenericJournalsTable):
    """Table class for Funders Journals
    """

    pass


class FunderCollaborationsTable(FunderReportTable, GenericCollaborationsTable):
    """Table class for Funders Collaborations
    """

    pass


class FunderFundersTable(FunderReportTable, GenericFundersTable):
    """Table class for Funders
    """

    pass


class CountryReportTable(object):
    """Table class for Country Reports
    """

    bq_table = 'academic-observatory-sandbox.country.countries_latest'


class CountryOpenAccessTable(CountryReportTable, GenericOpenAccessTable):
    """Table class for Country Open Access
    """

    pass


class CountryOutputsTable(CountryReportTable, GenericOutputsTable):
    """Table class for Country Outputs
    """

    pass


class CountryCitationsTable(CountryReportTable, GenericCitationsTable):
    """Table class for Country Citations
    """

    pass


class CountryPublishersTable(CountryReportTable, GenericPublishersTable):
    """Table class for Country Publishers
    """

    pass


class CountryJournalsTable(CountryReportTable, GenericJournalsTable):
    """Table class for Country Journals
    """

    pass


class CountryCollaborationsTable(CountryReportTable, GenericCollaborationsTable):
    """Table class for Country Collaborations
    """

    pass


class CountryFundersTable(CountryReportTable, GenericFundersTable):
    """Table class for Country Funders
    """

    pass


class GroupReportTable(object):
    """Table class for Group Reports
    """

    bq_table = 'academic-observatory-sandbox.group.groups_latest'


class GroupOpenAccessTable(GroupReportTable, GenericOpenAccessTable):
    """Table class for Group Open Access
    """

    pass


class GroupOutputsTable(GroupReportTable, GenericOutputsTable):
    """Table class for Group Outputs
    """

    pass


class GroupCitationsTable(GroupReportTable, GenericCitationsTable):
    """Table class for Group Citations
    """

    pass


class GroupPublishersTable(GroupReportTable, GenericPublishersTable):
    """Table class for Group Publishers
    """

    pass


class GroupJournalsTable(GroupReportTable, GenericJournalsTable):
    """Table class for Group Journals
    """

    pass


class GroupCollaborationsTable(GroupReportTable, GenericCollaborationsTable):
    """Table class for Group Collaborations
    """

    pass


class GroupFundersTable(GroupReportTable, GenericFundersTable):
    """Table class for Group Funders
    """

    pass
