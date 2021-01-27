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

# Author: Cameron Neylon & Richard Hosking

from observatory.reports import chart_utils
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
  time_period as published_year,
  table.total_outputs as total_outputs,
  access_types.oa.total_outputs as oa,
  access_types.green.total_outputs as green,
  access_types.gold.total_outputs as gold,
  access_types.hybrid.total_outputs as hybrid,
  access_types.green_only.total_outputs as green_only,
  access_types.gold_doaj.total_outputs as gold_doaj,
  access_types.bronze.total_outputs as bronze,
  access_types.oa.percent as percent_oa,
  access_types.green.percent as percent_green,
  access_types.gold.percent as percent_gold,
  access_types.gold_doaj.percent as percent_gold_just_doaj,
  access_types.green_only.percent as percent_green_only,
  access_types.hybrid.percent as percent_hybrid,
  access_types.bronze.percent as percent_bronze
FROM `{bq_table}` as table
WHERE
    time_period > {year_range[0]} and
    time_period < {year_range[1]} 
    {scope}
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
  table.time_period as published_year,
  table.total_outputs as total_outputs,   
  type.output_type as type,
  type.total_outputs as count,
  type.num_oa_outputs as oa,
  type.num_green_outputs as green,
  type.num_gold_outputs as gold,
  type.num_hybrid_outputs as hybrid,
  type.num_gold_just_doaj_outputs as gold_doaj,
  type.num_bronze_outputs as bronze,
  type.num_green_only_outputs as green_only   
FROM `{bq_table}` as table,
  UNNEST(output_types) as type
WHERE
    time_period > {year_range[0]} and
    time_period < {year_range[1]} and
    type.total_outputs > 3 
    {scope}
"""

    def clean_data(self):
        """Clean data
        """

        chart_utils.calculate_percentages(self.df,
                                          ['oa', 'green', 'gold'],
                                          'total_outputs')
        chart_utils.clean_output_type_names(self.df)
        super().clean_data()


class GenericCollaborationsTable(AbstractObservatoryTable):
    """Generic table class for Collaborations
    """

    sql_template = """
SELECT
  table.id as id,
  table.name as name,
  table.country as country,
  table.country_code as country_code,
  table.time_period as published_year,
  table.total_outputs as total_outputs,
  collabs.id as collaborator_id,
  collabs.total_outputs as count,
  collabs.name as collaborator_name,
  collabs.country as collaborator_country,
  collabs.country_code as collaborator_country_code,
  collabs.region as collaborator_region,
  collabs.coordinates as collaborator_coordinates,
  collabs.num_oa_outputs as oa,
  collabs.num_gold_outputs as gold,
  collabs.num_hybrid_outputs as hybrid,
  collabs.num_bronze_outputs as bronze,
  collabs.num_green_outputs as green,
  collabs.num_green_only_outputs as green_only,
  collabs.num_gold_just_doaj_outputs as gold_doaj
FROM `{bq_table}` as table,
  UNNEST(collaborations) as collabs
WHERE
    time_period > {year_range[0]} and
    time_period < {year_range[1]} and
    collabs.total_outputs > 5 
    {scope}
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
  time_period as published_year,
  table.total_outputs as total_outputs,
  publishers.publisher as publisher,
  publishers.total_outputs as count,
  publishers.num_oa_outputs as oa,
  publishers.num_gold_outputs as gold,
  publishers.num_hybrid_outputs as hybrid,
  publishers.num_bronze_outputs as bronze,
  publishers.num_green_outputs as green
FROM `{bq_table}` as table,
  UNNEST(publishers) as publishers
WHERE
    time_period > {year_range[0]} and
    time_period < {year_range[1]} and
    publishers.total_outputs > 5 
    {scope}
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
    time_period > {year_range[0]} and
    time_period < {year_range[1]} and
    journals.total_outputs > 1 
    {scope}
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
  table.time_period as published_year,
  table.total_outputs as total_outputs,
  funders.name as funder,
  funders.country as funder_country,
  funders.country_code as funder_country_code,
  funders.funding_body_type as funder_type,
  funders.total_outputs as count,
  funders.num_oa_outputs as oa,
  funders.num_gold_outputs as gold,
  funders.num_hybrid_outputs as hybrid,
  funders.num_bronze_outputs as bronze,
  funders.num_green_outputs as green,
  funders.num_green_only_outputs as green_only,
  funders.num_gold_just_doaj_outputs as gold_doaj
FROM `{bq_table}` as table,
  UNNEST(funders) as funders
WHERE
    time_period > {year_range[0]} and
    time_period < {year_range[1]} and
    funders.total_outputs > 10 
    {scope}
"""

    def clean_data(self):
        """Clean data
        """

        chart_utils.calculate_percentages(self.df,
                                          ['oa', 'green', 'gold'],
                                          'count')
        self.df['Funder Type'] = self.df.funder_type.map(chart_utils.funder_type_clean['funder_type'])
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
