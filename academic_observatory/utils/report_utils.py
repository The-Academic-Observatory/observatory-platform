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
import numpy as np
import pydata_google_auth

from num2words import num2words

comptext_larger = [
    'substantialy lower than',
    'somewhat lower than',
    'approximately equal to',
    'somewhat larger than',
    'substantially larger than'
]

comptext_bigness = [
    'a very small',
    'a smaller than average',
    'an average',
    'a larger than average',
    'a very large'
]

comptext_quartiles = [
    'fourth quartile',
    'third quartile',
    'second quartile',
    'first quartile'
]

comptext_higherlower = [
    'lower than',
    'roughly the same as',
    'higher than'
]

# General BQ Defaults
project_id = 'academic-observatory-sandbox'
scope = ''
funder_scope = ''
institutions_scope = """and
  institutions.id in (SELECT id FROM `open-knowledge-publications.institutional_oa_evaluation_2020.grids_in_scope`)"""


def create_new_report(dir):
    """ Create a new blank report in the specified directory.

    :param dir: the target directory.
    """
    a = 1


def execute_report():
    """ Execute the report in the specified directory.

    :param dir: the target directory.
    """
    a = 2


def generate_table_data(batch,
                        title,
                        df: pd.DataFrame,
                        identifier: str,
                        columns: list,
                        identifier_column: str = 'id',
                        sort_column: str = 'Year of Publication',
                        sort_ascending: bool = True,
                        decimals: int = 0,
                        short_column_names: list = None,
                        column_alignments=None) -> dict:

    table_data = pd.DataFrame()
    df = df[df[identifier_column] == identifier]
    df.sort_values('Year of Publication', inplace=True)
    for i, column in enumerate(columns):
        col_data = df[column]
        if short_column_names:
            column = short_column_names[i]
        if col_data.dtype == 'float64':
            col_data = np.int_(col_data.round(decimals=decimals))
        table_data[column] = col_data
    table_data.sort_values(sort_column, ascending=sort_ascending, inplace=True)
    table_data_list = table_data.to_dict(orient='records')

    if short_column_names:
        columns = short_column_names
    if not column_alignments:
        column_alignments = ['center'] * len(columns)
    column_list = [{'name': name, 'alignment': alignment}
                   for name, alignment in zip(columns, column_alignments)]
    return {'title': title,
            'columns': column_list,
            'rows': table_data_list}


def get_gcp_credentials():
    SCOPES = [
        'https://www.googleapis.com/auth/cloud-platform',
        'https://www.googleapis.com/auth/drive',
    ]

    credentials = pydata_google_auth.get_user_credentials(
        SCOPES,
    )
    return credentials


def close_comparators(df: pd.DataFrame,
                      identifier: str,
                      focus_year: int,
                      number: int = 5,
                      variables: list = [],
                      filter_column: str = 'country') -> list:
    """Generate a comparison group based on an identifier
    :param df: DataFrame containing COKI standard data
    :type df: pd.DataFrame
    :param identifier: Identifier for the organisation of interest
    :type identifier: str
    :param focus_year: The year to do the comparison for
    :type focus_year: int
    :param number: Number of orgs in the comparison group, defaults to 5
    :type number: int, optional
    :param variables: List of columns to use as comparison, defaults to []
    :type variables: list, optional
    :param filter_column: Name of column to use as filter to restrict the
    search for similar organisations, defaults to 'country'
    :type filter_column: str, optional
    """

    if filter_column:
        same = df[df.id == identifier][filter_column].unique()[0]
        comparison_data = df[(df[filter_column] == same) &
                             (df.published_year == focus_year)]
    else:
        comparison_data = df[df.published_year == focus_year]

    unique_ids = comparison_data.id.unique()
    comparison_data = comparison_data.set_index('id')

    #unique_years = comparison_data.published_year.unique()

    comptable = pd.DataFrame(index=unique_ids)
    for variable in variables:
        org_value = comparison_data.loc[identifier, variable]
        scale = comparison_data[variable].max()
        comptable[variable] = (
            (comparison_data[variable] - org_value)/scale)**2
    comptable['sum'] = comptable.sum(axis=1)
    comptable['diff'] = (comptable.sum())**2
    comptable.sort_values('sum', inplace=True, ascending=True)

    return list(comptable[0:number+1].index.values)


def get_biggest(df: pd.DataFrame,
                identifier: str,
                focus_year: int,
                total_column: str = 'total',
                filter_column: str = 'country') -> str:
    """Provide the identifier for the biggest org in the dataset within the filtergroup
    Generally used to identify the largest university in the country as part
    of a comparison set for reports.
    :param df: DataFrame containing report data
    :type df: pd.DataFrame
    :param focus_year: The year for defining the largest organisation
    :type focus_year: int
    :param total_column: The column in which to find the values to check for the
    'largest' organisation, defaults to 'total'
    :type total_column: str, optional
    :param filter_column: Optional filter to define the set of organisations to
    search amongst for the largets, defaults to 'country'
    :type filter_column: str, optional
    :return: Provides the id of the largest organisations, generally a grid_id
    :rtype: str
    """

    if filter_column:
        same = df[df.id == identifier][filter_column].unique()[0]
        comparison_data = df[(df[filter_column] == same) &
                             (df.published_year == focus_year)]
    else:
        comparison_data = df[df.published_year == focus_year]
    comparison_data = comparison_data.set_index('id')

    return comparison_data[total_column].idxmax()


def generate_comparison_group(df: pd.DataFrame,
                              identifier: str,
                              focus_year: int,
                              variables: list = ['total', 'percent_total_oa', 'percent_green'],
                              include_biggest: bool = True,
                              include_topoa: bool = True,
                              number: int = 5,
                              filter_column: str = 'country',
                              average=pd.Series.median) -> list:
    """Convenience function for generating a comparison group
    :param df: DataFrame for analysis
    :type df: pd.DataFrame
    :param identifier: Identifier for the report
    :type identifier: str
    :param focus_year: Year for focus of the comparison to generate group
    :type focus_year: int
    :param number: Length of the comparison group to return. Overall length
    :type number: int
    :param variables: Variables to do the comparison, defaults to ['total', 'percent_total_oa', 'percent_green']
    :type variables: list, optional
    :param include_biggest: Include the largest org found within the filtered group, defaults to True
    :type include_biggest: bool, optional
    :param include_topoa: Include org with highest overall OA levels within the filtered group, defaults to True
    :type include_topoa: bool, optional
    :param filter_column: Column to use to filter down the set of orgs to compare, defaults to 'country'
    :type filter_column: str, optional
    :param average: Function to use to calculate the average, passed to close_comparators, defaults to pd.Series.median
    :type average: A function that can be called on a pandas Series object an returns a single float or int, optional
    :return: A list of identifers that make up a comparison group
    :rtype: list
    """

    # Fix this hack at some point by chasing through the column name
    if 'percent_total_oa' not in df.columns:
        df['percent_total_oa'] = df.percent_oa
    comparators = close_comparators(
        df, identifier, focus_year, number=number, 
        variables=variables, filter_column=filter_column)

    extras = []
    if include_biggest:
        biggest = get_biggest(df, identifier, focus_year,
                              total_column='total', filter_column=filter_column)
        comparators.pop(len(comparators)-1)
        extras.append(biggest)

    if include_topoa:
        try:
            topoa = get_biggest(df, identifier, focus_year,
                            total_column='percent_total_oa', filter_column=filter_column)
        except KeyError: # changing over to 'percent_oa' as preferred name
            topoa = get_biggest(df, identifier, focus_year,
                            total_column='percent_oa', filter_column=filter_column)
        comparators.pop(len(comparators)-1)
        extras.append(topoa)
    return comparators + extras


def general_text_comparison(df: pd.DataFrame,
                            identifier: str,
                            focus_year: int,
                            column: str,
                            filter_column: str = 'country',
                            average=pd.Series.median,
                            output: list = defaults.comptext_larger) -> str:
    """Generate a basic comparison on one variable
    Compares the value for `identifier` to the median and variance of the other
    organisations where `filter_column` == `filter_value`
    :param df: DataFrame containing report data
    :type df: pd.DataFrame
    :param focus_year: The year for calculating the comparison
    :type focus_year: int
    :param total_column: The column in which to find the values to check for the
    comparison of values
    :type total_column: str
    :param filter_column: Optional filter to define the set of organisations to
    search amongst for the comparison, defaults to 'country'
    :type filter_column: str, optional
    :param average: Function to use to calculate the averave over the column, defaults
    to median
    :type average: a callable on a pd.Series
    :return: A string of the form "{{variance_term}} {{smaller/larger}} than the median"
    :rtype: str
    """

    if filter_column:
        same = df[df.id == identifier][filter_column].unique()[0]
        comparison_data = df[(df[filter_column] == same) &
                             (df.published_year == focus_year)]
    else:
        comparison_data = df[df.published_year == focus_year]
    comparison_data = comparison_data.set_index('id')
    average = average(comparison_data[column])
    std = comparison_data[column].std()
    org_value = comparison_data.loc[identifier, column]
    deviation = (org_value - average) / std

    if (output is None) or (output == []):
        return deviation
    if deviation < -1:
        return output[0]
    elif deviation < -0.15:
        return output[1]
    elif deviation < 0.15:
        return output[2]
    elif deviation < 1:
        return output[3]
    else:
        return output[4]


def is_ranked(df: pd.DataFrame,
              identifier: str,
              focus_year: int,
              column: str,
              filter_column: str = 'country',
              rank_kwargs: dict = {'ascending': False},
              do_num2words: bool = True,
              num2words_kwargs: dict = {'to': 'ordinal'},
              verbose: bool = False):
    """Return the ranking for an org for a given column
    :param df: DataFrame containing report data
    :type df: pd.DataFrame
    :param focus_year: The year for defining the ranking position
    :type focus_year: int
    :param column: The column in which to find the values to calculate the rank
    :type total_column: str, optional
    :param filter_column: Optional filter to define the set of organisations to
    search amongst for the largets, defaults to 'country'
    :type filter_column: str, optional
    :return: The rank
    :rtype: int or float
    """

    try:
        df.sort_values(column, inplace=True, ascending=False)
    except KeyError: #switching from percent_total_oa to percent_oa
        return None
    if filter_column:
        same = df[df.id == identifier][filter_column].unique()[0]
        comparison_data = df[(df[filter_column] == same) &
                             (df.published_year == focus_year)]
    else:
        comparison_data = df[df.published_year == focus_year]
        same = 'the world'
    comparison_data = comparison_data.set_index('id')

    comparison_data.sort_values(column, inplace=True, ascending=True)
    comparison_data['rank'] = comparison_data[column].rank(**rank_kwargs)
    rank = comparison_data.loc[identifier, 'rank']
    if not do_num2words:
        return rank

    if rank_kwargs.get('pct', False):
        text = f"""{num2words(int(rank*100), **num2words_kwargs)} percentile"""
    else:
        text = num2words(rank, **num2words_kwargs)
    if verbose:
        return f'{text} in {same} for {focus_year}'
    else:
        return text


def generate_highlights(df: pd.DataFrame,
                        identifier: str,
                        focus_year: int,
                        number: int = 5, 
                        filter_column='country',
                        measures = ['percent_total_oa', 'percent_gold', 'percent_green',
                'citations_per_output', 'oa_citation_advantage', 'total']):

    nice_text = {
        'percent_total_oa': 'overall percentage of open access', 
        'percent_oa' : 'overall percentage of open access',
        'percent_gold': 'proportion of outputs published open access', 
        'percent_green': 'proportion of outputs available through a repository',
        'citations_per_output': 'average citations per output', 
        'oa_citation_advantage': 'increase in citations for open access outputs', 
        'total': 'overall output count'
    }
    highlights = []
    for measure in measures:
        rank = is_ranked(df, identifier, focus_year, measure, None, do_num2words=False)
        if not rank:
            continue
        if rank < 201:
            highlights.append({'type': 'Ranked',
                               'measure' : measure,
                               'scope': 'in the world',
                               'value': num2words(rank, to='ordinal')})

        rank = is_ranked(df, identifier, focus_year, measure, filter_column, do_num2words=False)
        if rank < 26 and (filter_column is not None):
            highlights.append({'type': 'Ranked',
                               'measure' : measure,
                               'scope': f'in the {filter_column}',
                               'value': num2words(rank, to='ordinal')})

        position = general_text_comparison(df, identifier, focus_year, measure, None)
        if position in ['somewhat larger than', 'substantially larger than']:
            highlights.append({'type': 'Has',
                               'measure' : measure,
                               'scope': 'the global average',
                               'value': position})
        
        position = general_text_comparison(df, identifier, focus_year, measure, filter_column)
        if position in ['somewhat larger than', 'substantially larger than'] and (
            filter_column is not None):
            highlights.append({'type': 'Has',
                               'measure' : measure,
                               'scope': f'the average for the {filter_column}',
                               'value': position})

    highlight_strings = []
    for highlight in highlights:
        string = f"""{highlight['type']} {highlight['value']} {highlight['scope']} on {nice_text[highlight['measure']]}"""
        string = string.capitalize()
        highlight_strings.append(string)
    return highlight_strings