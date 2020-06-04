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

import pandas as pd
from inspect import signature
from academic_observatory.analysis.defaults import country_clean, outputs_clean


# --- TODO: cleanup required, mainly on country names #
def clean_geo_names(df: pd.DataFrame) -> pd.DataFrame:
    """Convenience function for standardising country names
    The input country names can be quite long and for our purposes we want to
    separate Mexico from the rest of North America and treat it as part of
    Latin America. This function cleans up and shortens some specific country
    names (which are defined in the `country_clean` dict in the defaults
    module) and the region names.
    :param df: Input data frame to be cleaned up
    :type df: pandas DataFrame
    :return: DataFrame with country and region  names cleaned up.
    :rtype: pandas DataFrame
    """

    df.replace(to_replace=country_clean, inplace=True)

    df.loc[df.country.isin(
        ['Canada', 'United States of America']), 'region'] = 'North America'
    df.replace('Americas', 'Latin America', inplace=True)
    return df


def clean_output_type_names(df: pd.DataFrame) -> pd.DataFrame:
    """Convenience function for cleaning up output type names
    The `outputs_clean` dict is located in the defaults submodule
    :param df: Input data frame to be cleaned up
    :type df: pandas DataFrame
    :return: DataFrame with output type names cleaned up.
    :rtype: pandas DataFrame
    """
    df.replace(to_replace=outputs_clean, inplace=True)
    return df


def nice_column_names(df):
    """Convenience function to convert standard names from BigQuery to nice names for plotting"""

    cols = [
        ('Open Access (%)', 'percent_OA'),
        ('Open Access (%)', 'percent_oa'),
        ('Open Access (%)', 'percent_total_oa'),
        ('Total Green OA (%)', 'percent_green'),
        ('Total Gold OA (%)', 'percent_gold'),
        ('Gold in DOAJ (%)', 'percent_gold_just_doaj'),
        ('Hybrid OA (%)', 'percent_hybrid'),
        ('Bronze (%)', 'percent_bronze'),
        ('Total Green OA (%)', 'percent_green'),
        ('Green Only (%)', 'percent_green_only'),
        ('Green in IR (%)',
         'percent_green_in_home_repo'),
        ('Total Publications', 'total'),
        ('Change in Open Access (%)', 'total_oa_pc_change'),
        ('Change in Green OA (%)', 'green_pc_change'),
        ('Change in Gold OA (%)', 'gold_pc_change'),
        ('Change in Total Publications (%)', 'total_pc_change'),
        ('Year of Publication', 'published_year'),
        ('University Name', 'name'),
        ('Region', 'region'),
        ('Country', 'country'),
        ('Citation Count', 'total_citations'),
        ('Cited Articles', 'cited_articles'),
        ('Citations to OA Outputs', 'oa_citations'),
        ('Citations to Gold Outputs', 'gold_citations'),
        ('Citations to Green Outputs', 'green_citations'),
        ('Citations to Hybrid Outputs', 'hybrid_citations'),
        ('Total Outputs', 'total'),
        ('Journal Articles', 'journal_articles'),
        ('Proceedings', 'proceedings_articles'),
        ('Books', 'authored_books'),
        ('Book Sections', 'book_sections'),
        ('Edited Volumes', 'edited_volumes'),
        ('Reports‡', 'reports'),
        ('Datasets‡', 'datasets')
    ]
    for col in cols:
        if col[1] in df.columns.values:
            df[col[0]] = df[col[1]]

    return df


def calculate_pc_change(df: pd.DataFrame,
                        columns: list,
                        id_column: str = 'id',
                        year_column: str = 'published_year',
                        column_name_add: str = '_pc_change') -> pd.DataFrame:
    """Convenience function for creating a new column giving percentage change
    For some graphs we want to show change vs the previous year. This
    function takes a set of columns which must be numeric and
    creates a new column that gives the percentage change for each year
    from the previous year.
    :param df: Input DataFrame with the data to be analysed
    :type df: pd.DataFrame
    :param columns: A list of strings with the names of the columns to be
    analysed
    :type columns: list
    :param id_column: The name of the column containing object IDs, defaults
    to 'id'
    :type id_column: str, optional
    :param year_column: The name of the column containing year, defaults to
    'published_year'
    :type year_column: str, optional
    :param column_name_add: String to add to the column names for new columns,
    defaults to '_pc_change'
    :type column_name_add: str, optional
    :return: DataFrame containing new columns with percent change year on year
    :rtype: pd.DataFrame
    """

    df = df.sort_values(year_column, ascending=True)
    for column in columns:
        new_column_name = column + column_name_add
        df[new_column_name] = list(df.groupby(
            id_column)[column].pct_change()*100)
    return df


def calculate_percentages(df: pd.DataFrame,
                          numer_columns: list,
                          denom_column: str = 'total',
                          column_name_add: str = 'percent_') -> pd.DataFrame:
    """Convenience function to calculate percentages based on counts data
    By default the core COKI tables have counts and not percentages. This
    convenience function calculates percentages for a set of columns based on a
    single denominator column. The most common use case for this is calculating
    open access percentages for several types of OA based on the counts of each
    type and the total outputs count.
    :param df: Input DataFrame with data to be analysed
    :type df: pd.DataFrame
    :param numer_columns: A list of strings containing column names for
    numerator
    :type numer_columns: list
    :param denom_column: A single column name giving the denominator for
    percentages
    :type denom_column: str
    :param column_name_add: Text to add to numberator column name to give the
    new column name, defaults to 'percent_'
    :type column_name_add: str, optional
    :return: DataFrame with new columns containing the percentage calculations
    :rtype: pd.DataFrame
    """

    for column in numer_columns:
        pc_column_name = column_name_add + column
        df[pc_column_name] = df[column] / df[denom_column] * 100

    return df


# --- TODO: Check that this calculation is up to date with our standard practise
def calculate_confidence_interval(df: pd.DataFrame,
                                  columns: list,
                                  total_column: str = 'total',
                                  column_name_add: str = '_err'
                                  ) -> pd.DataFrame:
    """Convenience function for calculating confidence intervals
    For various graphs we calculate a confidence interval as described in
    Huang et al 2020. This function centralises that calculation
    :param df: Input DataFrame with data to be analysed
    :type df: pd.DataFrame
    :param columns: List of column names for CI to be calculated
    :type columns: list
    :param total_column: Column containing the total counts for CI calculation,
    defaults to 'total'
    :type total_column: str, optional
    :param column_name_add: Text to add to input columns to give new column
    names, defaults to '_err'
    :type column_name_add: str, optional
    :return: DataFrame containing new columns with the CI calculated
    :rtype: pd.DataFrame
    """

    for column in columns:
        new_column_name = column + column_name_add
        df[new_column_name] = 100*1.96*(
            df[column] / 100 *
            (
                1 - df[column] / 100
            ) /
            df[total_column]
        )**(.5)
    return df


def _collect_kwargs_for(func: callable,
                        input_kwargs: dict) -> dict:
    """Convenience function for collecting keywords for functions
    As we are manipulating kwargs for axes, figures and other functions it
    is sometimes useful to pass a larger set of kwargs around and then select
    the set relevant for a specific class or function.
    :param func: Callable that we want to identify input args for
    :type func: callable
    :param input_kwargs: The full set of args that we want to check through.
    Relevant kwargs are removed as a side effect
    :type input_kwargs: dict
    :return: Dict containing the subset of kwargs that are recognised for the
    callable
    :rtype: dict
    """

    sig = signature(func)
    names = [parameter for parameter in sig.parameters]
    kwargs = {k: input_kwargs.pop(k) for k in input_kwargs.keys() & names}
    return kwargs


def id2name(df: pd.DataFrame,
            identifier: str) -> str:
    """Convenience function to generate the name of an entity from its identifier
    :param df: Input DataFrame
    :type df: pd.DataFrame
    :param identifier: A str containing a COKI relevant id (eg a GRID)
    :type identifier: str
    :return: Returns the name associated with that identifier as a str
    :rtype: str
    """
    return df[df.id == identifier].name.unique()[0]
