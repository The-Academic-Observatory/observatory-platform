import pandas as pd
from inspect import signature


# Data cleanup required, mainly on country names #
def clean_geo_names(df):
    country_clean = {"country": {
        "United Kingdom of Great Britain and Northern Ireland":
            "United Kingdom",
        "Iran (Islamic Republic of)": "Iran",
        "Korea, Republic of": "South Korea",
        "Taiwan, Province of China": "Taiwan"
    }
    }
    df.replace(to_replace=country_clean, inplace=True)

    df.loc[df.country.isin(
        ['Canada', 'United States of America']), 'region'] = 'North America'
    df.replace('Americas', 'Latin America', inplace=True)
    return df


def clean_output_type_names(df):
    outputs_clean = {'type': {
                     'total': 'Total Outputs',
                     'journal_articles': 'Journal Articles',
                     'proceedings_articles': 'Proceedings',
                     'authored_books': 'Books',
                     'book_sections': 'Book Sections',
                     'edited_volumes': 'Edited Volumes',
                     'reports': 'Reports‡',
                     'datasets': 'Datasets‡'
                     }}
    df.replace(to_replace=outputs_clean, inplace=True)
    return df

# Creating nice column names for graphing


def nice_column_names(df):
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


def calculate_pc_change(df, columns,
                        id_column='id',
                        year_column='published_year',
                        column_name_add='_pc_change') -> pd.DataFrame:
    """Function for creating percent_changes year on year
    """

    df = df.sort_values(year_column, ascending=True)
    for column in columns:
        new_column_name = column + column_name_add
        df[new_column_name] = list(df.groupby(
            id_column)[column].pct_change()*100)
    return df


def calculate_percentages(df: pd.DataFrame,
                          numer_columns: list,
                          denom_column: str,
                          column_name_add: str = 'percent_') -> pd.DataFrame:
    """Calculate Percentages from Specific Columns
    """

    for column in numer_columns:
        pc_column_name = column_name_add + column
        df[pc_column_name] = df[column] / df[denom_column] * 100

    return df


def calculate_confidence_interval(df, columns,
                                  total_column='total',
                                  column_name_add='_err') -> pd.DataFrame:
    """Function for calculating confidence intervals
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


def _collect_kwargs_for(func,
                        input_kwargs: dict):
    """Convenience function for collecting keywords for functions

    param: func: a callable, will raise a TypeError or ValueError if a
                 a signature cannot be identified.
    param: input_kwargs: input set of keywords from which the correct ones
                         for the callable should be extracted
    returns: kwargs: <dict> with keywords for the callable and pops the
                     relevant keys and values from the input dictionary
    """

    sig = signature(func)
    names = [parameter for parameter in sig.parameters]
    kwargs = {k: input_kwargs.pop(k) for k in input_kwargs.keys() & names}
    return kwargs


def id2name(df, identifier):
    return df[df.id == identifier].name.unique()[0]
