import pandas as pd
import numpy as np
import pydata_google_auth

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

    #TODO see if this function can be generalised
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
    #TODO this function can be generalised
    SCOPES = [
        'https://www.googleapis.com/auth/cloud-platform',
        'https://www.googleapis.com/auth/drive',
    ]

    credentials = pydata_google_auth.get_user_credentials(
        SCOPES,
    )
    return credentials