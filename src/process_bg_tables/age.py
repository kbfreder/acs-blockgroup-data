# TODO: revamp this to only return df with pertinent coulmns. See households.py or misc_pop.py


import os, sys

import pandas as pd
from fuzzywuzzy import fuzz
from numbers import Number

import numpy as np
import re
import pickle

from util import (load_table, 
                  get_column_names,
                  process_all_cols_bg_table,
                  process_single_col_bg_table
                  )




# df = load_table(tbl_id)
# col_names_df = get_column_names(tbl_id)
# cols_to_keep = list(summary_df.columns)


def _generate_prop_age_column(df, new_age_col, age_ranges_to_add, cols_to_keep):
    """
    df: pd.DataFrame
        Contains age data
    new_age_col: str
        Stub of new `prop_age` column to create
    age_ranges_to_add: list(str)
        List of age ranges to combine for `prop_age` column.
        Ex: ['25 to 29 years', '30 to 34 years'] for '25_34'
    cols_to_keep: list(str)
        Running list of columns to keep at end of processing
    """
    prop_age_col = f'prop_age_{new_age_col}'
    cols_to_add = [f"Total {age_str}" for age_str in age_ranges_to_add]
    
    df[new_age_col] = np.sum(df[cols_to_add], axis=1)
    df[prop_age_col] = df[new_age_col] / df['Total']
    cols_to_keep.append(prop_age_col)
    return df, cols_to_keep
    

def generate_age_prop_data():
    """
    Generage `prop_age_X_to_Y` columns.
    Also creates `prop_female` column
    """
    tbl_id = 'B01001'
    col_names_df = get_column_names(tbl_id)
    bg_df = process_all_cols_bg_table(tbl_id, 'int')
    bg_df['prop_female'] = bg_df['Female total'] / bg_df['Total'] 
    cols_to_keep = ['prop_female']
    
    age_range_str_list = col_names_df[col_names_df['Indent'] == 2]['Label'].unique()
    for age_str in age_range_str_list:
        bg_df[f"Total {age_str}"] = (bg_df[f"Male {age_str}"] +
                                                bg_df[f"Female {age_str}"])

    age_range_dict = {
        'under_18': ['Under 5 years', '5 to 9 years', '10 to 14 years', '15 to 17 years'],
        '18_24': ['18 and 19 years', '20 years', '21 years', '22 to 24 years'],
        '18_29': ['18 and 19 years', '20 years', '21 years',
                  '22 to 24 years', '25 to 29 years'],
        '25_34': ['25 to 29 years', '30 to 34 years'],
        '35_44': ['35 to 39 years', '40 to 44 years'],
        '45_54': ['45 to 49 years', '50 to 54 years'],
        '50_64': ['50 to 54 years', '55 to 59 years', '60 and 61 years', '62 to 64 years'],
        '55_64': ['55 to 59 years', '60 and 61 years', '62 to 64 years'],
        '65_69': ['65 and 66 years', '67 to 69 years'],
        '65_74': ['65 and 66 years', '67 to 69 years', '70 to 74 years']
    }

    for age_col, age_ranges in age_range_dict.items():
        bg_df, cols_to_keep = _generate_prop_age_column(
            bg_df, age_col, age_ranges, cols_to_keep)
    
    return bg_df, cols_to_keep


def get_median_age():
    df = process_single_col_bg_table('B01002', 1, 'median_age', 'float')
    return df, ['median_age']


def get_all_age_data():
    df1, cols_to_keep1 = generate_age_prop_data()
    df2, cols_to_keep2 = get_median_age()
    
    df = df1[cols_to_keep1 + ['GEO_ID']].merge(
        df2[cols_to_keep2 + ['GEO_ID']], 
        on='GEO_ID')
    cols_to_keep = cols_to_keep1 + cols_to_keep2

    return df, cols_to_keep


# if __name__ == "__main__":
#     process_all_age_data()
