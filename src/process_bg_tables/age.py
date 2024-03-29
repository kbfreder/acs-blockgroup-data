
import numpy as np

from .util import (get_column_names_df,
                  process_all_cols_bg_table,
                  process_some_cols_bg_table
                  )
from configs import BG_TABLE_KEY_COL



def _generate_prop_age_column(df, age_range_str, age_ranges_to_add):
    """
    df: pd.DataFrame
        Contains age data
    age_range_str: str
        Age range ('X_Y') to calculate proportion ('prop_age_X_Y') for.
    age_ranges_to_add: list(str)
        List of age ranges to combine for `prop_age` column.
        Ex: ['25 to 29 years', '30 to 34 years'] for '25_34'
    cols_to_keep: list(str)
        Running list of columns to keep at end of processing
    """
    prop_age_col = f'prop_age_{age_range_str}'
    cols_to_add = [f"Total {age_str}" for age_str in age_ranges_to_add]
    
    df[age_range_str] = np.sum(df[cols_to_add], axis=1)
    df[prop_age_col] = df[age_range_str] / df['Total']

    return df, prop_age_col
    

def generate_age_prop_data():
    """
    Generage `prop_age_X_to_Y` columns.
    Also creates `prop_female` column
    """
    tbl_id = 'B01001' # Sex by Age
    col_names_df = get_column_names_df(tbl_id)
    bg_df = process_all_cols_bg_table(tbl_id, col_names_df, 'int')
    bg_df['prop_female'] = bg_df['Female total'] / bg_df['Total'] 
    cols_to_keep = [BG_TABLE_KEY_COL, 'prop_female']
    
    age_range_str_list = col_names_df[col_names_df['Indent'] == 2]['Label'].unique()
    for age_str in age_range_str_list:
        bg_df[f"Total {age_str}"] = (bg_df[f"Male {age_str}"] +
                                                bg_df[f"Female {age_str}"])

    age_range_dict = {
        'under_18': ['Under 5 years', '5 to 9 years', '10 to 14 years', '15 to 17 years'],
        '18_24': ['18 and 19 years', '20 years', '21 years', '22 to 24 years'],
        '18_29': ['18 and 19 years', '20 years', '21 years', '22 to 24 years', '25 to 29 years'],
        '25_34': ['25 to 29 years', '30 to 34 years'],
        '35_44': ['35 to 39 years', '40 to 44 years'],
        '45_54': ['45 to 49 years', '50 to 54 years'],
        '50_64': ['50 to 54 years', '55 to 59 years', '60 and 61 years', '62 to 64 years'],
        '55_64': ['55 to 59 years', '60 and 61 years', '62 to 64 years'],
        '65_69': ['65 and 66 years', '67 to 69 years'],
        '65_74': ['65 and 66 years', '67 to 69 years', '70 to 74 years']
    }

    for age_col, age_ranges in age_range_dict.items():
        bg_df, new_col = _generate_prop_age_column(
            bg_df, age_col, age_ranges)
        cols_to_keep.append(new_col)
    
    return bg_df[cols_to_keep]


def get_median_age():
    bg_df = process_some_cols_bg_table(
        tbl_id='B01002', # Median Age by Sex (Total Population)
        col_idxs=[1], # Total
        new_col_names=['median_age'], 
        dtype='float'
    )
    return bg_df[[BG_TABLE_KEY_COL, 'median_age']]

