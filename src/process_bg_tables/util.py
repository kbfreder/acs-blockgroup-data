import re
import pickle

import numpy as np
import pandas as pd

from configs import BG_TABLE_KEY_COL

pd.set_option('mode.chained_assignment', None)


def get_column_names(tbl_id, shell_df):
    return shell_df[shell_df['Table ID'] == tbl_id.upper()]


# LOAD LOCAL DATA TABLES

BG_DATA_PATH = "../../data/sumlevel=150/"
YEAR = 2022
DATASET = 5

def load_table(table_id):
    file_path = f"{BG_DATA_PATH}acsdt{DATASET}y{YEAR}-{table_id.lower()}.dat"
    try:
        df = pd.read_csv(file_path, sep="|", dtype="str")
    except FileNotFoundError:
        print("File not found!", file_path)
        df = None
    return df


# LOAD/SAVE SUMMARY TABLE
def load_summary_df(checkpoint_name):
    file_stub = f"../data/parsed_acs_data/{checkpoint_name}"
    with open(f"{file_stub}.pkl", "rb") as f:
        dtype_dict = pickle.load(f)
    summary_df = pd.read_csv(f"{file_stub}.csv", dtype=dtype_dict)
    return summary_df

def save_summary_df(summary_df, save_name):
    file_stub = f"../data/parsed_acs_data/{save_name}"
    dtype_dict = summary_df.dtypes.apply(lambda x: x.name).to_dict()
    summary_df.to_csv(f"{file_stub}.csv", index=False)
    with open(f"{file_stub}.pkl", "wb") as pf:
        pickle.dump(dtype_dict, pf)


# MERGE LOCAL & SUMMARY TABLES
        
JAM_VALS = [-666666666, -888888888, -999999999]


def process_single_col_bg_table(tbl_id, col_idx, new_col_name, dtype):
    """
    Merges blockgroup table with summary data whilst:
    - converting object/str datatypes to numeric
    - checking for "Jam" values and converting to NaN
    
    **Only works up to indentation level == 2**

    params
    --------
    tbl_id: str
        Table ID to process. Ex: 'B01001'
    col_idx: int
        Integer representing "line number" of column to process. 
        Note these should not be the index of the column in the bg table, 
        but rather the "Line" of the column from the table shell lookup. 
        Also corresponds to X in blockgroup table column name: '[TBL_ID]_E00X'.
    new_col_name: str
        Name to assign to column. 
    dtype: str
        Datatypes for the new column. Should be in format acceptable by 
       `df.astype()`, e.g. numpy.dtypes (`int`, `int32`, `int64`, `float`
       etc.)

    """
    # make backwards compatible
    if dtype == 'integer':
        dtype = 'int'

    bg_table_df = load_table(tbl_id) 
    old_col_name = f"{tbl_id}_E{col_idx:03}"
    bg_table_df[new_col_name] = bg_table_df[old_col_name].astype(dtype)
    bg_table_df.replace(JAM_VALS, np.nan, inplace=True)
    return bg_table_df


def process_multiple_cols_bg_table(tbl_id, col_idxs, new_col_names, dtype):
    """
    tbl_id: str
        Table ID to process. Ex: 'B01001'
    col_idxs: list(int)
        List if integers representing "line number" of column to process. 
        Note these should not be the index of the column in the bg table, 
        but rather the "Line" of the column from the table shell lookup. 
        Also corresponds to X in blockgroup table column name: '[TBL_ID]_E00X'.
    new_col_names: list(str_
        List of names to assign to columns.
    dtype: str
       Datatype for the new columns. Should be in format acceptable by 
       `df.astype()`, e.g. numpy.dtypes (`int`, `int32`, `int64`, `float`
       etc.)

    """
    assert len(col_idxs) == len(new_col_names), "Lists must be of equal length"

    old_col_names = [f"{tbl_id}_E{int(i):03}" for i in col_idxs]
    
    bg_table_df = load_table(tbl_id) 
    bg_table_df.rename(columns=dict(zip(old_col_names, new_col_names)), inplace=True)
    bg_table_df = bg_table_df.astype(dict(zip(new_col_names, [dtype]*len(new_col_names))))
    bg_table_df.replace(JAM_VALS, np.nan, inplace=True)

    return bg_table_df


def process_all_cols_bg_table(tbl_id, dtype):
    """
    Merges blockgroup table with summary data whilst:
    - renaming all E columns
        - uses 'Indent' information
    - converting datatypes to numeric
    - checking for "Jam" values and converting to NaN
    
    params
    --------
    tbl_id: str
        Table ID to process. Ex: 'B01001'
    dtype: str
       Datatype for the new columns. Should be in format acceptable by 
       `df.astype()`, e.g. numpy.dtypes (`int`, `int32`, `int64`, `float`
       etc.)

    returns 
    ---------
    pd.DataFrame
    """
    bg_table_df = load_table(tbl_id) #bg_table_df.copy()
    col_names_df = get_column_names(tbl_id)

    new_col_names = []
    for i, row in col_names_df.iterrows():
        indent = row['Indent']
        if indent == 0:
            new_col_name = row['Label'].strip(":")
        elif indent == 1:
            new_col_name = row['Label'].strip(":") + " total"
            prefix = row['Label'].strip(":")
        else:
            new_col_name = prefix + " " + row['Label']
    
        new_col_names.append(new_col_name)
    
    old_col_names = [f"{tbl_id}_E{int(i):03}" for i in col_names_df['Line']]
    
    # rename columns
    bg_table_df.rename(columns=dict(zip(old_col_names, new_col_names)), inplace=True)
    # convert dtype
    bg_table_df = bg_table_df.astype(dict(zip(new_col_names, [dtype]*len(old_col_names))))
    # replace Jam values
    bg_table_df.replace(JAM_VALS, np.nan, inplace=True)
        
    return bg_table_df



def extract_bg_fips_from_geo_id(df):
        df['bg_fips'] = df[BG_TABLE_KEY_COL].str.split('US').apply(lambda arr: arr[-1])
        return df.drop(columns=[BG_TABLE_KEY_COL])


def merge_bg_table_with_summary_df(bg_table_df, summary_df):
    """Merges processed blockgroup table with summary data.
    
    Assumes certain formats/columns names in both dataframes.

    """
    if 'bg_fips' not in bg_table_df:
        bg_table_df = extract_bg_fips_from_geo_id(bg_table_df)
    if 'bg_fips' not in summary_df:
        summary_df = extract_bg_fips_from_geo_id(summary_df)

    summary_update_df = summary_df.merge(bg_table_df, on='bg_fips', how='left')
    return summary_update_df


