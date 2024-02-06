
import os
import pickle
import numpy as np
import pandas as pd


from configs import (
    ACS_PARSED_DATA_DIR, YEAR, DATASET_YRS, ACS_SUMMARY_FILES_DIR, BG_TABLE_KEY_COL
)


def get_column_names_df(tbl_id):
    shell_ftp_dir = "https://www2.census.gov/programs-surveys/acs/summary_file/2022/table-based-SF/documentation/ACS20225YR_Table_Shells.txt"
    shell_df = pd.read_csv(shell_ftp_dir, sep="|", )
    return shell_df[shell_df['Table ID'] == tbl_id.upper()]


def load_table(table_id, rel_path="..", sum_level='150'):
    """Load ACS Summary File table (.csv)

    params:
    ---------
    table_id (str):
        Table ID. Ex: 'B07001'. 
    rel_path (str):
        Relative path from calling script to root of project.
        Ex: ".."
    sum_level (str):
        ACS summary level to load data for. '150' = block group; '140' = tract.
    """
    file_path = f"{rel_path}/{ACS_SUMMARY_FILES_DIR}/sumlevel={sum_level}/acsdt{DATASET_YRS}y{YEAR}-{table_id.lower()}.dat"
    try:
        df = pd.read_csv(file_path, sep="|", dtype="str")
    except FileNotFoundError:
        print("File not found!", file_path)
        df = None
    return df


def load_csv_with_dtypes(path_without_ext, rel_path=".."):
    with open(f"{rel_path}/{path_without_ext}.pkl", "rb") as f:
        dtype_dict = pickle.load(f)
    df = pd.read_csv(f"{rel_path}/{path_without_ext}.csv", dtype=dtype_dict)
    return df


def save_csv_and_dtypes(df, path_without_ext, rel_path=".."):
    dtype_dict = df.dtypes.apply(lambda x: x.name).to_dict()
    save_path = f"{rel_path}/{path_without_ext}"
    df.to_csv(f"{save_path}.csv", index=False)
    with open(f"{save_path}.pkl", "wb") as pf:
        pickle.dump(dtype_dict, pf)


# LOAD/SAVE CHECKPOINTS
def load_checkpoint_df(checkpoint_name, rel_path=".."):
    file_stub = f"{ACS_PARSED_DATA_DIR}/{checkpoint_name}"
    return load_csv_with_dtypes(file_stub, rel_path)


def save_checkpoint_df(summary_df, save_name, rel_path=".."):
    file_stub = f"{ACS_PARSED_DATA_DIR}/{save_name}"
    save_csv_and_dtypes(summary_df, file_stub, rel_path)


# PROCESSING ACS TABLES
JAM_VALS = [-666666666, -888888888, -999999999]

def process_some_cols_bg_table(tbl_id, col_idxs, new_col_names, dtype):
    """Process specified columns of blockgroup table.
    Return only those columns, plus the blockgroup key/id column.

    "Process" = load table, rename column, convert datatype, convert "Jam" 
    values to NaN.

    params:
    -------
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

    return bg_table_df[[BG_TABLE_KEY_COL] + new_col_names]


def process_all_cols_bg_table(tbl_id, col_names_df, dtype):
    """Process all columns of blockgroup table.

    "Process" ::: load table, rename column, convert datatype, convert "Jam" 
    values to NaN.
    
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
    bg_table_df = load_table(tbl_id)

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
        
    return bg_table_df[[BG_TABLE_KEY_COL] + new_col_names]


def extract_bg_fips_from_geo_id(df):
        df['bg_fips'] = df[BG_TABLE_KEY_COL].str.split('US').apply(lambda arr: arr[-1])
        return df.drop(columns=[BG_TABLE_KEY_COL])


def merge_bg_table_with_summary_df(bg_table_df, summary_df):
    """Merges processed blockgroup table with summary data.
    
    Converts blockgroup table key column (usually 'GEO_ID') to 'bg_fips'.
    """
    if 'bg_fips' not in bg_table_df:
        bg_table_df = extract_bg_fips_from_geo_id(bg_table_df)
    if 'bg_fips' not in summary_df:
        summary_df = extract_bg_fips_from_geo_id(summary_df)

    summary_update_df = summary_df.merge(bg_table_df, on='bg_fips', how='left')
    return summary_update_df


