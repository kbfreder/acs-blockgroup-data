import os, sys

import pandas as pd
from fuzzywuzzy import fuzz
from numbers import Number
from collections import OrderedDict

import numpy as np
import re
import pickle


##############################
# CONFIGS
##############################


# nature ACS data
YEAR = 2022
DATASET = 5 # 5-year estimate

# which denominator to use in pct or prop calculations
    ## 'total' = total population or household
    ## 'table' = total value provided by table numerator data is found in
DENOM_TO_USE = 'total'

# the key or ID column in block group summary tables
BG_TABLE_KEY_COL = 'GEO_ID'


# paths
BG_DATA_PATH = "../../data/sumlevel=150/"
# filename to save "base" BG table as (contains only FIPS numbers)
BASE_TABLE_NAME = "bg_fips"
STATE_TABLE_NAME = "state_fips"


# misc
JAM_VALS = [-666666666, -888888888, -999999999]


##############################
# UTILITY FUNCTIONS
##############################

def load_table(table_id):
    """Read in locally-saved output from `fetch_acs_summary_files.py`.
    """
    file_path = f"{BG_DATA_PATH}acsdt{DATASET}y{YEAR}-{table_id.lower()}.dat"
    try:
        df = pd.read_csv(file_path, sep="|", dtype="str")
    except FileNotFoundError:
        print("File not found!", file_path)
        df = None
    return df


def get_column_names(tbl_id, shell_df):
    return shell_df[shell_df['Table ID'] == tbl_id.upper()]


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

def process_some_cols_bg_table(tbl_id, col_idxs, new_col_names, dtype):
    """Process specified columns of blockgroup table.

    "Process" = load table, rename column, convert datatype, convert "Jam" 
    values to NaN.

    params
    ----------
    tbl_id: str
        Table ID to process. Ex: 'B01001'
    col_idxs: list(int)
        List of integers representing "line number" of column to process. 
        Note these should *not* be the index of the column in the bg table, 
        but rather the "Line" of the column from the table shell lookup. 
        Also corresponds to X in blockgroup table column name: '[TBL_ID]_E00X'.
    new_col_names: list(str)
        List of names to assign to columns specified in `col_idxs`.
    dtype: str
       Datatype for the new columns. Should be in format acceptable by 
       `df.astype()`, e.g. numpy.dtypes (`int`, `int32`, `int64`, `float`
       etc.)

    returns
    --------
    pd.Dataframe
        Dataframe containing just the key/ID column and processed columns
    """
    assert len(col_idxs) == len(new_col_names), "Lists must be of equal length"
    tbl_id = tbl_id.upper()

    old_col_names = [f"{tbl_id}_E{int(i):03}" for i in col_idxs]
    
    bg_table_df = load_table(tbl_id) 
    bg_table_df.rename(columns=dict(zip(old_col_names, new_col_names)), inplace=True)
    bg_table_df = bg_table_df.astype(dict(zip(new_col_names, [dtype]*len(new_col_names))))
    bg_table_df.replace(JAM_VALS, np.nan, inplace=True)

    return bg_table_df[[BG_TABLE_KEY_COL] + new_col_names]


def process_all_cols_bg_table(tbl_id, dtype):
    """Process all columns of blockgroup table.
    
    "Process" = load table, rename column, convert datatype, convert "Jam" 
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


##############################
# PROCESSING BLOCKGROUP DATA
## `fetch_acs_summary_files.py` should first be run

##############################

# list of dataframes we will merge together on `bg_fips` at the end
df_list = []

# "BASE" info: 
## Downloads "geo" documentation from URL, of which there's no guarantee it won't change!
path = "https://www2.census.gov/programs-surveys/acs/summary_file/2022/table-based-SF/documentation/Geos20225YR.txt"
geo5_df = pd.read_csv(path, sep="|", dtype="str")

bg_cols = ['STATE', 'COUNTY', 'TRACT', 'BLKGRP', 'NAME',]
block_geo_df = geo5_df[geo5_df['SUMLEVEL'] == '150'][bg_cols]
block_geo_df['bg_fips'] = block_geo_df['STATE'] + block_geo_df['COUNTY'] + block_geo_df['TRACT'] + block_geo_df['BLKGRP']
block_geo_df['census_tract_fips'] = block_geo_df['STATE'] + block_geo_df['COUNTY'] + block_geo_df['TRACT']
block_geo_df['county_fips'] = block_geo_df['STATE'] + block_geo_df['COUNTY']

base_df = block_geo_df[['bg_fips']]

df_list.append(block_geo_df)

# note: need to filter on `COMPONENET`, otherwise get >600 entries
state_cols = ['STUSAB', 'STATE', 'NAME']
state_geo_df = geo5_df[(geo5_df['SUMLEVEL'] == '040') & (geo5_df['COMPONENT'] == '00')][state_cols]
state_geo_df.reset_index(inplace=True, drop=True)


# LAT/LON
## This downloads lat/lon data from Tigerweb, state-by-state, 
## then combines the data

BASE_TW_BG_URL = "https://tigerweb.geo.census.gov/tigerwebmain/Files/acs23/tigerweb_acs23_blkgrp_2022_acs22_{}.html"

def fetch_state_lat_lon(state_abbr):
    state_tw_bg_url = BASE_TW_BG_URL.format(state_abbr.lower())
    page_table_list = pd.read_html(state_tw_bg_url)
    state_df = page_table_list[0]
    
    if len(state_df) == 0:
        print(f"**No data for {state_abbr}! url: {state_tw_bg_url}")
        return None
        
    # need to convert int to str. just rename to col we want to join on, for simplicity
    state_df['bg_fips'] = state_df['GEOID'].apply(str).str.zfill(12)

    return state_df[['bg_fips', 'CENTLAT', 'CENTLON', 'AREALAND']]


def process_state_list(state_list):
    """
    state_list: list(str)
        List of state 2-letter abbreviations.
    """
    
    # state_merge_df_list = []
    state_df_list = []

    # for state_abbr in tqdm(state_geo_df['STUSAB'], desc="states"):
    for state_abbr in state_list:
        state_df = fetch_state_lat_lon(state_abbr)
        # if state_df is not None:
        #     state_merge_df = base_df.merge(
        #         state_df, on='bg_fips', how='inner')
        #     if len(state_merge_df) == 0:
        #         print(f"**No merged data for {state_abbr}!**")
        #     else:
                # state_merge_df_list.append(state_merge_df)
        state_df_list.append(state_df)

    # state_lat_lon_df = pd.concat(state_merge_df_list, axis=0)
    state_lat_lon_df = pd.concat(state_df_list, axis=0)
    
    SQMETERS_IN_SQMILE = 2589988.11
    state_lat_lon_df['area_sqmile'] = state_lat_lon_df['AREALAND'] / SQMETERS_IN_SQMILE

    # rejigger columns
    state_lat_lon_df.drop(columns=['AREALAND'], inplace=True)

    state_lat_lon_df.rename(columns={
        'CENTLAT': 'blockgroup_center_lat',
        'CENTLON': 'blockgroup_center_lng',
    }, inplace=True)

    return state_lat_lon_df 


# def get_lat_lon_data(summary_df, state_geo_df):
state_list = state_geo_df['STUSAB']
state_lat_lon_df = process_state_list(base_df, state_list)
