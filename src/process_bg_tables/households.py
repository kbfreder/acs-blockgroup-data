import os, sys

import pandas as pd
from fuzzywuzzy import fuzz
from numbers import Number

import numpy as np
import re

from functools import reduce

from util import (
    merge_bg_table_with_summary_df,
    process_multiple_cols_bg_table,
    process_single_col_bg_table,
    )
from configs import (
    DENOM_TO_USE,
    BG_TABLE_KEY_COL
    )


def get_total_num_households():
    tbl_id = 'B11012' # Households by Type
    col_idx = 1 # Total
    bg_df = process_single_col_bg_table(tbl_id, col_idx, 'households', 'int')
    return bg_df[[BG_TABLE_KEY_COL, 'households']]


def get_pct_internet(hh_df):
    tbl_id = 'B28002' # 'Presence and Types of Internet Subscriptions in Household
    col_idxs = [2] # Total, With an Internet subscription
    col_names = ['with_internet']
    bg_df = process_multiple_cols_bg_table(tbl_id, col_idxs, col_names, 'int')
    bg_merge_df = merge_bg_table_with_summary_df(bg_df[[BG_TABLE_KEY_COL]+col_names], hh_df)

    bg_merge_df['pct_hh_internet_subscription'] = bg_merge_df['with_internet'] / bg_merge_df['households']
    return bg_merge_df.drop(columns=col_names)


def get_pct_vehicle_ownership(hh_df):
    tbl_id = 'B25044' # Tenure by Vehicles Available
    col_idxs=[3,10] # Owner occupied: No vehicle available, Renter occupied: No vehicle available
    col_names=["hh_no_vehicle_owner_occ", "hh_no_vehicle_renter_occ"]
    bg_df = process_multiple_cols_bg_table(tbl_id, col_idxs, col_names, 'int')
    bg_merge_df = merge_bg_table_with_summary_df(bg_df[[BG_TABLE_KEY_COL]+col_names], hh_df)

    bg_merge_df['hh_w_vehicles'] = bg_merge_df['households'] - bg_merge_df['hh_no_vehicle_owner_occ'] - bg_merge_df['hh_no_vehicle_renter_occ']
    bg_merge_df['pct_hh_vehicle_ownership'] = bg_merge_df['hh_w_vehicles'] / bg_merge_df['households']

    return bg_merge_df.drop(columns=col_names+['hh_w_vehicles'])


def get_avg_vehicles_per_hh(hh_df):
    tbl_id = 'B25046' # Aggregate Number of Vehicles Available by Tenure
    col_idx = 1 # Aggregate number of vehicles available
    col_name = "agg_num_vehicles"
    bg_df = process_single_col_bg_table(tbl_id, col_idx, col_name, 'int')
    bg_merge_df = merge_bg_table_with_summary_df(bg_df[[BG_TABLE_KEY_COL, col_name]], hh_df)

    bg_merge_df['avg_vehicles_per_hh'] = bg_merge_df['agg_num_vehicles'] / bg_merge_df['households']

    return bg_merge_df.drop(columns=[col_name])


def get_pct_foodstamps(hh_df):
    tbl_id = 'B19058' # Public Assistance Income or Food Stamps/SNAP in the Past 12 Months for Households
    col_idx = 2 # With cash public assistance or Food Stamps/SNAP
    col_name = 'num_hh_foodstamps'
    bg_df = process_single_col_bg_table(tbl_id, col_idx, col_name, 'int')
    bg_merge_df = merge_bg_table_with_summary_df(bg_df[[BG_TABLE_KEY_COL, col_name]], hh_df)

    bg_merge_df['pct_hh_foodstamps'] = bg_merge_df['num_hh_foodstamps'] / bg_merge_df['households']

    return bg_merge_df.drop(columns=[col_name])


def get_all_household_data():
    hh_df = get_total_num_households()
    for func in [
        get_pct_internet,
        get_pct_vehicle_ownership,
        get_avg_vehicles_per_hh,
        get_pct_foodstamps
    ]:
        hh_df = func(hh_df)
    # reorder columns:
    hh_other_cols = [col for col in hh_df.columns if col != 'bg_fips']
    hh_df = hh_df[['bg_fips'] + hh_other_cols]
    return hh_df


if __name__ == "__main__":
    hh_df = get_all_household_data()
    print(hh_df.head())