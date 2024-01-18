"""households.py

Most of these attributes are not dependent on obtaining 
the total number of households prior to their calculation.
They are all in the "Households" or "Occupied housing units" universes, 
which are equivalent, so they can be calculated independently.
We group them here for simplicity in `main.py`

"""
import os, sys
import pandas as pd
import numpy as np
from functools import reduce

from .util import (
    get_column_names_df,
    process_some_cols_bg_table,
    process_all_cols_bg_table
    )
from .configs import (
    BG_TABLE_KEY_COL
    )


def get_total_num_households():
    tbl_id = 'B11012' # Households by Type
    col_idx = 1 # Total
    col_name = 'households'
    bg_df = process_some_cols_bg_table(tbl_id, [col_idx], [col_name], 'int')
    return bg_df[[BG_TABLE_KEY_COL, 'households']]


def get_pct_internet():
    tbl_id = 'B28002' # 'Presence and Types of Internet Subscriptions in Household
    col_idxs = [1, 2] # Total, With an Internet subscription
    col_names = ['total', 'with_internet']
    bg_df = process_some_cols_bg_table(tbl_id, col_idxs, col_names, 'int')

    bg_df['pct_hh_internet_subscription'] = (
        bg_df['with_internet'] / bg_df['total'])

    return bg_df[[BG_TABLE_KEY_COL,  'pct_hh_internet_subscription']]


# def get_pct_vehicle_ownership():
    # tbl_id = 'B25044' # Tenure by Vehicles Available
    # col_idxs = [1, 3, 10] # Total, Owner occupied: No vehicle available, Renter occupied: No vehicle available
    # col_names = ["total", "hh_no_vehicle_owner_occ", "hh_no_vehicle_renter_occ"]
    # bg_df = process_some_cols_bg_table(tbl_id, col_idxs, col_names, 'int')

    # # pct ownership
    # bg_df['hh_w_vehicles'] = bg_df['total'] - (
    #     bg_df['hh_no_vehicle_owner_occ'] - bg_df['hh_no_vehicle_renter_occ']
    # )
    # bg_df['pct_hh_vehicle_ownership'] = bg_df['hh_w_vehicles'] / bg_df['total']

    # return bg_df[[BG_TABLE_KEY_COL, 'pct_hh_vehicle_ownership']]

def get_vehicle_data():

    tbl_id = 'B25044' # Tenure by Vehicles Available
    col_names_df = get_column_names_df(tbl_id)
    bg_df = process_all_cols_bg_table(tbl_id, col_names_df, 'int')

    # pct ownership
    bg_df['hh_w_vehicles'] = bg_df['Total'] - (
        bg_df['Owner occupied No vehicle available'] + bg_df['Renter occupied No vehicle available']
    )
    bg_df['pct_hh_vehicle_ownership'] = bg_df['hh_w_vehicles'] / bg_df['Total']

    # avg num
    bg_df['avg_vehicles_per_hh'] = (
        (
            1*(bg_df['Owner occupied 1 vehicle available'] + bg_df['Renter occupied 1 vehicle available'])
            + 2*(bg_df['Owner occupied 2 vehicles available'] + bg_df['Renter occupied 2 vehicles available'])
            + 3*(bg_df['Owner occupied 3 vehicles available'] + bg_df['Renter occupied 3 vehicles available'])
            + 4*(bg_df['Owner occupied 4 vehicles available'] + bg_df['Renter occupied 4 vehicles available'])
            + 5*(bg_df['Owner occupied 5 or more vehicles available'] + bg_df['Renter occupied 5 or more vehicles available'])
        ) / bg_df['Total']
    )

    return bg_df[[BG_TABLE_KEY_COL, 'pct_hh_vehicle_ownership', 'avg_vehicles_per_hh']]


# def get_agg_num_vehicles_per_hh():
    # # Note: We ultimately want to calculate avg_num_vehicles_per_hh, but this table
    # # doesn't include totaly num households in it. We pass along the aggregate
    # # number of vehicles, and compute the average later

    # tbl_id = 'B25046' # Aggregate Number of Vehicles Available by Tenure
    # col_idx = 1 # Aggregate number of vehicles available
    # col_name = "agg_num_vehicles"
    # bg_df = process_some_cols_bg_table(tbl_id, [col_idx], [col_name], 'int')
    # return bg_df[[BG_TABLE_KEY_COL, 'agg_num_vehicles']]
    

def get_pct_foodstamps():
    tbl_id = 'B19058' # Public Assistance Income or Food Stamps/SNAP in the Past 12 Months for Households
    col_idxs = [1, 2] # With cash public assistance or Food Stamps/SNAP
    col_names = ['total', 'num_hh_foodstamps']
    bg_df = process_some_cols_bg_table(tbl_id, col_idxs, col_names, 'int')
    bg_df['pct_hh_foodstamps'] = bg_df['num_hh_foodstamps'] / bg_df['total']
    return bg_df[[BG_TABLE_KEY_COL, 'pct_hh_foodstamps']]


def get_pct_home_ownership():
    tbl_id = 'B25003' # Tenure [Universe: Occupied Housing Units]
    col_idxs = [1, 2] 
    col_names = ['total', 'owner_occupied']
    bg_df = process_some_cols_bg_table(tbl_id, col_idxs, col_names, 'int')
    bg_df['pct_home_ownership'] = bg_df['owner_occupied'] / bg_df['total']
    return bg_df[[BG_TABLE_KEY_COL, 'pct_home_ownership']]


# META FUNCTION
def get_all_household_data():
    df_list = []
    for func in [
        get_total_num_households,
        get_pct_internet,
        get_vehicle_data,
        get_pct_foodstamps,
        get_pct_home_ownership
    ]:
        df_list.append(func())
    
    # merge
    hh_df = reduce(
        lambda df1, df2: df1.merge(df2, on=BG_TABLE_KEY_COL, how='outer'), 
        df_list)
    
    # derived computations
    # hh_df['avg_vehicles_per_hh'] = hh_df['agg_num_vehicles'] / hh_df['households']

    return hh_df


if __name__ == "__main__":
    hh_df = get_all_household_data()
    print(hh_df.head())