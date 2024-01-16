
import pandas as pd

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


def process_state_list(summary_df, state_list):
    """
    state_list: list(str)
        List of state 2-letter abbreviations.
    """
    
    state_merge_df_list = []

    for state_abbr in state_list:
        state_df = fetch_state_lat_lon(state_abbr)
        if state_df is not None:
            state_merge_df = summary_df.merge(
                state_df, on='bg_fips', how='inner')
            if len(state_merge_df) == 0:
                print(f"**No merged data for {state_abbr}!**")
            else:
                state_merge_df_list.append(state_merge_df)

    state_lat_lon_df = pd.concat(state_merge_df_list, axis=0)
    SQMETERS_IN_SQMILE = 2589988.11
    state_lat_lon_df['area_sqmile'] = state_lat_lon_df['AREALAND'] / SQMETERS_IN_SQMILE

    # rejigger columns
    state_lat_lon_df.drop(columns=['AREALAND'], inplace=True)

    state_lat_lon_df.rename(columns={
        'CENTLAT': 'blockgroup_center_lat',
        'CENTLON': 'blockgroup_center_lng',
    }, inplace=True)

    return state_lat_lon_df 


def get_lat_lon_data(summary_df, state_geo_df):
    state_list = state_geo_df['STUSAB']
    state_lat_lon_df = process_state_list(summary_df, state_list)
    return state_lat_lon_df
