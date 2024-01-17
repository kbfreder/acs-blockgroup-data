
import pandas as pd

from src.process_bg_tables.configs import LAT_LON_FILENAME
from src.process_bg_tables.util import save_summary_df


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


def get_lat_lon_data(state_list):
    """
    state_list: list(str)
        List of state 2-letter abbreviations.
    """
    
    state_df_list = []

    for state_abbr in state_list:
        state_df = fetch_state_lat_lon(state_abbr)
        if state_df is not None:
            state_df_list.append(state_df)

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


if __name__ == "__main__":
    lat_lon_df = get_lat_lon_data()
    save_summary_df(lat_lon_df, LAT_LON_FILENAME)