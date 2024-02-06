
import sys
import pandas as pd
from tqdm import tqdm

sys.path.append("..")
from process_bg_tables.util import save_csv_and_dtypes #load_checkpoint_df, save_checkpoint_df
from configs import LAT_LON_PATH, STATE_FIPS_PATH


# path of this file, relative to parent folder of project/repo
REL_PATH = "../.."
BASE_TW_BG_URL = "https://tigerweb.geo.census.gov/tigerwebmain/Files/acs23/tigerweb_acs23_blkgrp_2022_acs22_{}.html"


def fetch_state_lat_lon(state_abbr):
    state_tw_bg_url = BASE_TW_BG_URL.format(state_abbr.lower())
    page_table_list = pd.read_html(state_tw_bg_url)
    state_lat_lon_df = page_table_list[0]
    
    if len(state_lat_lon_df) == 0:
        print(f"**No data for {state_abbr}! url: {state_tw_bg_url}")
        return None
        
    # need to convert int to str. just rename to col we want to join on, for simplicity
    state_lat_lon_df['bg_fips'] = state_lat_lon_df['GEOID'].apply(str).str.zfill(12)

    return state_lat_lon_df[['bg_fips', 'CENTLAT', 'CENTLON', 'AREALAND']]


def fetch_process_all_lat_lon_data(state_list):
    """
    state_list: list(str)
        List of state 2-letter abbreviations.
    """
    
    state_lat_lon_df_list = []

    for state_abbr in tqdm(state_list, desc='states'):
        state_lat_lon_df = fetch_state_lat_lon(state_abbr)
        if state_lat_lon_df is not None:
            state_lat_lon_df_list.append(state_lat_lon_df)

    lat_lon_df = pd.concat(state_lat_lon_df_list, axis=0)
    SQMETERS_IN_SQMILE = 2589988.11
    lat_lon_df['area_sqmile'] = lat_lon_df['AREALAND'] / SQMETERS_IN_SQMILE

    # rejigger columns
    lat_lon_df.drop(columns=['AREALAND'], inplace=True)
    lat_lon_df.rename(columns={
        'CENTLAT': 'blockgroup_center_lat',
        'CENTLON': 'blockgroup_center_lng',
    }, inplace=True)

    return lat_lon_df 


def main(rel_path):
    print("Fetching lat/lon data")
    # state_lkup_df = load_checkpoint_df(STATE_TABLE_NAME, rel_path)
    state_lkup_df = pd.read_csv(f"{rel_path}/{STATE_FIPS_PATH}", dtype="str")
    state_list = state_lkup_df['STUSAB']
    lat_lon_df = fetch_process_all_lat_lon_data(state_list)
    # save_checkpoint_df(lat_lon_df, LAT_LON_FILENAME, rel_path)
    save_csv_and_dtypes(lat_lon_df, LAT_LON_PATH, rel_path)


if __name__ == "__main__":
    main(REL_PATH)