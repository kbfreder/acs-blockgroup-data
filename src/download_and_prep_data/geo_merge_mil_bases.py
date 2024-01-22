import os, sys
import argparse

import pandas as pd
import geopandas as gpd
from functools import reduce
import numpy as np

sys.path.append("..")
from process_bg_tables.util import save_csv_and_dtypes
from configs import MIL_GEO_IND_PATH


# path of this file, relative to parent folder of project/repo
REL_PATH = "../.."

def fetch_military_geo_data():
    page_list = pd.read_html("https://tigerweb.geo.census.gov/tigerwebmain/Files/acs23/tigerweb_acs23_military_us.html")
    mil_base_df = page_list[0]

    mil_gdf = gpd.GeoDataFrame(
        mil_base_df, geometry=gpd.points_from_xy(mil_base_df['CENTLON'], mil_base_df['CENTLAT']), 
        crs = "EPSG:4269" # this is the CRS of other shapefiles from TigerWeb
    )
    return mil_gdf

def main(save_file=True):
    print("Fetching military base lat/lon data")
    mil_gdf = fetch_military_geo_data()
    
    print("Loading US blockgroup shapefile data")
    bg_shapefile_path = f"{REL_PATH}/data/tigerweb/us_bg_shapefiles_2020/us/"
    if os.path.exists(bg_shapefile_path):
        us_gdf = gpd.read_file(bg_shapefile_path)
    else:
        print("Please run generate US blockgroup shapefile.")
    
    print("Perforing spatial join")
    merge_df = us_gdf.sjoin(mil_gdf, how="left", predicate="contains")
    
    print("Aggregating data")
    merge_df['mil_base_ind'] = np.where(merge_df['BASENAME'].isna(), 0, 1)
    tmp = merge_df.groupby('GEOID')['mil_base_ind'].sum()
    mil_base_ind_df = pd.DataFrame(tmp)
    mil_base_ind_df.columns = ['num_bases']
    mil_base_ind_df.reset_index(inplace=True)
    mil_base_ind_df['military_base_flag_geo'] = np.where(mil_base_ind_df['num_bases'] > 0, 1, 0)

    mil_base_ind_df = mil_base_ind_df.drop(columns=['num_bases'])

    print("Final lookup data:")
    print(mil_base_ind_df.sample(10))

    if save_file:
        print("Saving data")
        save_csv_and_dtypes(mil_base_ind_df,
                            MIL_GEO_IND_PATH,
                            REL_PATH)
    else:
        mil_base_ind_df


if __name__ == "__main__":
    # main()
    print("imports OK")