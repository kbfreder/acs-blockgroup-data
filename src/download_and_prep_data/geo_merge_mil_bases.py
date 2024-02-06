import os, sys

import pandas as pd
import geopandas as gpd
import numpy as np

sys.path.append("..")
from process_bg_tables.util import save_csv_and_dtypes
from configs import MIL_GEO_IND_PATH, US_SHAPEFILE_PATH


# path of this file, relative to parent folder of project/repo
REL_PATH = "../.."
# column name assigned to indicator field
MIL_BASE_IND_COL = "military_base_flag_geo"


def fetch_military_geo_data():
    page_list = pd.read_html("https://tigerweb.geo.census.gov/tigerwebmain/Files/acs23/tigerweb_acs23_military_us.html")
    mil_base_df = page_list[0]

    mil_gdf = gpd.GeoDataFrame(
        mil_base_df, geometry=gpd.points_from_xy(mil_base_df['CENTLON'], mil_base_df['CENTLAT']), 
        crs = "EPSG:4269" # this is the CRS of other shapefiles from TigerWeb
    )
    return mil_gdf


def main(rel_path, save_file=True):
    print("Fetching military base lat/lon data")
    mil_gdf = fetch_military_geo_data()
    
    print("Loading US blockgroup shapefile data")
    bg_shapefile_path = f"{rel_path}/{US_SHAPEFILE_PATH}"
    if os.path.exists(bg_shapefile_path):
        us_gdf = gpd.read_file(bg_shapefile_path)
    else:
        print("Please generate US blockgroup shapefile.")
    
    print("Perforing spatial join")
    merge_df = us_gdf.sjoin(mil_gdf, how="left", predicate="contains")
    
    print("Aggregating data")
    merge_df['mil_base_ind'] = np.where(merge_df['BASENAME'].isna(), 0, 1)
    tmp = merge_df.groupby('GEOID')['mil_base_ind'].sum()
    mil_base_ind_df = pd.DataFrame(tmp)
    mil_base_ind_df.columns = ['num_bases']
    mil_base_ind_df.reset_index(inplace=True)
    mil_base_ind_df[MIL_BASE_IND_COL] = np.where(mil_base_ind_df['num_bases'] > 0, 1, 0)

    mil_base_ind_df = mil_base_ind_df.drop(columns=['num_bases'])

    # print("Final lookup data:")
    # print(mil_base_ind_df.sample(10))

    if save_file:
        print("Saving data")
        save_csv_and_dtypes(mil_base_ind_df,
                            MIL_GEO_IND_PATH,
                            rel_path)
    else:
        mil_base_ind_df


if __name__ == "__main__":
    main(rel_path=REL_PATH)