import os, sys
import pandas as pd
import geopandas as gpd
from tqdm import tqdm

OUTDIR = "../data/tigerweb"
STATE_FOLDER = "state_bg_shapefiles_2020"
CONCAT_FOLDER = "us_bg_shapefiles_2020"

if __name__ == "__main__":

    os.makedirs(f"{OUTDIR}/{CONCAT_FOLDER}", exist_ok=True)

    # load state list
    state_lkup_df = pd.read_csv("../data/state_geo_lkup.csv", 
                                dtype={'GEO_ID': 'object', 'STUSAB': 'object', 'STATE': 'object', 'NAME': 'object'})
    state_list = state_lkup_df['STATE']

    state_gdf_list = []
    print("Reading in state shapefiles...")
    for state_code in tqdm(state_list, desc="states"):
        state_path = f"{OUTDIR}/{STATE_FOLDER}/{state_code}"
        state_gdf = gpd.read_file(state_path)
        state_gdf_list.append(state_gdf)
    
    print("Combining & saving to disc")
    us_gdf = pd.concat(state_gdf_list, axis=0)
    us_gdf.to_file(f"{OUTDIR}/{CONCAT_FOLDER}/us")

    # conus_gdf = us_gdf[~us_gdf['STATEFP'].isin(['02', '15'])]
    # conus_gdf.to_file(f"{OUTDIR}/{CONCAT_FOLDER}/conus")