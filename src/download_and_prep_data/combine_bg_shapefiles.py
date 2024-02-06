import os
import shutil
import sys

import geopandas as gpd
import pandas as pd
from tqdm import tqdm

sys.path.append("..")
from configs import (
    PROCESSED_DIR,
    STATE_FIPS_PATH, STATE_SHAPEFILES_OUT_DIR, STATE_FOLDER,
    US_SHAPEFILE_PATH,
)
# from process_bg_tables.util import load_checkpoint_df

# path of this file, relative to parent folder of project/repo
REL_PATH = "../.."
# OUTDIR = f"data/tigerweb"
# OUT_DIR = PROCESSED_DIR
# STATE_FOLDER = "state_bg_shapefiles_2020"
## TODO: move this to config.py?
# CONCAT_FOLDER = "us_bg_shapefiles_2020"


def main(rel_path):

    # out_path = f"{rel_path}/{OUT_DIR}"
    os.makedirs(f"{rel_path}/{US_SHAPEFILE_PATH}", exist_ok=True)

    # load state list
    # state_lkup_df = load_checkpoint_df(STATE_TABLE_NAME, rel_path=rel_path)
    state_lkup_df = pd.read_csv(f"{rel_path}/{STATE_FIPS_PATH}", dtype="str")
    state_list = state_lkup_df['STATE']

    print("Reading in state shapefiles...")
    state_gdf_list = []
    for state_code in tqdm(state_list, desc="states"):
        state_path = f"{rel_path}/{STATE_SHAPEFILES_OUT_DIR}/{STATE_FOLDER}/{state_code}"
        state_gdf = gpd.read_file(state_path)
        state_gdf_list.append(state_gdf)

    print("Combining & saving to disc")
    us_gdf = pd.concat(state_gdf_list, axis=0)
    ## TODO: no longer need to nest this output?
    us_gdf.to_file(f"{rel_path}/{US_SHAPEFILE_PATH}")

    print("Deleting state shapefiles")
    for state_code in state_list:
        state_path = f"{rel_path}/{STATE_SHAPEFILES_OUT_DIR}/{STATE_FOLDER}/{state_code}"
        shutil.rmtree(state_path)
    
    print("")


if __name__ == "__main__":
    main(REL_PATH)