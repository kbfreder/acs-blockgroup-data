import os, sys
import shutil
import pandas as pd
import geopandas as gpd
from tqdm import tqdm

sys.path.append("..")
from process_bg_tables.util import load_summary_df
from configs import STATE_TABLE_NAME

# path of this file, relative to parent folder of project/repo
REL_PATH = "../.."
OUTDIR = f"data/tigerweb"
STATE_FOLDER = "state_bg_shapefiles_2020"
CONCAT_FOLDER = "us_bg_shapefiles_2020"


def main(rel_path):

    out_path = f"{rel_path}/{OUTDIR}"
    os.makedirs(f"{out_path}/{CONCAT_FOLDER}", exist_ok=True)

    # load state list
    state_lkup_df = load_summary_df(STATE_TABLE_NAME, rel_path=rel_path)
    state_list = state_lkup_df['STATE']

    print("Reading in state shapefiles...")
    state_gdf_list = []
    # CHANGE BACK!
    for state_code in tqdm(state_list, desc="states"):
        state_path = f"{out_path}/{STATE_FOLDER}/{state_code}"
        state_gdf = gpd.read_file(state_path)
        state_gdf_list.append(state_gdf)

    print("Combining & saving to disc")
    us_gdf = pd.concat(state_gdf_list, axis=0)
    us_gdf.to_file(f"{out_path}/{CONCAT_FOLDER}/us")

    print("Deleting state shapefiles")
    for state_code in state_list:
        state_path = f"{out_path}/{STATE_FOLDER}/{state_code}"
        shutil.rmtree(state_path)
    
    print("")


if __name__ == "__main__":
    main(REL_PATH)