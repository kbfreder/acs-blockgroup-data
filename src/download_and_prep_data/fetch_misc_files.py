
import sys
import os
import pandas as pd

sys.path.append("..")
from configs import (
    CPDB_PATH,
    CT_CW_PATH,
    GEO_FILE_PATH_NO_EXT, 
    GEO_FILE_STUB,
    YEAR
)
from process_bg_tables.util import save_csv_and_dtypes

# path of this file, relative to parent folder of project/repo
REL_PATH = "../.."

def download_geo_documentation(rel_path):
    print("Retrieving Geo Documentation")
    path = f"https://www2.census.gov/programs-surveys/acs/summary_file/{YEAR}/table-based-SF/documentation/{GEO_FILE_STUB}.txt"
    geo_df = pd.read_csv(path, sep="|", dtype="str")
    # save with data-types, since we use this as-is later (no consideration for string vs number)
    save_csv_and_dtypes(geo_df, GEO_FILE_PATH_NO_EXT, rel_path)


def _simple_get_csv(rel_path, uri, save_path, name):
    print(f"Retrieving {name}")
    df = pd.read_csv(uri, dtype="str")
    full_save_path = f"{rel_path}/{save_path}"
    # OK to simply use `to_csv`, since we load/use these datasets in a datatype-conscious manner
    df.to_csv(f"{full_save_path}", index=False)


dataset_dict = {
    "cpdb": {
        "name": "Census Planning Database",
        "uri": "https://www2.census.gov/adrm/PDB/2022/pdb2022bg.csv",
        "save_path": CPDB_PATH
    },
    "ct_cw": {
        "name": "CT Crosswalk",
        "uri": "https://raw.githubusercontent.com/CT-Data-Collaborative/2022-tract-crosswalk/main/2022tractcrosswalk.csv",
        "save_path": CT_CW_PATH
    },

}

def main(rel_path):
    download_geo_documentation(rel_path)
    for dataset, ds_info in dataset_dict.items():
        _simple_get_csv(rel_path, **ds_info)


if __name__ == "__main__":
    main(REL_PATH)