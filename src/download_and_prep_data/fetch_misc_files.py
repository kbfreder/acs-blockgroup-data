
import sys
# import os
import pandas as pd

sys.path.append("..")
from configs import (
    CPDB_PATH,
    CT_CW_RAW_PATH,
    CT_CW_PROC_PATH_NO_EXT,
    GEO_FILE_PATH_NO_EXT, 
    GEO_FILE_STUB,
    SHELL_DF_PATH,
    YEAR
)
from process_bg_tables.util import save_csv_and_dtypes

# path of this file, relative to parent folder of project/repo
REL_PATH = "../.."

dataset_dict = {
    # "cpdb": {
    #     "name": "Census Planning Database",
    #     "uri": "https://www2.census.gov/adrm/PDB/2022/pdb2022bg.csv",
    #     "save_path": CPDB_PATH
    # },
    "ct_cw": {
        "name": "CT Crosswalk",
        "uri": "https://raw.githubusercontent.com/CT-Data-Collaborative/2022-block-crosswalk/main/2022blockcrosswalk.csv",
        "save_path": CT_CW_RAW_PATH,
    },
    # "shells": {
    #     "name": "Table Shells",
    #     "uri": "https://www2.census.gov/programs-surveys/acs/summary_file/2022/table-based-SF/documentation/ACS20225YR_Table_Shells.txt",
    #     "save_path": SHELL_DF_PATH,
    #     "sep": "|"
    # }
}

def download_geo_documentation(rel_path):
    print("Retrieving Geo Documentation")
    path = f"https://www2.census.gov/programs-surveys/acs/summary_file/{YEAR}/table-based-SF/documentation/{GEO_FILE_STUB}.txt"
    geo_df = pd.read_csv(path, sep="|", dtype="str")
    # save with data-types, since we use this as-is later (no consideration for string vs number)
    save_csv_and_dtypes(geo_df, GEO_FILE_PATH_NO_EXT, rel_path)


def _simple_get_csv(rel_path, uri, save_path, name, sep=","):
    print(f"Retrieving {name}")
    df = pd.read_csv(uri, dtype="str", sep=sep)

    full_save_path = f"{rel_path}/{save_path}"
    # OK to simply use `to_csv`, since we load/use these datasets in a datatype-conscious manner
    df.to_csv(f"{full_save_path}", index=False)


def prep_ct_crosswalk():
    """Load and process data that cross-walks old FIPS to new FIPS for CT entities.
    """
    print("Processing CT Crosswalk file")
    cw_df = pd.read_csv(f"{REL_PATH}/{CT_CW_RAW_PATH}", dtype="object")

    # need to 0-pad some numbers:
    pad_dict = {
        'block_fips_2020': 15,
        'block_fips_2022': 15,
        'county_fips_2020': 5,
        'ce_fips_2022': 5,
        'zip5': 5
    }
    for col, pad in pad_dict.items():
        cw_df[col] = cw_df[col].apply(lambda x: x.zfill(pad))
        
    # data is at block level; we want blockgroup
    cw_df['bg_fips_2020'] = cw_df['block_fips_2020'].str[:-3]
    cw_df['bg_fips_2022'] = cw_df['block_fips_2022'].str[:-3]
    ## Note: I confirmed that there is only 1 zip or county per bg,
    ## so we don't need to use % land area for mapping
    cw_df_bg = cw_df[['bg_fips_2020', 'bg_fips_2022', 'zip5',
                      'county_fips_2020', 'ce_fips_2022', ]].drop_duplicates()

    # return cw_df_bg
    save_csv_and_dtypes(cw_df_bg, CT_CW_PROC_PATH_NO_EXT, REL_PATH)


def main(rel_path):
    # download_geo_documentation(rel_path)
    for dataset, ds_info in dataset_dict.items():
        _simple_get_csv(rel_path, **ds_info)
    prep_ct_crosswalk()


if __name__ == "__main__":
    main(REL_PATH)