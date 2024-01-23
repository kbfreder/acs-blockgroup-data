import os, sys
import argparse

import pandas as pd
import numpy as np

sys.path.append("..")
from process_bg_tables.util import save_csv_and_dtypes
from configs import (BG_ZIP_RAW_PATH, BG_ZIP_DEDUP_PATH, 
                     TRACT_ZIP_PATH,
                     TRACT_ZIP_DEDUP_PATH,
                     ZIP_SOURCE
)


# path of this file, relative to parent folder of project/repo
REL_PATH = "../.."

def main(rel_path, zip_source):
    if zip_source == 'tract':
        print("Preparing tract-zip crosswalk data")
        tract_zip_df = pd.read_excel(f"{REL_PATH}/{TRACT_ZIP_PATH}", 
                                    dtype={'TRACT': 'object', 'ZIP': 'object'})
        # assign zip with largest pop % to tract
        max_ratio_idxs = tract_zip_df.groupby('TRACT')['RES_RATIO'].idxmax()
        tract_zip_dedup = tract_zip_df.loc[max_ratio_idxs]
        tract_zip_dedup = tract_zip_dedup[['TRACT', 'ZIP']].rename(
                columns={'TRACT': 'census_tract_fips', 'ZIP': 'zip'})
        save_csv_and_dtypes(tract_zip_dedup, TRACT_ZIP_DEDUP_PATH, rel_path)

    elif zip_source == 'blockgroup':
        print("Preparing blockgroup-zip crosswalk data")
        alt_cw_df = pd.read_csv(
            f"{rel_path}/{BG_ZIP_RAW_PATH}",
            dtype={'county': 'object', 'tract': 'object', 'blockgroup': 'object'},
            skiprows=[1],
            encoding="ISO-8859-1" # otherwise get error:
            # UnicodeDecodeError: 'utf-8' codec can't decode byte 0xf1 in position 113110: invalid continuation byte
        )

        # extract bg_fips
        alt_cw_df['bg_fips'] = alt_cw_df['county'] + alt_cw_df['tract'] + alt_cw_df['blockgroup']
        alt_cw_df['bg_fips'] = alt_cw_df['bg_fips'].str.replace('.', '')

        # assign zip with largest pop % 
        max_ratio_idxs = alt_cw_df.groupby('bg_fips')['afact'].idxmax()
        alt_cw_single_zip = alt_cw_df.loc[max_ratio_idxs]

        bg_zip_df = alt_cw_single_zip[['bg_fips', 'zcta']].rename(
            columns={'zcta': 'zip'}
        )
        save_csv_and_dtypes(bg_zip_df, BG_ZIP_DEDUP_PATH, rel_path)


if __name__ == "__main__":
    main(REL_PATH, ZIP_SOURCE)