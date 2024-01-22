
import os, sys
import argparse

import pandas as pd
from functools import reduce
import numpy as np

from process_bg_tables.population import get_total_pop, get_non_instit_pop
from process_bg_tables.households import get_all_household_data
from process_bg_tables.age import generate_age_prop_data, get_median_age
from process_bg_tables.race import get_race_pops, get_hispanic_pop
from process_bg_tables.misc_independent import (
    get_median_income, get_grp_qrtrs, get_military_employed
)
from process_bg_tables.misc_pop import get_all_misc_pop_data
from process_bg_tables.fips_names import generate_bg_fips_data

from process_bg_tables.util import (
    extract_bg_fips_from_geo_id,
    load_summary_df,
    load_csv_with_dtypes,
    save_summary_df,
    save_csv_and_dtypes
)
from configs import (
    LAT_LON_FILENAME,
    ACS_BG_FILENAME, FINAL_OUTPUT_DIR,
    BG_TABLE_KEY_COL,
    CPDB_PATH, 
    CT_CW_PATH,
    TRACT_ZIP_PATH,
    MSA_PATH, MSA_SHEET,
    MIL_GEO_IND_PATH
)

# relative path to main level of project
REL_PATH = ".."
MAX_STEP = 6


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--from-step",
        help="Which step to resume processing at.",
        type=int,
        default=0,
        required=False,
        choices=range(1,MAX_STEP)
    )
    args = parser.parse_args()
    return args


def process_acs_bg_tables():
    df_list = []
    
    # population
    print("Generating population data")
    pop_df = get_total_pop()
    df_list.append(pop_df)
    df_list.append(get_non_instit_pop())

    # households
    print("Generating household data")
    df_list.append(get_all_household_data())

    # age
    print("Generating age data")
    df_list.append(generate_age_prop_data())
    df_list.append(get_median_age())    

    # race
    print("Generating race data")
    df_list.append(get_race_pops())
    df_list.append(get_hispanic_pop())

    # misc/other - independent from other variables
    print("Generating misc data")
    df_list.append(get_median_income())
    df_list.append(get_grp_qrtrs())
    df_list.append(get_military_employed())

    # misc/other - potentially dependent on other variables
    ## for some, we can derive a percent or proprotion either from 
    ## the total population, or from total for the "Universe" of the table
    df_list.append(get_all_misc_pop_data(pop_df=pop_df))

    print("Merging ACS Blockgroup data")
    acs_df = reduce(
        lambda df1, df2: df1.merge(df2, on=BG_TABLE_KEY_COL, how="outer"), 
        df_list)
    acs_df = extract_bg_fips_from_geo_id(acs_df)
    print("Saving step 1 data")
    save_summary_df(acs_df, "acs_data_step_1")
    return acs_df


def crosswalk_ct_fips(pdb_df):
    """Cross-walk old FIPS to new FIPS for CT entities.
    """
    # Block-to-block crosswalk data downloaded from here: 
    # https://github.com/CT-Data-Collaborative/2022-tract-crosswalk
    cw_df = pd.read_csv(f"{REL_PATH}/{CT_CW_PATH}", dtype="object")

    # need to 0-pad fips numbers:
    cw_df['block_fips_2020'] = cw_df['block_fips_2020'].apply(lambda x: x.zfill(15))
    cw_df['block_fips_2022'] = cw_df['block_fips_2022'].apply(lambda x: x.zfill(15))
    # data is at block level; we want blockgroup
    cw_df['bg_fips_2020'] = cw_df['block_fips_2020'].str[:-3]
    cw_df['bg_fips_2022'] = cw_df['block_fips_2022'].str[:-3]
    cw_df_bg = cw_df[['bg_fips_2020', 'bg_fips_2022']].drop_duplicates()
    # merge with Planning Database
    merge_df = pdb_df.merge(cw_df_bg, left_on='GIDBG', right_on='bg_fips_2020', how='left')
    merge_df['GIDBG'] = np.where(merge_df['bg_fips_2020'].isna(), 
                                 merge_df['GIDBG'], merge_df['bg_fips_2022'])
    # clean up
    return merge_df.drop(columns=['bg_fips_2020', 'bg_fips_2022'])


# =========================
# MAIN
# =========================

if __name__ == "__main__":
    args = parse_args()
    step = args.from_step
    
    # --------------------
    # 1. PROCESS ACS BG TABLE DATA

        ## ACS Summary File tables must be downloaded first, using the
        ## `fetch_acs_summary_files.py` script.
    # --------------------
    
    if step < 1:
        acs_df = process_acs_bg_tables()
    elif step == 1:
        print("Loading step 1 data")
        acs_df = load_summary_df("acs_data_step_1")
    
    
    # --------------------
    # 2. FIPS and LAT/LON 
        ## See `base.py` for how FIPS data are derived.
        ## Lat/Lon data is downloaded/compiled by a separate script (`fetch_lat_lon_data.py`)
        ## Here, we just load the output.
    # --------------------
    if step < 2:
        # "FIPS" data (derive bg, census tract, county FIPS)
        print("Generating FIPS data")
        bg_geo_df = generate_bg_fips_data()

        # lat/lon
        print("Fetching lat/lon data")
        lat_lon_df = load_summary_df(LAT_LON_FILENAME)

        # merge again
        print("Merging & saving")
        tmp_df1 = bg_geo_df.merge(acs_df, on='bg_fips')
        step_2_df = tmp_df1.merge(lat_lon_df, on='bg_fips')

        print("Saving step 2 data")
        save_summary_df(step_2_df, "acs_data_step_2")
    elif step == 2:
        print("Loading step 2 data")
        step_2_df = load_summary_df("acs_data_step_2")


    # --------------------
    # 3. CENSUS PLANNING DATABASE (CPDB)
        
        ## Download from here: https://www2.census.gov/adrm/PDB/2022/pdb2022bg.csv
        ## Or Google for latest/updated location
    # --------------------

    # these must be defined outside of if/else 
    area_col = 'LAND_AREA' # alt: 'area_sqmile'
    median_age_col = 'median_age' # from ACS data / alt: 'Median_Age_ACS_16_20' from CPDB
    
    sources = ['acs', 'cpdb']
    non_inst_pop_src = 'acs' 

    if step < 3:
        print("Loading & Processing Census Planning Database data")
        pdb_dtype_dict = {
            'GIDBG': 'object',
            'LAND_AREA': 'float',
            'AIAN_LAND': 'int',
            'Tot_Population_CEN_2020': 'int',
            'Median_Age_ACS_16_20': 'float',
            'Tot_GQ_CEN_2020': 'int', # pull in for now, to cross-check with tract-level ACS data
            'Inst_GQ_CEN_2020': 'int',
            'Non_Inst_GQ_CEN_2020': 'int',
        }
        pdb_cols = pdb_dtype_dict.keys()
        pdb_df = pd.read_csv(f"{REL_PATH}/{CPDB_PATH}", sep=",", dtype=pdb_dtype_dict)
        pdb_df = pdb_df[pdb_cols]

        # The Census implemented Connecticut's change from counties to "planning regions" in 2022.
        # The 2022 ACS data has the new FIPS numbers/designations, but the 2020 Planning Database
        # does not. We must therefore leverage a cross-walk to join the two datasets for the
        # state of CT.
        pdb_cw_df = crosswalk_ct_fips(pdb_df)

        # merge together the datasets
        bg_df = step_2_df.merge(pdb_cw_df, how='inner', left_on='bg_fips', right_on='GIDBG')


        # DERIVE ATTRIBUTES FROM CPDB et al
        # --------------------
        print("Deriving other attributes")
    
        bg_df['pop_density_sqmile'] = bg_df['population'] / bg_df[area_col]
        # area from TigerWeb can be NaN, so fillna
        bg_df['pop_density_sqmile'] = bg_df['pop_density_sqmile'].fillna(0)

        bg_df['median_age'] = bg_df[median_age_col]

        bg_df['pct_inst_groupquarters_2020'] = bg_df['Inst_GQ_CEN_2020'] / bg_df['Tot_Population_CEN_2020']
        bg_df['pct_non_inst_groupquarters_2020'] = bg_df['Non_Inst_GQ_CEN_2020'] / bg_df['Tot_Population_CEN_2020']
        if non_inst_pop_src == 'cpdb':
            bg_df['non_institutionized_pop'] = round(
                bg_df['population'] * (1-bg_df['pct_inst_groupquarters_2020']), 0)

        step_3_df = bg_df.rename(columns={
            'AIAN_LAND': 'amindian_aknative_hawaiiannative_land_flag'
        })
        print("Saving step 3 data")
        save_summary_df(step_3_df, "acs_data_step_3")
    elif step == 3:
        print("Loading step 3 data")
        step_3_df = load_summary_df("acs_data_step_3")

    step_3_drop_cols = [
        'GIDBG', 'Inst_GQ_CEN_2020', 'Non_Inst_GQ_CEN_2020', 
        'Tot_Population_CEN_2020', 'Tot_GQ_CEN_2020', 
        'Median_Age_ACS_16_20'
    ]

    # --------------------
    # 4. TRACT-LEVEL ATTRIBUTES

        ## These derived from rollups from blockgroup-level data.
        ## (Previously/alternatively, we could download tract-level data.)
    # --------------------
    if step < 4:
        bg_tract_df = step_3_df.drop(columns=step_3_drop_cols)

        print("Deriving Tract-level data")
        bg_tract_df['tract_pop'] = bg_tract_df.groupby("census_tract_fips")['population'].transform("sum")
        bg_tract_df['tract_pop_density_sqmile'] = bg_tract_df['tract_pop'] / bg_tract_df[area_col]

        bg_tract_df['tract_grp_quarters'] = bg_tract_df.groupby("census_tract_fips")['grp_quarters_pop'].transform("sum")
        bg_tract_df['pct_tract_groupqtrs'] = bg_tract_df['tract_grp_quarters'] / bg_tract_df['tract_pop']

        bg_tract_df['tract_military_employed'] = bg_tract_df.groupby('census_tract_fips')['military_employed'].transform('sum')
        bg_tract_df['pct_tract_military_employed'] = bg_tract_df['tract_military_employed'] / bg_tract_df['tract_pop']

        # TODO: decide whether to use this logic, or do a spatial join
        bg_tract_df['military_base_flag'] = np.where(
            (bg_tract_df['pct_tract_military_employed'] > 0.1) 
            & (bg_tract_df['pct_non_inst_groupquarters_2020'] > 0.1),
            1, 0)
        
        step_4_df = bg_tract_df.copy()
        print("Saving step 4 data")
        save_summary_df(step_4_df, "acs_data_step_4")
    elif step == 4:
        print("Loading step 4 data")
        step_4_df = load_summary_df("acs_data_step_4")
    
    step_4_drop_cols = ['area_sqmile','LAND_AREA',
                        'tract_pop', 'grp_quarters_pop', 'tract_grp_quarters',
                        'military_employed', 'tract_military_employed',
                        ]

    # --------------------
    # 5. ZIP CODE & MSA

        ## cross-walk between tract & zip code is available from HUD: 
        ## https://www.huduser.gov/portal/datasets/usps_crosswalk.html

        ## cross-walk between county & MSA (Metropolitan Statistical Areas) from BLS:
        ## https://www.bls.gov/cew/classifications/areas/county-msa-csa-crosswalk.htm
        
    # --------------------
    if step < 5:
        step_4_df = step_4_df.drop(columns=step_4_drop_cols)
        
        print("Merging ZIP & MSA data")
        tract_zip_df = pd.read_excel(f"{REL_PATH}/{TRACT_ZIP_PATH}", 
                                     dtype={'TRACT': 'object', 'ZIP': 'object'})
        # assign zip with largest pop % to tract
        max_ratio_idxs = tract_zip_df.groupby('TRACT')['RES_RATIO'].idxmax()
        tract_zip_dedup = tract_zip_df.loc[max_ratio_idxs]

        tmp_df1 = step_4_df.merge(
            tract_zip_dedup[['TRACT', 'ZIP']].rename(
                columns={'TRACT': 'census_tract_fips', 'ZIP': 'zip'}), 
                on='census_tract_fips', how='left')

        msa_df = pd.read_excel(f"{REL_PATH}/{MSA_PATH}", sheet_name=MSA_SHEET,
                               dtype={'County Code': 'object'})
        msa_df['county_fips'] = msa_df['County Code'].apply(lambda x: str(x).zfill(5))
        
        tmp_df2 = tmp_df1.merge(msa_df[['county_fips', 'MSA Code', 'MSA Title']], 
                                on='county_fips',  how='left')

        step_5_df = tmp_df2.rename(columns={'MSA Code': 'msa_code', 'MSA Title': 'MSA'})

        print("Saving step 5 data")
        save_summary_df(step_5_df, "acs_data_step_4")
    elif step == 5:
        print("Loading step 5 data")
        step_5_df = load_summary_df("acs_data_step_5")

    if step < 6:
        print("Merging in Military Base Geo indiciators")
        mil_geo_df = load_csv_with_dtypes(MIL_GEO_IND_PATH, REL_PATH)

        merge_df = step_5_df.merge(mil_geo_df, how='left',
                                   left_on='bg_fips', right_on='GEOID'
                                   )
        
        final_df = merge_df.drop(columns=['GEOID'])

        print("Saving Final data")
        save_csv_and_dtypes(final_df, f"{FINAL_OUTPUT_DIR}/{ACS_BG_FILENAME}", REL_PATH)
        print("Done!")