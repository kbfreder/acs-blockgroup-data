
import os, sys
import argparse

import pandas as pd
from functools import reduce
import numpy as np



from process_bg_tables.base import generate_bg_fips_data, generate_state_fips_data
# from fetch_lat_lon_data import get_lat_lon_data
from process_bg_tables.population import get_total_pop, get_non_instit_pop #get_population_data 
from process_bg_tables.households import get_all_household_data # checked
from process_bg_tables.age import generate_age_prop_data, get_median_age #get_all_age_data
from process_bg_tables.race import get_race_pops, get_hispanic_pop
from process_bg_tables.misc_independent import (
    get_median_income, get_grp_qrtrs, get_military_employed
)
from process_bg_tables.misc_pop import get_all_misc_pop_data

from process_bg_tables.util import (
    extract_bg_fips_from_geo_id,
    load_summary_df,
    save_summary_df
)
from process_bg_tables.configs import (
    LAT_LON_FILENAME,
    ACS_BG_FILENAME,
    BG_TABLE_KEY_COL
)

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--from-step",
        help="Which step to resume processing at.",
        type=int,
        default=0,
        required=False,
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

    # --------------------
    # MERGE ACS TABLE DATA TOGETHER
    # --------------------
    # print("Checking ACS BG data")
    # bad_cnt = 0
    # for df in df_list:
    #     if BG_TABLE_KEY_COL not in df.columns:
    #         bad_cnt += 1
    #         print(df.head())
    #         print("")
    
    # if bad_cnt > 0:
    #     sys.exit(1)

    print("Merging ACS BG data")
    acs_df = reduce(
        lambda df1, df2: df1.merge(df2, on=BG_TABLE_KEY_COL, how="outer"), 
        df_list)
    acs_df = extract_bg_fips_from_geo_id(acs_df)
    save_summary_df(acs_df, "acs_data_step_1")
    return acs_df


# =========================
# MAIN
# =========================

if __name__ == "__main__":
    args = parse_args()
    step = args.from_step
    
    # PROCESS "RAW" ACS TABLE DATA
    # --------------------
    if step < 1:
        acs_df = process_acs_bg_tables()
    else:
        print("Loading step 1 data")
        acs_df = load_summary_df("acs_data_step_1")
    

    # LOAD / GENERATE OTHER "BASE" data
    # --------------------
    if step < 2:
        print("Generating FIPS data")
        bg_geo_df = generate_bg_fips_data()

        # lat/lon
        ## For now, require this be run separately, as it involves state-by-state downloads 
        ## and can thus take a while. Here, we just load the output of `fetch_lat_lon_data.py`
        print("Fetching lat/lon data")
        lat_lon_df = load_summary_df(LAT_LON_FILENAME)

        # MERGE AGAIN
        # -----------------
        print("Merging & saving")
        acs_ext_df = bg_geo_df.merge(acs_df, on='bg_fips')
        acs_ext_df = acs_ext_df.merge(lat_lon_df, on='bg_fips')

        # save_summary_df(acs_ext_df, ACS_BG_FILENAME)
        save_summary_df(acs_ext_df, "acs_data_step_2")
    else:
        print("Loading step 2 data")
        acs_ext_df = load_summary_df("acs_data_step_2")

    # --------------------
    # LOAD & MERGE CENSUS PLANNING DATABASE (CPDB)
    ## Download from here: https://www2.census.gov/adrm/PDB/2022/pdb2022bg.csv
    ## Or Google for latest/updated location
    # --------------------

    area_col = 'LAND_AREA' # alt: 'area_sqmile'
    median_age_col = 'median_age' # from ACS data / alt: 'Median_Age_ACS_16_20' from CPDB
    
    sources = ['acs', 'cpdb']
    non_inst_pop_src = 'acs' 

    if step < 3:
        print("Loading & Processing Census Planning Database data")
        pdb_df = pd.read_csv("../data/pdb2022bg.csv", sep=",", dtype='object')
        # convert data types
        pdb_dtype_dict = {
            'GIDBG': 'object',
            'LAND_AREA': 'float',
            'AIAN_LAND': 'int',
            'Tot_Population_CEN_2020': 'int',
            # 'Median_Age_ACS_16_20': 'float',
            'Tot_GQ_CEN_2020': 'int', # keep for now, to cross-check with tract-level ACS data
            'Inst_GQ_CEN_2020': 'int',
            'Non_Inst_GQ_CEN_2020': 'int',
            # 'pct_Tot_GQ_CEN_2020': 'float',
            # 'pct_Inst_GQ_CEN_2020': 'float',
            # 'pct_Non_Inst_GQ_CEN_2020': 'float'
        }
        pdb_cols = pdb_dtype_dict.keys()
        pdb_df = pdb_df.astype(pdb_dtype_dict)
        
        # note: we use an inner join -- this ignores the CT mis-match for now
        # TODO: figure out CT geo's
        bg_df = acs_ext_df.merge(pdb_df[pdb_cols], 
                                 how='inner',
                                 left_on='bg_fips', right_on='GIDBG')
        # --------------------
        # DERIVE OTHER ATTRIBUTES
        # --------------------
        print("Deriving other attributes")
    
        bg_df['pop_density_sqmile'] = bg_df['population'] / bg_df[area_col]
        # area from TigerWeb can be NaN, so fillna
        bg_df['pop_density_sqmile'] = bg_df['pop_density_sqmile'].fillna(0)
            ## alt: we could be selective about how/why Na's get filled:
            # np.where(acs_df['population'] == 0, 0, acs_df['pop_density_sqmile'])

        bg_df['median_age'] = bg_df[median_age_col]

        bg_df['pct_inst_groupquarters_2020'] = bg_df['Inst_GQ_CEN_2020'] / bg_df['Tot_Population_CEN_2020']
        bg_df['pct_non_inst_groupquarters_2020'] = bg_df['Non_Inst_GQ_CEN_2020'] / bg_df['Tot_Population_CEN_2020']
        if non_inst_pop_src == 'cpdb':
            bg_df['non_institutionized_pop'] = round(
                bg_df['population'] * (1-bg_df['pct_inst_groupquarters_2020']), 0)

        bg_df = bg_df.rename(columns={
            'AIAN_LAND': 'amindian_aknative_hawaiiannative_land_flag'
        })
        print("Saving step 3 data")
        save_summary_df(bg_df, "acs_data_step_3")
    else:
        print("Loading step 3 data")
        bg_df = load_summary_df("acs_data_step_3")


    # compute tract things
    print("Deriving Tract-level data")
    bg_df['tract_pop'] = bg_df.groupby("census_tract_fips")['population'].transform("sum")
    bg_df['tract_pop_density_sqmile'] = bg_df['tract_pop'] / bg_df[area_col]

    ## TODO: compare this to tract-level data
    bg_df['tract_grp_quarters'] = bg_df.groupby("census_tract_fips")['grp_quarters_pop'].transform("sum")
    bg_df['pct_tract_groupqtrs'] = bg_df['tract_grp_quarters'] / bg_df['tract_pop']

    bg_df['tract_military_employed'] = bg_df.groupby('census_tract_fips')['military_employed'].transform('sum')
    bg_df['pct_tract_military_employed'] = bg_df['tract_military_employed'] / bg_df['tract_pop']

    bg_df['military_base_flag'] = np.where(
        (bg_df['pct_tract_military_employed'] > 0.1) & (bg_df['pct_non_inst_groupquarters_2020'] > 0.1),
        1, 0)

    # drop cols we no longer need
    drop_cols = ['area_sqmile',
                 'tract_pop', 'grp_quarters_pop', 'tract_grp_quarters',
                 'military_employed', 'tract_military_employed',
                 'GIDBG',
                 'LAND_AREA', 'Inst_GQ_CEN_2020', 'Non_Inst_GQ_CEN_2020', 'Tot_Population_CEN_2020', 
                 'Tot_GQ_CEN_2020', 
                 ]
    print("Saving final data")
    save_summary_df(bg_df.drop(columns=drop_cols), ACS_BG_FILENAME)