
import os, sys

import pandas as pd
from fuzzywuzzy import fuzz
from numbers import Number
from collections import OrderedDict
from functools import reduce

import numpy as np
import re
import pickle


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
    merge_bg_table_with_summary_df,
    process_some_cols_bg_table,
    process_all_cols_bg_table,
    save_summary_df
)
from process_bg_tables.configs import (
    BASE_TABLE_NAME,
    LAT_LON_FILENAME,
    STATE_TABLE_NAME,
    YEAR
)

# =========================
# MAIN
# =========================

# process all the data, in one fell swoop!
if __name__ == "__main__":
    # --------------------
    # PROCESS "RAW" DATA
    # --------------------
    df_list = []
    
    # "base" / geo
    block_geo_df = generate_bg_fips_data()
    state_geo_df = generate_state_fips_data()
    state_abbrev_list = state_geo_df['STUSAB']
    df_list.append(block_geo_df)

    # lat/lon
    ## require this be run separately, as it involves state-by-state downloads and
    ## can thus take a while.
    ## Here, we just load the output of `fetch_lat_lon_data.py`
    lat_lon_df = load_summary_df(LAT_LON_FILENAME)
    df_list.append(lat_lon_df)

    # population

    ## option of keeping all functions in other files:
    # df_list.append(get_total_pop())
    # df_list.append(get_non_instit_pop())

    pop_df = get_total_pop()
    df_list.append(pop_df)
    df_list.append(get_non_instit_pop())

    ## or of calling the utility functions here:
    # pop_df = process_some_cols_bg_table(
    #     tbl_id='B01003', # Total Population
    #     col_idxs=[1], # Total
    #     new_col_names=['population'], 
    #     dtype='int'
    # )
    # non_inst_pop_df = process_some_cols_bg_table(
    #     tbl_id='B27010', # random table in 'Civilian noninstitutionalized population' universe
    #     col_idxs=[1], # Total
    #     new_col_names=['non_institutionized_pop'],
    #     dtype='int'
    # )
    # df_list.append(pop_df)
    # df_list.append(non_inst_pop_df)


    # households
    df_list.append(get_all_household_data())

    # age
    df_list.append(generate_age_prop_data())
    df_list.append(get_median_age())

    # race
    df_list.append(get_race_pops())
    df_list.append(get_hispanic_pop())

    # misc/other - independent from other variables
    df_list.append(get_median_income())
    df_list.append(get_grp_qrtrs())
    df_list.append(get_military_employed())


    # misc/other - potentially dependent on other variables
    ## for some, we can derive a percent or proprotion either from 
    ## the total population, or from total for the "Universe" of the table
    df_misc = get_all_misc_pop_data(pop_df)


    # --------------------
    # MERGE DATA TOGETHER
    # --------------------
    acs_df = reduce(lambda df1, df2: df1.merge(df2, on='GEO_ID'), df_list)
    acs_df = extract_bg_fips_from_geo_id(acs_df)



    # --------------------
    # DERIVE OTHER METRICS
    # --------------------
    acs_df['pop_density_sqmile'] = acs_df['population'] / acs_df['area_sqmile']
    # sometimes area is 0
    acs_df['pop_density_sqmile'] = np.where(acs_df['population'] == 0, 0, 
                                            acs_df['pop_density_sqmile'])

    ## tract things    
    acs_df['tract_pop'] = acs_df.groupby("census_tract_fips")['population'].transform("sum")
    
    acs_df['tract_pop_density_sqmile'] = acs_df['tract_pop'] / acs_df['area_sqmile']

    bg_df['tract_grp_quarters'] = bg_df.groupby("census_tract_fips")['grp_quarters_pop'].transform("sum")
    bg_df['pct_tract_groupqtrs'] = bg_df['tract_grp_quarters'] / bg_df['tract_pop']

    bg_df['tract_military_employed'] = bg_df.groupby('census_tract_fips')['military_employed'].transform('sum')
    bg_df['pct_tract_military_employed'] = bg_df['tract_military_employed'] / bg_df['tract_pop']

    
    # drop cols we no longer need
    drop_cols = ['area_sqmile', 'tract_pop', 'grp_quarters_pop', 'tract_grp_quarters',
                 'military_employed', 'tract_military_employed'
                 ]
    