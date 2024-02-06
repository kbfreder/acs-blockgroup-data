
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
    load_checkpoint_df,
    load_csv_with_dtypes,
    save_checkpoint_df,
    save_csv_and_dtypes
)
from configs import (
    ACS_BG_FILENAME, 
    BG_TABLE_KEY_COL,
    BG_ZIP_DEDUP_PATH_NO_EXT,
    CPDB_PATH, 
    CT_CW_PATH,
    CT_MSA_CW_DICT,
    FINAL_OUTPUT_DIR,
    LAT_LON_PATH_NO_EXT,
    MSA_PATH, MSA_SHEET,
    MIL_GEO_IND_PATH_NO_EXT,
    TRACT_ZIP_DEDUP_PATH,
    ZIP_SOURCE
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
    save_checkpoint_df(acs_df, "acs_data_step_1")
    return acs_df


def load_planning_db():
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
    return pdb_df


def get_ct_crosswalk():
    """Get data that cross-walks old FIPS to new FIPS for CT entities.
    """
    # Block-to-block crosswalk data downloaded from here: 
    # https://github.com/CT-Data-Collaborative/2022-block-crosswalk
    cw_df = pd.read_csv(f"{REL_PATH}/{CT_CW_PATH}", dtype="object")

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
    ## confirmed that there is only 1 zip or county per bg
    cw_df_bg = cw_df[['bg_fips_2020', 'bg_fips_2022', 'zip5',
                      'county_fips_2020', 'ce_fips_2022', ]].drop_duplicates()

    return cw_df_bg


def derive_planning_db_attrs(pdb_df: pd.DataFrame, 
                             area_col: str, 
                             median_age_col: str,
                             use_cpdb_for_non_inst_pop: bool
                             ) -> (pd.DataFrame, list):
    pdb_df['pop_density_sqmile'] = pdb_df['population'] / pdb_df[area_col]
    # area from TigerWeb can be NaN, so fillna
    pdb_df['pop_density_sqmile'] = pdb_df['pop_density_sqmile'].fillna(0)

    pdb_df['median_age'] = pdb_df[median_age_col]

    pdb_df['pct_inst_groupquarters_2020'] = pdb_df['Inst_GQ_CEN_2020'] / pdb_df['Tot_Population_CEN_2020']
    pdb_df['pct_non_inst_groupquarters_2020'] = pdb_df['Non_Inst_GQ_CEN_2020'] / pdb_df['Tot_Population_CEN_2020']
    if use_cpdb_for_non_inst_pop:
        pdb_df['non_institutionized_pop'] = round(
            pdb_df['population'] * (1-pdb_df['pct_inst_groupquarters_2020']), 0)

    drop_cols = [
        'Inst_GQ_CEN_2020', 'Non_Inst_GQ_CEN_2020', 
        'Tot_Population_CEN_2020', 'Tot_GQ_CEN_2020', 
        'Median_Age_ACS_16_20'
    ]

    pdb_df = (pdb_df
              .rename(columns={'AIAN_LAND': 'amindian_aknative_hawaiiannative_land_flag'})
              .drop(columns=drop_cols)
    )

    return pdb_df


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
        acs_df = load_checkpoint_df("acs_data_step_1")
    
    
    # --------------------
    # 2. FIPS and LAT/LON 
        ## See `fips_names.py` for how FIPS data are derived.
        ## Lat/Lon data is downloaded/compiled by a separate script (`fetch_lat_lon_data.py`)
        ## Here, we just load the output.
    # --------------------
    if step < 2:
        # "FIPS" data (derive bg, census tract, county FIPS)
        print("Generating FIPS data")
        bg_geo_df = generate_bg_fips_data(REL_PATH)

        # lat/lon
        print("Fetching lat/lon data")
        # lat_lon_df = load_checkpoint_df(LAT_LON_PATH_NO_EXT, REL_PATH)
        lat_lon_df = load_csv_with_dtypes(LAT_LON_PATH_NO_EXT, REL_PATH)

        # merge again
        print("Merging & saving")
        tmp_df1 = bg_geo_df.merge(acs_df, on='bg_fips')
        step_2_df = tmp_df1.merge(lat_lon_df, on='bg_fips')

        print("Saving step 2 data")
        save_checkpoint_df(step_2_df, "acs_data_step_2")
    elif step == 2:
        print("Loading step 2 data")
        step_2_df = load_checkpoint_df("acs_data_step_2")


    # --------------------
    # 3. THINGS THAT REQUIRE THE CONNECTICUT CROSSWALK
        # The Census implemented Connecticut's change from counties to "planning regions" in 2022.
        # The 2022 ACS data has the new FIPS numbers/designations, but the 2020 Planning Database,
        # the Zip crosswalks, and the MSA crosswalk do not. We must therefore leverage a CT-specific 
        # cross-walk to join these two datasets for the state of CT.

        ## CENSUS PLANNING DATABASE (CPDB)
        ## Download from here: https://www2.census.gov/adrm/PDB/2022/pdb2022bg.csv
        ## Or Google for latest/updated location

        ## ZIP CODE
        ## cross-walk between tract & zip code is available from HUD: 
        ## https://www.huduser.gov/portal/datasets/usps_crosswalk.html
        ## alt: cross-walk between blockgroup & zip from MCDC:
        ## https://mcdc.missouri.edu/applications/geocorr2022.html

        ## MSA 
        ## cross-walk between county & MSA (Metropolitan Statistical Areas) from BLS:
        ## https://www.bls.gov/cew/classifications/areas/county-msa-csa-crosswalk.htm
        
    # --------------------

    # these must be defined outside of if/else 
    area_col = 'LAND_AREA' # from (?) / alt: 'area_sqmile' from ?
    median_age_col = 'median_age' # from ACS data / alt: 'Median_Age_ACS_16_20' from CPDB
    
    # whether to use Census Planning DB for the source of the non-institutionalized population
    # if False, use ACS data as source
    use_cpdb_for_non_inst_pop = False

    if step < 3:
        # CENSUS PLANNING DB
        # --------------------
        print("Loading & Processing Census Planning Database data")
        pdb_df = load_planning_db()

        ## This function returns old bg --> new bg crosswalk, plus zip and old & new county fips
        ct_cw_df = get_ct_crosswalk()

        # merge with Planning Database
        pdb_cw_df = pdb_df.merge(ct_cw_df, how='left',
                                 left_on='GIDBG', right_on='bg_fips_2020')
        pdb_cw_df['bg_fips'] = np.where(pdb_cw_df['bg_fips_2020'].isna(), 
                                     pdb_cw_df['GIDBG'], pdb_cw_df['bg_fips_2022'])
        pdb_cw_df = (pdb_cw_df
                    .drop(columns=['bg_fips_2020', 'bg_fips_2022', 'GIDBG'])
                    .rename(columns={'zip5': 'zip_ct'})
        )

        # merge back wtih ACS data
        pdb_merge_df = step_2_df.merge(pdb_cw_df, how='left', on='bg_fips')

        # derive attributes
        pdb_attr_df = derive_planning_db_attrs(
            pdb_merge_df, area_col, median_age_col, use_cpdb_for_non_inst_pop)


        # ZIP
        # ----------------
        print("Loading & merging Zip data")
        # zip crosswalk data has already been pre-processed
        # (see `src/download_and_prep_data/process_zip_data.py`)
        if ZIP_SOURCE == 'tract':
            tract_zip_dedup = load_csv_with_dtypes(TRACT_ZIP_DEDUP_PATH, REL_PATH)
            zip_merge_df = pdb_attr_df.merge(
                tract_zip_dedup, how='left', on='census_tract_fips')
        
        elif ZIP_SOURCE == 'blockgroup':
            bg_zip_dedup_df = load_csv_with_dtypes(BG_ZIP_DEDUP_PATH_NO_EXT, REL_PATH)
            zip_merge_df = pdb_attr_df.merge(
                bg_zip_dedup_df, on='bg_fips', how='left')

        # regardless of source, harmonize zip for CT
        zip_merge_df['zip'] = zip_merge_df['zip'].fillna(zip_merge_df['zip_ct'])
        zip_merge_df.drop(columns=['zip_ct'], inplace=True)

        # MSA data
        # --------------------
        print("Loading & merging MSA data")
        # manual download, so read in straight
        msa_df = pd.read_excel(f"{REL_PATH}/{MSA_PATH}", sheet_name=MSA_SHEET,
                               dtype={'County Code': 'object'})
        msa_df['county_fips'] = msa_df['County Code'].apply(lambda x: str(x).zfill(5))
        
        # this will result in 100% nulls for CT (which we fill in below)
        msa_merge_df = zip_merge_df.merge(msa_df[['county_fips', 'MSA Code', 'MSA Title']], 
                                on='county_fips',  how='left')
        msa_merge_df.rename(columns={'MSA Code': 'msa_code', 'MSA Title': 'MSA'}, inplace=True)

        # get MSA Title --> Code lookup
        msa_ct_df = msa_df[msa_df['county_fips'].str.startswith('09')]
        tmp_dict = msa_ct_df[['MSA Title', 'MSA Code']].drop_duplicates().to_dict(orient='records')
        ct_msa_title_code_dict = {d['MSA Title']: d['MSA Code'] for d in tmp_dict}

        # apply crosswalk/lookups to CT entries
        ## Note: the crosswalk between old & new CT counties/county equivalents is not a 1:1
        ## relationship, so using it to merge ACS data & MSA crosswalk results in duplicates
        ## in CT entries. Use a hand-defined crosswalk instead (`configs.CT_MSA_CW_DICT`)
        msa_merge_df['MSA'] = np.where(msa_merge_df['state_fips'] == '09', msa_merge_df['county_name'].map(CT_MSA_CW_DICT), msa_merge_df['MSA'])
        msa_merge_df['msa_code'] = np.where(msa_merge_df['state_fips'] == '09', msa_merge_df['MSA'].map(ct_msa_title_code_dict), msa_merge_df['msa_code'])

        step_3_df = msa_merge_df.drop(columns=['county_fips_2020', 'ce_fips_2022'])

        print("Saving step 3 data")
        save_checkpoint_df(step_3_df, "acs_data_step_3")
    elif step == 3:
        print("Loading step 3 data")
        step_3_df = load_checkpoint_df("acs_data_step_3")


    # --------------------
    # 4. TRACT-LEVEL ATTRIBUTES

        ## These are derived from rollups from blockgroup-level data.
        ## (Previously/alternatively, we could derive from tract-level data.)
    # --------------------
    if step < 4:
        bg_tract_df = step_3_df.copy()

        print("Deriving Tract-level data")
        bg_tract_df['tract_pop'] = bg_tract_df.groupby("census_tract_fips")['population'].transform("sum")
        bg_tract_df['tract_pop_density_sqmile'] = bg_tract_df['tract_pop'] / bg_tract_df[area_col]

        bg_tract_df['tract_grp_quarters'] = bg_tract_df.groupby("census_tract_fips")['grp_quarters_pop'].transform("sum")
        bg_tract_df['pct_tract_groupqtrs'] = bg_tract_df['tract_grp_quarters'] / bg_tract_df['tract_pop']

        bg_tract_df['tract_military_employed'] = bg_tract_df.groupby('census_tract_fips')['military_employed'].transform('sum')
        bg_tract_df['pct_tract_military_employed'] = bg_tract_df['tract_military_employed'] / bg_tract_df['tract_pop']

        # TODO: decide whether to use this logic, or do a spatial join
        ## for now, keep both
        bg_tract_df['military_base_flag'] = np.where(
            (bg_tract_df['pct_tract_military_employed'] > 0.1) 
            & (bg_tract_df['pct_non_inst_groupquarters_2020'] > 0.1),
            1, 0)
        
        step_4_drop_cols = ['area_sqmile','LAND_AREA',
                    'tract_pop', 'grp_quarters_pop', 'tract_grp_quarters',
                    'military_employed', 'tract_military_employed',
                    ]
        step_4_df = bg_tract_df.drop(columns=step_4_drop_cols)

        print("Saving step 4 data")
        save_checkpoint_df(step_4_df, "acs_data_step_4")
    elif step == 4:
        print("Loading step 4 data")
        step_4_df = load_checkpoint_df("acs_data_step_4")
    

    # --------------------
    # 5. MILITARY BASE INDICATORS

        ## These were derived from geospatial data.
    # --------------------
    if step < 5:
        print("Merging in Military Base Geo indiciators")
        mil_geo_df = load_csv_with_dtypes(MIL_GEO_IND_PATH_NO_EXT, REL_PATH)

        msa_merge_df = step_4_df.merge(mil_geo_df, how='left',
                                   left_on='bg_fips', right_on='GEOID'
                                   )
        
        final_df = msa_merge_df.drop(columns=['GEOID'])

        print("Saving Final data")
        save_csv_and_dtypes(final_df, f"{FINAL_OUTPUT_DIR}/{ACS_BG_FILENAME}", REL_PATH)
        print("Done!")