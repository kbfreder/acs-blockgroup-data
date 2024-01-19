
import pandas as pd

from .util import save_summary_df, load_csv_with_dtypes
from configs import STATE_TABLE_NAME, GEO_FILE_PATH


def generate_bg_fips_data():
    geo_df = load_csv_with_dtypes(GEO_FILE_PATH)
    bg_cols = ['STATE', 'COUNTY', 'TRACT', 'BLKGRP', ]
    block_geo_df = geo_df[geo_df['SUMLEVEL'] == '150'][bg_cols]
    block_geo_df['bg_fips'] = (block_geo_df['STATE'] 
                               + block_geo_df['COUNTY'] 
                               + block_geo_df['TRACT'] 
                               + block_geo_df['BLKGRP']
    )
    block_geo_df['census_tract_fips'] = (block_geo_df['STATE'] 
                                        + block_geo_df['COUNTY'] 
                                        + block_geo_df['TRACT']
    )
    block_geo_df['county_fips'] = (block_geo_df['STATE'] 
                                   + block_geo_df['COUNTY']
    )
    
    # get county name
    cnty_cols = ['STATE', 'COUNTY', 'NAME']
    county_geo_df = geo_df[geo_df['SUMLEVEL'] == '050'][cnty_cols]
    county_geo_df['county_fips'] = (county_geo_df['STATE'] 
                                + county_geo_df['COUNTY']
    )
    # get state names
    state_geo_df = generate_state_fips_data(geo_df)

    # merge 
    bg_cnty_df = block_geo_df.merge(county_geo_df[['county_fips', 'NAME']],
                                    on='county_fips', how='left')

    bg_cnty_st_df = bg_cnty_df.merge(state_geo_df[['STUSAB', 'STATE']], on='STATE')

    bg_clean_df = (bg_cnty_st_df
                   .rename(columns={'STATE': 'state_fips', 
                                    'NAME': 'county_name',
                                    'STUSAB': 'state'})
                    .drop(columns=['COUNTY', 'TRACT', 'BLKGRP'])
    )
    
    # reorder columns for my sanity
    bg_clean_df = bg_clean_df[['bg_fips', 'census_tract_fips', 'county_fips',
                               'state_fips', 'state', 'county_name']]
    
    return bg_clean_df
    

def generate_state_fips_data(save_file=False, rel_path="../..", geo_df=None):
    if geo_df is None:
        geo_df = load_csv_with_dtypes(GEO_FILE_PATH, rel_path)
    state_cols = ['STUSAB', 'STATE', 'NAME']
    # note: need filter on `COMPONENET`, otherwise get >600 entries
    state_geo_df = geo_df[(geo_df['SUMLEVEL'] == '040') 
                          & (geo_df['COMPONENT'] == '00')][state_cols]
    state_geo_df.reset_index(inplace=True, drop=True)

    if save_file:
        save_summary_df(state_geo_df, STATE_TABLE_NAME, rel_path)
    else:
        return state_geo_df
    
