
import pandas as pd

from .util import save_summary_df
from .configs import STATE_TABLE_NAME, YEAR, DATASET_YRS

path = F"https://www2.census.gov/programs-surveys/acs/summary_file/{YEAR}/table-based-SF/documentation/Geos2022{DATASET_YRS}YR.txt"

geo_df = pd.read_csv(path, sep="|", dtype="str")


def generate_bg_fips_data():

    bg_cols = ['STATE', 'COUNTY', 'TRACT', 'BLKGRP', 'NAME',]
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

    return block_geo_df
    

def generate_state_fips_data(save_file=False):
    state_cols = ['STUSAB', 'STATE', 'NAME']
    # note: need filter on `COMPONENET`, otherwise get >600 entries
    state_geo_df = geo_df[(geo_df['SUMLEVEL'] == '040') 
                          & (geo_df['COMPONENT'] == '00')][state_cols]
    state_geo_df.reset_index(inplace=True, drop=True)

    if save_file:
        save_summary_df(state_geo_df, STATE_TABLE_NAME)
    else:
        return state_geo_df
    
