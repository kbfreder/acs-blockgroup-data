
import pandas as pd

from util import save_summary_df
from configs import BASE_TABLE_NAME, STATE_TABLE_NAME


class Geos:
    path = "https://www2.census.gov/programs-surveys/acs/summary_file/2022/table-based-SF/documentation/Geos20225YR.txt"

    def __init__(self):
        self.geo5_df = pd.read_csv(self.path, sep="|", dtype="str")

    def generate_bg_fips_data(self, save_file=False):
    
        bg_cols = ['STATE', 'COUNTY', 'TRACT', 'BLKGRP', 'NAME',]
        block_geo_df = self.geo5_df[self.geo5_df['SUMLEVEL'] == '150'][bg_cols]
        block_geo_df['bg_fips'] = block_geo_df['STATE'] + block_geo_df['COUNTY'] + block_geo_df['TRACT'] + block_geo_df['BLKGRP']
        block_geo_df['census_tract_fips'] = block_geo_df['STATE'] + block_geo_df['COUNTY'] + block_geo_df['TRACT']
        block_geo_df['county_fips'] = block_geo_df['STATE'] + block_geo_df['COUNTY']

        if save_file:
            save_summary_df(block_geo_df, BASE_TABLE_NAME)
        else:
            return block_geo_df
    

    def generate_state_fips_data(self, save_file=False):
        state_cols = ['STUSAB', 'STATE', 'NAME']
        # note: need filter on `COMPONENET`, otherwise get >600 entries
        state_geo_df = self.geo5_df[(self.geo5_df['SUMLEVEL'] == '040') & (self.geo5_df['COMPONENT'] == '00')][state_cols]
        state_geo_df.reset_index(inplace=True, drop=True)

        if save_file:
            save_summary_df(state_geo_df, STATE_TABLE_NAME)
        else:
            return state_geo_df
    
