
import os, sys

import pandas as pd
from fuzzywuzzy import fuzz
from numbers import Number

import numpy as np
import re
import pickle


from base import generate_base_data
from age import get_all_age_data
from process_bg_tables.misc_pop import get_all_misc_data
from util import (
    merge_bg_table_with_summary_df,
    save_summary_df
)


# year of ACS data
YEAR = 2022

# # Not sure if I need any of this...
# # ----------------------------------
# shell_ftp_dir = "https://www2.census.gov/programs-surveys/acs/summary_file/2022/table-based-SF/documentation/ACS20225YR_Table_Shells.txt"
# shell_df = pd.read_csv(shell_ftp_dir, sep="|", )

# # just pull out table names
# table_list_df = shell_df[['Table ID', 'Title', 'Universe', 'Type']].drop_duplicates()
# table_list = table_list_df['Title'].to_list()

# # filter on those that contain block group data (which we downloaded from the census website via FTP, using a script)
# file_list = os.listdir("../data/sumlevel=150/")
# bg_table_id_list = [re.search(r"[bc]\d{4,5}\w?", f).group().upper() for f in file_list]
# bg_table_list_df = table_list_df[table_list_df['Table ID'].isin(bg_table_id_list)]
# bg_table_list = bg_table_list_df['Title'].to_list()

# # just pull out universe names
# universe_list_df = bg_table_list_df.groupby(['Universe', 'Type']).agg({
#     'Title': list,
#     'Table ID': list
# })
# universe_list_df.reset_index(inplace=True, drop=False)
# universe_list = universe_list_df['Universe'].to_list()


class DatasetInfo:
    def __init__(self, data_name, prereqs, data_func):
        self.name = data_name
        self.prereqs = prereqs
        self.func = data_func
    

class ACSSummaryData:
    def __init__(self):
        self.summary_df = generate_base_data()
        self.cols_to_keep = list(self.sdf.columns())
        self.data_processed_list = ['base']
        self.init_datasets()


    def init_datasets(self):
        self.data_info = {
            'lat_lon': DatasetInfo('lat_lon', None, get_lat_lon_data),
            'population': DatasetInfo('population', ['lat_lon'], get_population_data),
            'misc_pop': DatasetInfo('misc_pop', ['population'], get_all_misc_data),
            'age': DatasetInfo('age', None, get_all_age_data),
            'households': DatasetInfo('households', None, get_household_data)
        }


    def _process_and_merge_data(self, data_name):
        data_obj = self.data_info.get(data_name)
        if data_obj is None:
            print(f"Data Name {data_name} not found in class")
            return -1

        if data_name not in self.data_processed_list:
            data_prereqs = data_obj.prereqs
            if data_prereqs is not None:
                for prereq in data_prereqs:
                    self._process_and_merge_data(prereq)

            new_df, new_cols = data_obj.func(summary_df=self.summary_df)
            self.summary_df, self.cols_to_keep = (
                merge_bg_table_with_summary_df(
                    new_df, self.summary_df, self.cols_to_keep + new_cols
                )
            )

    def process_all_data(self):
        data_to_process_in_order = [
            'lat_lon', 'population', 'age', 'households',
            'misc_pop'
        ]
        for data_name in data_to_process_in_order:
            self._process_and_merge_data(data_name)

        save_summary_df(self.summary_df, f"acs_summary_data_{YEAR}")


# =========================
# MAIN
# =========================

if __name__ == "__main__":
    acs_data = ACSSummaryData()
    acs_data.process_all_data()