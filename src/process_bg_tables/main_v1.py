
import os, sys

import pandas as pd
from fuzzywuzzy import fuzz
from numbers import Number
from collections import OrderedDict

import numpy as np
import re
import pickle


from base import Geos 
from fetch_lat_lon_data import get_lat_lon_data
from population import get_population_data 
from households import get_all_household_data # checked
from age import get_all_age_data
from misc_pop import get_all_misc_pop_data

from util import (
    load_summary_df,
    merge_bg_table_with_summary_df,
    save_summary_df
)
from configs import (
    BASE_TABLE_NAME,
    STATE_TABLE_NAME,
    YEAR
)

class DatasetInfo:
    def __init__(self, data_name, prereqs, data_func):
        self.name = data_name
        self.prereqs = prereqs
        self.func = data_func
    

class ACSSummaryData:

    def __init__(self):
        # self.summary_df = generate_base_data()
        # self.cols_to_keep = list(self.sdf.columns())
        self.data_processed_list = []
        self.generate_base_data()
        self.init_datasets()


    def generate_base_data(self):
        if 'base' not in self.data_processed_list:
            base_obj = Geos
            self.summary_df = base_obj.generate_bg_fips_data(save_file=True)
            self.state_df = base_obj.generate_state_fips_data(save_file=True)
            self.data_processed_list.append('base')
        else:
            self.summary_df = load_summary_df(BASE_TABLE_NAME)
            self.state_df = load_summary_df(STATE_TABLE_NAME)

    def init_datasets(self):
        self.data_info = OrderedDict([
            ('base', DatasetInfo('base', None, self.generate_base_data)),
            ('lat_lon', DatasetInfo('lat_lon', 'base', get_lat_lon_data)),
            ('population', DatasetInfo('population', ['lat_lon'], get_population_data)),
            ('misc_pop', DatasetInfo('misc_pop', ['population'], get_all_misc_pop_data)),
            ('households', DatasetInfo('households', None, get_all_household_data))
            ('age', DatasetInfo('age', None, get_all_age_data)),
        ])

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

            new_df = data_obj.func(summary_df=self.summary_df)
            self.summary_df = merge_bg_table_with_summary_df(
                new_df, self.summary_df)

    def process_all_data(self):
        data_to_process_in_order = [
            'lat_lon', 'population', 'age', 'households', 'misc_pop'
        ]
        for data_name in data_to_process_in_order:
            self._process_and_merge_data(data_name)

        save_summary_df(self.summary_df, f"acs_summary_data_{YEAR}")


    def process_from_step(self, step):
        pass


# =========================
# MAIN
# =========================

# process all the data, in one fell swoop!
if __name__ == "__main__":
    acs_data = ACSSummaryData()
    acs_data.process_all_data()