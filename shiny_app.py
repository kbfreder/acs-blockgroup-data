"""Compare attribute between 2019 & 2022 datasets

2019 dataset is a sample (AK - AZ)
"""

import json
import pickle
import pandas as pd
import numpy as np

from shiny import App, ui, reactive, render
from shinywidgets import register_widget, output_widget, reactive_read
import seaborn as sns

# ==========================
# DATA LOADING / PREP
# ==========================

YEAR = 2022
DATASET_YRS = 5
ACS_BG_FILENAME = f"ACS_BG_DATA_{YEAR}"

def load_summary_df(checkpoint_name):
    file_stub = f"data/parsed_acs_data/{checkpoint_name}"
    with open(f"{file_stub}.pkl", "rb") as f:
        dtype_dict = pickle.load(f)
    summary_df = pd.read_csv(f"{file_stub}.csv", dtype=dtype_dict)
    return summary_df

attrs = ["population", "pop_density_sqmile", 
        "households", 'pct_hh_internet_subscription',
        'pct_hh_vehicle_ownership', 'avg_vehicles_per_hh', 'pct_hh_foodstamps',
        'pct_home_ownership', 
        "median_age", 'prop_age_under_18', 'prop_age_25_34', 'prop_age_35_44',
        'prop_age_45_54',  'prop_age_55_64', 'prop_age_65_74',
        'prop_female', 'race_pct_white',
        'race_pct_african_american', 'race_pct_nativeam', 'race_pct_asian',
        'race_pct_other', 'race_pct_hispanic', 
        'pct_college_enrolled', 'prop_college_educated', 'pct_married',
        'pct_tract_military_employed'
    ]
DEFAULT_ATTR = 'population'
ID_COL = 'bg_fips'
df_2022 = load_summary_df(ACS_BG_FILENAME)

# 2019 data sample
num_like_obj_cols = ['bg_fips', 'census_tract_fips', 'county_fips', 'zip']
df_2019 = pd.read_csv("data/blockgroup_sample_2019.txt", sep=",",
                     dtype=dict(zip(num_like_obj_cols, ['object']*len(num_like_obj_cols)))
                         )


# ==========================
# APP/DASHBOARD CODE
# ==========================

app_ui = ui.page_fluid(
    ui.row(
        ui.input_selectize(id="attr", label="Attribute",
                           choices=attrs, selected=DEFAULT_ATTR)
    ),
    ui.row(
        ui.column(6,
                  [ui.output_plot("scatter"),
                   ui.output_text_verbatim("corr")]
        ), 
        ui.column(6,
                  ui.output_plot("diff_histo")
        )
    ),
)

def server(input, output, session):
    @reactive.Calc
    def merge_dfs() -> pd.DataFrame:
        attr = input.attr()
        cols_to_merge = [ID_COL, attr]
        comp_df = df_2019[cols_to_merge].merge(
            df_2022[cols_to_merge],
            how='left', on=ID_COL, suffixes=['_19', '_22']
        )
        cols_in_comp = [f'{attr}_19', f'{attr}_22']
        comp_df_drop_na = comp_df.dropna(axis='index', subset=cols_in_comp)
        comp_df_drop_na['diff'] = comp_df_drop_na[f'{attr}_22'] - comp_df[f'{attr}_19']
        return comp_df_drop_na

    @render.plot
    def scatter():
        df = merge_dfs()
        print(df.head())
        attr = input.attr()
        return sns.scatterplot(
            data=df,
            x=f'{attr}_19',
            y=f'{attr}_22'
        ).set(title="2019 vs 2022 values")

    @render.text
    def corr():
        attr = input.attr()
        df = merge_dfs()
        corr = np.corrcoef(df[f'{attr}_19'], df[f'{attr}_22'])[0][1]
        return f"Correlation: {corr}"

    @render.plot
    def diff_histo():
        plot = sns.histplot(merge_dfs(), x='diff').set(
            title=f"Difference {input.attr()}, 2022 - 2019")
        return plot

app = App(app_ui, server)
