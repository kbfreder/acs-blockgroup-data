from pathlib import Path

import pandas as pd
import geopandas as gpd
import seaborn as sns

from shiny import App, Inputs, Outputs, Session, reactive, render, ui

import sys
sys.path.append("./src/")
from src.process_bg_tables.util import load_csv_with_dtypes
from src.configs import ACS_BG_FILENAME

sns.set_theme(style="white")
df = load_csv_with_dtypes(ACS_BG_FILENAME, ".") #na_values=??
# geo_df = gpd.read_file("data/tigerweb/us_bg_shapefiles_2020/us/")
## load smaller dataset whilst dev'ing
geo_df = gpd.read_file("data/tigerweb/us_bg_shapefiles_2020/wi/")

attrs = ["population", "pop_density_sqmile", 
         "households", 'pct_hh_internet_subscription',
       'pct_hh_vehicle_ownership', 'avg_vehicles_per_hh', 'pct_hh_foodstamps',
       'pct_home_ownership', 
         "median_age", 'prop_age_under_18', 'prop_age_25_34', 'prop_age_35_44',
       'prop_age_45_54',  'prop_age_55_64', 'prop_age_65_74',
       'prop_female', 'race_pct_white',
       'race_pct_african_american', 'race_pct_nativeam', 'race_pct_asian',
       'race_pct_other', 'race_pct_hispanic', 'median_income',
       'pct_college_enrolled', 'prop_college_educated', 'pct_married',
         'pct_tract_military_employed'
    ]

gdf_enr = geo_df[['GEOID', 'STATEFP', 'geometry']].merge(
    df, left_on='GEOID', right_on='bg_fips', how='inner')

app_ui = ui.page_sidebar(
    ui.sidebar(
        ui.input_radio_buttons(
            id="attrs",
            label="Attribute",
            choices=attrs
        ),
        width=350
    ),
    ui.row(
        ui.layout_columns(
            ui.output_text_verbatim("txt")
        )
    ),
    ui.row(
        ui.layout_columns(
            ui.card(
                ui.card_header("Histogram"),
                ui.output_plot("histo"),
            ),
            ui.card(
                ui.output_data_frame("geo_plot"),
            ),
        ),
    )
)

def server(input: Inputs, output: Outputs, session: Session):
    @reactive.Calc
    def filtered_df() -> pd.Series:
        filt_df = df[input.attrs()]
        return filt_df

    @render.text
    def txt():
        return input.attrs()
    
    @render.plot
    def histo():
        return sns.distplot(
            filtered_df(),
            bins=40
        )

    @render.plot
    def geo_plot():
        # conus = gdf_enr[~gdf_enr['STATEFP'].isin(['02', '15'])]
        # return conus.plot(input.attrs(), legend=True, figsize=(12,8))
        return gdf_enr.plot(input.attrs(), legend=True, figsize=(12,8))

app = App(app_ui, server)
