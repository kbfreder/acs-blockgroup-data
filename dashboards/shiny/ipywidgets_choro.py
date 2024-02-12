# NOT WORKING: !! Can't get Choropleth to render !!

import json
import pickle
import pandas as pd

from shiny import App, ui, reactive, render
from shinywidgets import register_widget, output_widget, reactive_read
import ipyleaflet as ipyl
from branca.colormap import linear

# ==========================
# DATA LOADING / PREP
# ==========================

# load some geo JSON data
with open("data/tigerweb/us_bg_shapefiles_2020/wi/wi.json", "r") as file:
    geo_json_data = json.load(file)

# reformat geo data so 'id' is a key of `features`
geo_data_mod = {
    'type': geo_json_data['type'],
    'crs': geo_json_data['crs']
}
geo_data_mod['features'] = [
    {
        'type': 'Feature',
        'id': f['properties']['GEOID'],
        # must include a properties dict, otherwise throws an error
        'properties': {'STATEFP': '55'},
        'geometry': f['geometry']
    }
    for f in geo_json_data['features']
]

YEAR = 2022
DATASET_YRS = 5
ACS_BG_FILENAME = f"ACS_BG_DATA_{YEAR}"

def load_checkpoint_df(checkpoint_name):
    file_stub = f"data/parsed_acs_data/{checkpoint_name}"
    with open(f"{file_stub}.pkl", "rb") as f:
        dtype_dict = pickle.load(f)
    summary_df = pd.read_csv(f"{file_stub}.csv", dtype=dtype_dict)
    return summary_df


acs_df = load_checkpoint_df(ACS_BG_FILENAME)
# filter on Wisconsin, for now
acs_df = acs_df[acs_df['STATE'] == '55']

var = 'pop_density_sqmile'
choro_data = dict(zip(acs_df['bg_fips'].to_list(), acs_df[var].to_list()))


# ==========================
# APP/DASHBOARD CODE
# ==========================

app_ui = ui.page_fluid(
    ui.row(
        # inputs
        # ui.input_select(
        #     id="center",
        #     label="Center",
        #     choices=list(center_dict.keys())
        # ),
        # ui.input_slider(id="zoom", label="Zoom", min=1, max=10, value=6),
        ui.output_text_verbatim("center")
    ),
    output_widget("map")
)

def server(input, output, session):
    map = ipyl.Map(center=(43.7844, 269.49), zoom=7)
    map.add(geo_json_data)
    choro_layer = ipyl.Choropleth(geo_data=geo_data_mod, choro_data=choro_data, 
                                  colormap=linear.Blues_07, border_color='black', )
    map.add(choro_layer)
    register_widget(id="map", widget=map)


    # @reactive.Effect()
    # def _():
    #     center = input.center()
    #     map.center = center_dict[center]
    #     map.zoom = input.zoom()
    
    @output
    @render.text
    def center():
        center = reactive_read(map, "center")
        return f"Current center: {center}"


app = App(app_ui, server)
