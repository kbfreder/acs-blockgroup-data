
import json

import pandas as pd
import numpy as np

import plotly.express as px
from dash import Dash, html, Input, Output, dcc, callback
import dash_bootstrap_components as dbc
import plotly.graph_objects as go
import plotly.express as px


import sys
sys.path.append("./src/")
from src.process_bg_tables.util import load_csv_with_dtypes
from src.configs import ACS_BG_FILENAME


df = load_csv_with_dtypes(ACS_BG_FILENAME, ".") 
print(df.head())
# filter to match geo data:
df = df[df['state'] == 'WI']
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
with open("data/tigerweb/us_bg_shapefiles_2020/wi/wi.json", "r") as file:
    geo_json = json.load(file)

app = Dash(__name__)

app.layout = html.Div([
    html.Div([
        html.H1('ACS 2022 Data'),
    ],style={'textAlign':'center'}),    
    html.Div([
        html.Div([
            html.H3('Attribute', className = 'fix_label', style={'color':'black', 'margin-top': '2px'}),
            dcc.Dropdown(attrs, 'population', id='dropdown-selection'),
        ], className = 'create_container2 five columns', 
        style = {'width': '20%', 'margin-bottom': '20px'}),
    ], className = 'row flex-display'),
    
    html.Div([
        html.Div([
            dcc.Graph(id='hist-plot')
        ], style={'width': '50%', 'display': 'inline-block'}),
        html.Div([
            dcc.Graph(id = 'geo-plot', figure = {}),
            ], style={'width': '50%',
                      'display': 'inline-block',
                      'vertical-align': 'top'
                  }
            )
    ], className="row"),

], id='mainContainer', style={'display':'flex', 'flex-direction':'column'})


@callback(
    Output('hist-plot', 'figure'),
    Input('dropdown-selection', 'value')
)
def update_plot(attr):
    return px.histogram(df, attr)



@callback(
    Output('geo-plot', 'figure'),
    Input('dropdown-selection', 'value')
)
def display_choropleth(attr):
    fig = px.choropleth(
        df, 
        geojson=geo_json,
        color=attr,
        locations='bg_fips',
        featureidkey='properties.GEOID',
        color_continuous_scale='Viridis',
        range_color=[0, max(df[attr])],
        projection="mercator"
    )
    fig.update_geos(fitbounds="locations", visible=False)
    fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
    return fig


if __name__ == "__main__":
    app.run_server(debug=True)