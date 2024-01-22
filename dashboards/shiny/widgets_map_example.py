from shiny import App, ui, reactive, render
from shinywidgets import register_widget, output_widget, reactive_read
import ipyleaflet as ipyl

center_dict = {
    'London': (51.5074, 0.1278),
    'Paris': (48.8566, 2.3522),
    'New York': (40.7128, -74.0060)
}

app_ui = ui.page_fluid(
    ui.row(
        ui.input_select(
            id="center",
            label="Center",
            choices=list(center_dict.keys())
        ),
        ui.input_slider(id="zoom", label="Zoom", min=1, max=10, value=6),
        ui.output_text_verbatim("center")
    ),
    output_widget("map")
)

def server(input, output, session):
    map = ipyl.Map(zoom=4, center=center_dict['London'])
    register_widget(id="map", widget=map)

    @reactive.Effect()
    def _():
        center = input.center()
        map.center = center_dict[center]
        map.zoom = input.zoom()
    
    @output
    @render.text
    def center():
        center = reactive_read(map, "center")
        return f"Current center: {center}"


app = App(app_ui, server)
