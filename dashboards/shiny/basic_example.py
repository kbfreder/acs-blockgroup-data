from shiny import render, ui
# from shiny.express import input #ui


ui.panel_title("ACS Blockgroup data - 2022")

# with ui.sidebar():
ui.page_sidebar(
    ui.sidebar(
        ui.input_slider("n", "N", 0, 100, 20)
    ),
    ui.card(
        ui.output_text_verbatim("txt")
    )
)


# @render.text
# def result():
#     return f"n*2 is {input.n() * 2}"

def server(input, output, session):
    @output
    @render.text
    def txt():
        return f"n * 2 is {input.n() * 2}"
