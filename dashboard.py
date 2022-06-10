import numpy as np
import pymysql
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import date
from dash import Dash, dcc, html, Input, Output


app = Dash(__name__)



# Connect to the database
connection = pymysql.connect(host='127.0.0.1',
                             user='root',
                             password='root',
                             database='Weather_DataWarehouse',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)
cursor = connection.cursor()

#import data from datawarehouse
query = "SELECT weather_fait.TAVG, station_dim.country, date_dim.Year, date_dim.Season, date_dim.Month_Name,date_dim.Month_Number" \
        " FROM weather_fait, station_dim, date_dim WHERE (weather_fait.station_id=station_dim.station_id) AND " \
        "(weather_fait.Date=date_dim.Date) AND (TAVG IS NOT NULL) AND (weather_fait.station_id='AG000060390')"

df = pd.read_sql(query, connection)

df = df.groupby(['country', 'Year', 'Season', 'Month_Name', 'Month_Number'])[['TAVG']].mean()
df.reset_index(inplace=True)
df = df.sort_values('Month_Number')
print(df[:15])


app.layout = html.Div([
    html.Div(children=[
    html.Div(children=[
        html.H1(id='H1', children='Weather dashboard', style={'textAlign': 'center', \
                                                              'marginTop': 40, 'marginBottom': 40}),

        html.Br(),

    ], style={'padding': 10, 'flex': 1}),


    html.Div(children=[
        html.Label('Choose a Year'),
        html.Br(),
        dcc.Input(
            id="slct_year", type="number", placeholder="1920",
            min=1920, max=2022,
        ),
        html.Br(),
        html.Br(),
        html.Label('Select a Season'),
        dcc.Dropdown( id = 'dropdown',
                options = [
                {'label':'Winter', 'value':'Winter' },
                {'label': 'Summer', 'value':'Summer'},
                {'label': 'Spring', 'value':'Spring'},
                {'label': 'Fall', 'value':'Automn'},
                ],
        value = 'Winter'
        ),
        html.Br(),
        html.Label('Chose a Country'),
        dcc.RadioItems(['New York City', 'Montréal', 'San Francisco'], 'Montréal'),
    ], style={'padding': 10, 'flex': 1}),

    html.Div(children=[
        html.Label('Chose a Country'),
        dcc.RadioItems(['Algeria', 'Morocco', 'Tunisia'], 'Algeria'),

        html.Br(),
        html.Label('Text Input'),
        dcc.Input(value='MTL', type='text'),

        html.Br(),
        html.Label('Slider'),
        dcc.Slider(
            min=0,
            max=9,
            marks={i: f'Label {i}' if i == 1 else str(i) for i in range(1, 6)},
            value=5,
        ),
    ], style={'padding': 10, 'flex': 1}),
],style={'display': 'flex'} ),
    html.Div(children=[
        dcc.Graph(id='line_plot'),

    ], style={'marginTop': 10, 'text-align':'center','left': 0})

], style={})
@app.callback(
    Output(component_id='line_plot', component_property='figure'),
    Input("slct_year", "value"),
    Input(component_id='dropdown', component_property= 'value')
)
def stock_prices(slct_year,dropdown_value):
    dff = df.copy()
    print(slct_year)
    print(dropdown_value)
    if not slct_year == ['1945']:
        if not isinstance(slct_year, list):
            slct_year = [slct_year]
        dff = dff[dff ["Year"].isin(slct_year)]
    print(dff[:15])
    fig = go.Figure([go.Scatter(x=dff['Month_Number'], y=dff['TAVG'], line=dict(color='firebrick', width=4), name='Google')
                     ])
    fig.update_layout(title='Tempirature',
                      xaxis_title='TAVG',
                      yaxis_title='Month_Number'
                      )

    return fig



if __name__ == "__main__":
    app.run_server(debug=True)



# Close the database connection
cursor.close()
connection.commit()
connection.close()