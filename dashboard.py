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
query = "SELECT weather_fait.TAVG,weather_fait.Date, weather_fait.PRCP, weather_fait.SNWD, station_dim.country, date_dim.Year, date_dim.Season, date_dim.Month_Name,date_dim.Month_Number" \
        " FROM weather_fait, station_dim, date_dim WHERE (weather_fait.station_id=station_dim.station_id) AND " \
        "(weather_fait.Date=date_dim.Date) AND (PRCP IS NOT NULL) "

df = pd.read_sql(query, connection)

#df = df.groupby(['country', 'Year', 'Season', 'Month_Name', 'Month_Number'])[['TAVG']].mean()
#df.reset_index(inplace=True)
df = df.sort_values('Date')
#print(df[:15])


app.layout = html.Div([
    html.Div(children=[
        html.H1(id='H1', children='Weather dashboard', style={'textAlign': 'center', \
                                                              'marginTop': 40, 'marginBottom': 40, 'color': 'blue'}),
        html.Br(),
    ], style={'padding': 10}),

    html.Div(children=[
    html.Div(children=[
        html.H2(id='H2', children='', style={'marginTop': 20, 'marginleft': 40}),
        html.Br(),
    ], style={'padding': 10, 'flex': 1}),

    html.Div(children=[
    html.Label('Choose a Year'),
    html.Br(),
    dcc.Input(
        id="slct_year", type="number", placeholder="1920",
        min=1920, max=2022, style={'border': '2px solid white'}),
    ], style={'textAlign': 'center', 'padding': 10, 'flex': 1}),

    html.Div(children=[
        html.Label('Chose a Country'),
        dcc.RadioItems(['Algeria', 'Morocco', 'Tunisia'], 'Algeria',
                       id='countries_radio',
                       ),

        html.Br(),


    ], style={'padding': 10, 'flex': 1,'textAlign': 'center'}),
],style={'display': 'flex'} ),
    html.Div(children=[
        dcc.Graph(id='line_plot'),

    ], style={'marginTop': 10, 'text-align':'center','left': 0}),


    ###########PRCP
    html.Div(children=[
    html.Div(children=[
        dcc.Graph(id='prcp'),
    ], style={'padding': 10, 'flex': 1}),

    html.Div(children=[
    dcc.Graph(id='snwd'),
    ], style={'padding': 10, 'flex': 1}),

],style={'display': 'flex'} ),


], style={'backgroundColor':"#321F28", 'background-size': 'cover'})
@app.callback(
    Output(component_id='line_plot', component_property='figure'),
    Output(component_id='prcp', component_property='figure'),
    Output(component_id='snwd', component_property='figure'),
    Input("slct_year", "value"),
    Input('countries_radio', 'value')
)
def graphh(slct_year,countries_radio):
    dff = df.copy()
    print(slct_year)

    if not slct_year == ['1945']:
        if not isinstance(slct_year, list):
            slct_year = [slct_year]
        dff = dff[dff["Year"].isin(slct_year)]

    filtered_df = dff[dff.country == countries_radio]
    print(filtered_df[:15])

    df_p = filtered_df.groupby(['Date', 'Season'])[['PRCP']].mean().reset_index()
    df_p = df_p.sort_values('Date')
    df_p1 = df_p[df_p.Season == 'Winter']
    df_p2 = df_p[df_p.Season == 'Summer']
    df_p3 = df_p[df_p.Season == 'Spring']
    df_p4 = df_p[df_p.Season == 'Fall']

    df_s = filtered_df.groupby(['Month_Name'])[['SNWD']].mean().reset_index()

    fig = go.Figure()

    fig.add_trace(
        go.Scatter(x=filtered_df['Date'], y=filtered_df['TAVG'], name="Air Temperature", line_color='deepskyblue',opacity=0.7))

    fig.update_layout(template='plotly_dark', title_text='Mean temperature', xaxis_rangeslider_visible=True,
                      yaxis_title='TAVG'
                      )

    fig2 = go.Figure()
    fig2.add_trace(
        go.Scatter(x=df_p1['Date'], y=df_p1['PRCP'], name="Winter", line_color='deepskyblue',
                   opacity=0.7))
    fig2.add_trace(
        go.Scatter(x=df_p3['Date'], y=df_p1['PRCP'], name="Spring", line_color='red',
                   opacity=0.7))
    fig2.add_trace(
        go.Scatter(x=df_p2['Date'], y=df_p2['PRCP'], name="Summer", line_color='yellow',
                   opacity=0.7))
    fig2.add_trace(
        go.Scatter(x=df_p4['Date'], y=df_p1['PRCP'], name="Automn", line_color='green',
                   opacity=0.7))

    fig2.update_layout(template='plotly_dark', title_text='Rain precipitation',
                      yaxis_title='PRCP')

    fig3 = px.bar(df_s, x="Month_Name", y="SNWD")
    fig3.update_layout(template='plotly_dark', title_text='Snow depth',
                       yaxis_title='SNWD')
    return fig,fig2,fig3



if __name__ == "__main__":
    app.run_server(debug=True)



# Close the database connection
cursor.close()
connection.commit()
connection.close()