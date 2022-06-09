import numpy as np
import pymysql
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
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
query = "SELECT TAVG, station_dim.country, date_dim.Year, date_dim.Season, date_dim.Month_Name FROM weather_fait, " \
        "station_dim, date_dim WHERE (weather_fait.station_id=station_dim.station_id) AND (weather_fait.Date=date_dim.Date)"

df = pd.read_sql(query, connection)

df = df.groupby(['country', 'Year', 'Season', 'Month_Name'])[['TAVG']].mean()
df.reset_index(inplace=True)
print(df[:15])





# Close the database connection
cursor.close()
connection.commit()
connection.close()