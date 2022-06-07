import numpy as np
import pymysql
import pandas as pd
from datetime import datetime


# Create a table following the given schema, if table exists it will be deleted
def create_table(co_cursor, table_name, table_schema):

    co_cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
    sql = "DROP TABLE IF EXISTS " + table_name
    co_cursor.execute(sql)
    sql = "CREATE TABLE " + table_name + "(" + table_schema + ")"
    co_cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
    co_cursor.execute(sql)



# Get the indices of a certain attribute type
def get_attribute_type_index(schema, attribute_type):
    row_splt = ","
    ele_splt = " "

    # Create a schema matrix, where the first column stores the attribute's name and the second its type
    temp = schema.split(row_splt)
    # Using list comprehension as shorthand
    schema_matrix = [ele.split(ele_splt) for ele in temp]

    # Get attributes' types list from schema_matrix
    attributes_types = np.array(schema_matrix)[:, 1]

    # Get all the indices of attributes of type 'attribute_type'
    indices = [i for i, e in enumerate(attributes_types) if e.lower() == attribute_type.lower()]
    return indices


# Insert values into a table from a csv file
def populate_table(co_cursor, csv_path, table_name, attributes):
    data = pd.read_csv(csv_path, sep=",", encoding='cp1252')
    data_indices = get_attribute_type_index(attributes, "DATE")
    for index, row in data.iterrows():

        attribute_number = (row.size * '%s,')[:-1]
        sql = "INSERT INTO " + table_name + "  VALUES (" + attribute_number + ")"
        # convert data attributes to mysql format
        if data_indices:
            for date_index in data_indices:
                row[date_index] = datetime.strptime(row[date_index], '%Y-%m-%d').date()

        co_cursor.execute(sql, tuple(row))

def populate_dim_station(co_cursor, csv_path, table_name, country):
    dataf = pd.read_csv(csv_path, sep=",", encoding='cp1252',  low_memory=False)
    data = dataf.iloc[:,:5]
    data = data.drop_duplicates()
    country = country

    for index, row in data.iterrows():
        sql = "INSERT IGNORE INTO " + table_name + " (station_id,station_name,country,LATITUDE,LONGITUDE,ELEVATION)" \
              "VALUES(%s, %s, %s, %s, %s, %s) "

        co_cursor.execute(sql, (row[0], row[1], country, row[2], row[3], row[4] ))



def get_date_id(date_str):
    date_id = date_str.strftime("%Y") + date_str.strftime("%m") + date_str.strftime("%d")
    return date_id

def populate_dim_date(co_cursor, csvpath):
    data = pd.read_csv(csvpath, low_memory=False)
    for index, line in data.iterrows():
        sql = "INSERT INTO Date_dim (Date_ID,Date,Day_Name,Day_Name_Abbrev,Day_Of_Month,Day_Of_Week,Day_Of_Year," \
              "Holiday_Name,Is_Holiday,Is_Weekday,Is_Weekend,Month_Abbrev,Month_End_Flag,Month_Name,Month_Number,Quarter," \
              "Quarter_Name,Quarter_Short_Name,Same_Day_Previous_Year,Same_Day_Previous_Year_ID,Season,Week_Begin_Date," \
              "Week_Begin_Date_ID,Week_Num_In_Month,Week_Num_In_Year,Year,Year_And_Month,Year_And_Month_Abbrev," \
              "Year_And_Quarter)" \
              "VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, " \
              "%s, %s, %s, %s, %s) "
        date_id = line.Date_ID
        if pd.isna(line.Date_ID):
            date_id = None
        holiday_name = line.Holiday_Name
        if pd.isna(line.Holiday_Name):
            holiday_name = None
        date = line.Date
        if pd.isna(line.Date):
            date = None
        day_name = line.Day_Name
        if pd.isna(line.Day_Name):
            day_name = None
        day_name_abbrev = line.Day_Name_Abbrev
        if pd.isna(line.Day_Name_Abbrev):
            day_name_abbrev = None
        day_of_month = line.Day_Of_Month
        if pd.isna(line.Day_Of_Month):
            day_of_month = None
        day_of_week = line.Day_Of_Week
        if pd.isna(line.Day_Of_Week):
            day_of_week = None
        day_of_year = line.Day_Of_Year
        if pd.isna(line.Day_Of_Year):
            day_of_year = None
        is_holiday = line.Is_Holiday
        if pd.isna(line.Is_Holiday):
            is_holiday = None
        is_weekday = line.Is_Weekday
        if pd.isna(line.Is_Weekday):
            is_weekday = None
        is_weekend = line.Is_Weekend
        if pd.isna(line.Is_Weekend):
            is_weekend = None
        month_abbrev = line.Month_Abbrev
        if pd.isna(line.Month_Abbrev):
            month_abbrev = None
        month_end_flag = line.Month_End_Flag
        if pd.isna(line.Month_End_Flag):
            month_end_flag = None
        month_name = line.Month_Name
        if pd.isna(line.Month_Name):
            month_name = None
        month_number = line.Month_Number
        if pd.isna(line.Month_Number):
            month_number = None
        quarter = line.Quarter
        if pd.isna(line.Quarter):
            quarter = None
        quarter_name = line.Quarter_Name
        if pd.isna(line.Quarter_Name):
            quarter_name = None
        quarter_short_name = line.Quarter_Short_Name
        if pd.isna(line.Quarter_Short_Name):
            quarter_short_name = None
        same_day_previous_year = line.Same_Day_Previous_Year
        if pd.isna(line.Same_Day_Previous_Year):
            same_day_previous_year = None
        same_day_previous_year_id = line.Same_Day_Previous_Year_ID
        if pd.isna(line.Same_Day_Previous_Year_ID):
            same_day_previous_year_id = None
        season = line.Season
        if pd.isna(line.Season):
            season = None
        week_begin_date = line.Week_Begin_Date
        if pd.isna(line.Week_Begin_Date):
            week_begin_date = None
        week_begin_date_id = line.Week_Begin_Date_ID
        if pd.isna(line.Week_Begin_Date_ID):
            week_begin_date_id = None
        week_num_in_month = line.Week_Num_In_Month
        if pd.isna(line.Week_Num_In_Month):
            week_num_in_month = None
        week_num_in_year = line.Week_Num_In_Year
        if pd.isna(line.Week_Num_In_Year):
            week_num_in_year = None
        year = line.Year
        if pd.isna(line.Year):
            year = None
        year_and_month = line.Year_And_Month
        if pd.isna(line.Year_And_Month):
            year_and_month = None
        year_and_month_abbrev = line.Year_And_Month_Abbrev
        if pd.isna(line.Year_And_Month_Abbrev):
            year_and_month_abbrev = None
        year_and_quarter = line.Year_And_Quarter
        if pd.isna(line.Year_And_Quarter):
            year_and_quarter = None


        co_cursor.execute(sql, (
            date_id, date, day_name, day_name_abbrev, day_of_month, day_of_week, day_of_year, holiday_name,
            is_holiday, is_weekday, is_weekend, month_abbrev, month_end_flag, month_name, month_number, quarter,
            quarter_name, quarter_short_name, same_day_previous_year, same_day_previous_year_id, season,
            week_begin_date, week_begin_date_id, week_num_in_month, week_num_in_year, year,
            year_and_month, year_and_month_abbrev, year_and_quarter ))





# Connect to the database
connection = pymysql.connect(host='127.0.0.1',
                             user='root',
                             password='root',
                             database='Weather_DataWarehouse',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)
cursor = connection.cursor()

# Create tables
Date_dim = '''
   Date_ID VARCHAR(15) primary key,
   Date DATE,
   Day_Name VARCHAR(10), 
   Day_Name_Abbrev VARCHAR(3),
   Day_Of_Month INT(2), 
   Day_Of_Week INT(1), 
   Day_Of_Year INT(3), 
   Holiday_Name VARCHAR(35), 
   Is_Holiday VARCHAR(5),
   Is_Weekday VARCHAR(5), 
   Is_Weekend VARCHAR(5), 
   Month_Abbrev VARCHAR(3), 
   Month_End_Flag VARCHAR(5), 
   Month_Name VARCHAR(15), 
   Month_Number INT(2), 
   Quarter INT(1), 
   Quarter_Name VARCHAR(6),
   Quarter_Short_Name VARCHAR(2), 
   Same_Day_Previous_Year DATE, 
   Same_Day_Previous_Year_ID INT(8),
   Season VARCHAR(10), 
   Week_Begin_Date DATE, 
   Week_Begin_Date_ID INT(8), 
   Week_Num_In_Month INT(1),
   Week_Num_In_Year INT(2), 
   Year INT(4), 
   Year_And_Month VARCHAR(7), 
   Year_And_Month_Abbrev VARCHAR(8),
   Year_And_Quarter VARCHAR(7)
'''
station_dim = '''
  station_id  VARCHAR(20) primary key,
  station_name VARCHAR(30) , 
  country  VARCHAR(20) ,
  LATITUDE FLOAT,
  LONGITUDE FLOAT,
  ELEVATION FLOAT
'''
weather_fait = '''

Date_ID VARCHAR(8) NOT NULL REFERENCES Date_dim(Date_ID) ,
station_id  VARCHAR(20) NOT NULL REFERENCES station_dim(station_id),

ACSH INT(20),
ACSH_ATTRIBUTES VARCHAR(20),

PRCP FLOAT,
PRCP_ATTRIBUTES VARCHAR (20),
PGTM INT(20),
PGTM_ATTRIBUTES VARCHAR(20),

SNWD FLOAT,
SNWD_ATTRIBUTES VARCHAR(20),
SNOW FLOAT,
SNOW_ATTRIBUTES VARCHAR(20),

TAVG FLOAT,
TAVG_ATTRIBUTES VARCHAR(20),
TMAX FLOAT,
TMAX_ATTRIBUTES VARCHAR(20),
TMIN INT,
TMIN_ATTRIBUTES VARCHAR(20),

WDFG FLOAT,
WDFG_ATTRIBUTES VARCHAR(20),
WSFG  FLOAT,
WSFG_ATTRIBUTES VARCHAR(20),
WDFM FLOAT,
WDFM_ATTRIBUTES VARCHAR(20),
WSFM FLOAT,
WSFM_ATTRIBUTES VARCHAR(20),

WT05 VARCHAR(20),
WT05_ATTRIBUTES VARCHAR(20),
WT07 VARCHAR(20),
WT07_ATTRIBUTES VARCHAR(20),
WT08 VARCHAR(20),
WT08_ATTRIBUTES VARCHAR(20),
WT09 VARCHAR(20),
WT09_ATTRIBUTES VARCHAR(20),
WT16 VARCHAR(20),
WT16_ATTRIBUTES VARCHAR(20),
WT18 VARCHAR(20),
WT18_ATTRIBUTES VARCHAR(20),
WT01 VARCHAR(20),
WT01_ATTRIBUTES VARCHAR(20),

primary key (Date_ID , station_id),
FOREIGN KEY (Date_ID) REFERENCES date_dim(Date_ID),
FOREIGN KEY (station_id) REFERENCES station_dim(station_id)
'''

create_table(cursor, "Date_dim", Date_dim)
print("Table Date created")
create_table(cursor, "station_dim", station_dim)
print("Table Station created")
create_table(cursor, "weather_fait", weather_fait)
print("Table Weather created")

# Populate tables
date_path = "C:/Users/Hamida/Desktop/projet_Entrepot/Dim_Date_1850-2050.csv"

#ALGERIA
station_path = "C:/Users/Hamida/Desktop/projet_Entrepot/Weather Data/Algeria/Weather_1920-1929_ALGERIA.csv"
station2_path = "C:/Users/Hamida/Desktop/projet_Entrepot/Weather Data/Algeria/Weather_1930-1939_ALGERIA.csv"
station3_path = "C:/Users/Hamida/Desktop/projet_Entrepot/Weather Data/Algeria/Weather_1940-1949_ALGERIA.csv"
station4_path = "C:/Users/Hamida/Desktop/projet_Entrepot/Weather Data/Algeria/Weather_1950-1959_ALGERIA.csv"
station5_path = "C:/Users/Hamida/Desktop/projet_Entrepot/Weather Data/Algeria/Weather_1960-1969_ALGERIA.csv"
station6_path = "C:/Users/Hamida/Desktop/projet_Entrepot/Weather Data/Algeria/Weather_1970-1979_ALGERIA.csv"
station7_path = "C:/Users/Hamida/Desktop/projet_Entrepot/Weather Data/Algeria/Weather_1980-1989_ALGERIA.csv"
station8_path = "C:/Users/Hamida/Desktop/projet_Entrepot/Weather Data/Algeria/Weather_1990-1999_ALGERIA.csv"
station9_path = "C:/Users/Hamida/Desktop/projet_Entrepot/Weather Data/Algeria/Weather_2000-2009_ALGERIA.csv"
station10_path = "C:/Users/Hamida/Desktop/projet_Entrepot/Weather Data/Algeria/Weather_2010-2019_ALGERIA.csv"
station11_path = "C:/Users/Hamida/Desktop/projet_Entrepot/Weather Data/Algeria/Weather_2020-2022_ALGERIA.csv"
#MOROCCO
station12_path = "C:/Users/Hamida/Desktop/projet_Entrepot/Weather Data/Morocco/Weather_1920-1959_MOROCCO.csv"
station13_path = "C:/Users/Hamida/Desktop/projet_Entrepot/Weather Data/Morocco/Weather_1960-1989_MOROCCO.csv"
station14_path = "C:/Users/Hamida/Desktop/projet_Entrepot/Weather Data/Morocco/Weather_1990-2019_MOROCCO.csv"
station15_path = "C:/Users/Hamida/Desktop/projet_Entrepot/Weather Data/Morocco/Weather_2020-2022_MOROCCO.csv"
#TUNISIA
station16_path = "C:/Users/Hamida/Desktop/projet_Entrepot/Weather Data/Tunisia/Weather_1920-1959_TUNISIA.csv"
station17_path = "C:/Users/Hamida/Desktop/projet_Entrepot/Weather Data/Tunisia/Weather_1960-1989_TUNISIA.csv"
station18_path = "C:/Users/Hamida/Desktop/projet_Entrepot/Weather Data/Tunisia/Weather_1990-2019_TUNISIA.csv"
station19_path = "C:/Users/Hamida/Desktop/projet_Entrepot/Weather Data/Tunisia/Weather_2020-2022_TUNISIA.csv"


populate_dim_date(cursor, date_path)
print("Table Date populated")

populate_dim_station(cursor, station_path, 'station_dim', 'Algeria')
populate_dim_station(cursor, station2_path, 'station_dim', 'Algeria')
populate_dim_station(cursor, station3_path, 'station_dim', 'Algeria')
populate_dim_station(cursor, station4_path, 'station_dim', 'Algeria')
populate_dim_station(cursor, station5_path, 'station_dim', 'Algeria')
populate_dim_station(cursor, station6_path, 'station_dim', 'Algeria')
populate_dim_station(cursor, station7_path, 'station_dim', 'Algeria')
populate_dim_station(cursor, station8_path, 'station_dim', 'Algeria')
populate_dim_station(cursor, station9_path, 'station_dim', 'Algeria')
populate_dim_station(cursor, station10_path, 'station_dim', 'Algeria')
populate_dim_station(cursor, station11_path, 'station_dim', 'Algeria')

populate_dim_station(cursor, station12_path, 'station_dim', 'Morocco')
populate_dim_station(cursor, station13_path, 'station_dim', 'Morocco')
populate_dim_station(cursor, station14_path, 'station_dim', 'Morocco')
populate_dim_station(cursor, station15_path, 'station_dim', 'Morocco')

populate_dim_station(cursor, station16_path, 'station_dim', 'Tunisia')
populate_dim_station(cursor, station17_path, 'station_dim', 'Tunisia')
populate_dim_station(cursor, station18_path, 'station_dim', 'Tunisia')
populate_dim_station(cursor, station19_path, 'station_dim', 'Tunisia')







# Close the database connection
cursor.close()
connection.commit()
connection.close()









