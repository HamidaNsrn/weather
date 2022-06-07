# Activity 1

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
    # TODO: Récupérer les valeurs des attributs présentes dans "data" et construire une requête "INSERT" pour les insérer dans la table
    df = pd.DataFrame(data)

    #changing date format
    indice = get_attribute_type_index(attributes, "DATE") # [5 6]
    for j in range(0, len(indice)):     # j=0 or j=1
        for row in df.itertuples():
            old_date= row[indice[j]+1]   #row[6] old_date=5/28/2010
            new_date = datetime.strptime(old_date, "%m/%d/%Y")
            new_date = new_date.date()
            new = new_date.isoformat()
            df.replace(to_replace=row[indice[j]+1], value=new, inplace=True)

    row_splt = ","
    ele_splt = " "

    temp = attributes.split(row_splt)
    # Using list comprehension as shorthand
    schema_matrix = [ele.split(ele_splt) for ele in temp]

    # Get attributes' list from schema_matrix
    attributes_types = np.array(schema_matrix)[:, 0]

    # Insert DataFrame records one by one.
    for row in df.itertuples():
        sql2 = "INSERT INTO " + table_name + " (" + attributes_types[0] + ") VALUES ('" + str(row[1]) + "')"

        cursor.execute(sql2)
        connection.commit()
        for i in range(1, len(attributes_types)):

            sql = "UPDATE " + table_name +\
                  " SET " + attributes_types[i] + "='" + str(row[i+1])+ "'" \
                                                                     " WHERE "+ attributes_types[0] + "=" + str(row[1])


            co_cursor.execute(sql)
            connection.commit()


#afficher les tableaux
def fetch_table(co_cursor, table_name):
    sql3 = "SELECT * FROM " + table_name
    co_cursor.execute(sql3)
    myresult = co_cursor.fetchall()
    return myresult

def fetch_order_col(co_cursor, table_name):
    sql3 = "SELECT order_ID, item_type, sales_channel, order_priority FROM " + table_name
    co_cursor.execute(sql3)
    myresult = co_cursor.fetchall()
    return myresult
def fetch_date_col(co_cursor, table_name):
    sql3 = "SELECT order_date, ship_date FROM " + table_name
    co_cursor.execute(sql3)
    myresult = co_cursor.fetchall()
    return myresult
def fetch_sales_col(co_cursor, table_name):
    sql3 = "SELECT units_sold, unit_price, unit_cost, total_revenue, total_cost FROM " + table_name
    co_cursor.execute(sql3)
    myresult = co_cursor.fetchall()
    return myresult




# Connect to the database
connection = pymysql.connect(host='127.0.0.1',
                             user='root',
                             password='root',
                             database='TransactionalDB',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)
cursor = connection.cursor()

# Create tables
cus_schema = '''
   customers_ID INT(20) primary key,
   customer_name TEXT(30),
   country TEXT(20),
   country_Iso VARCHAR(20),
   region TEXT(20)
'''
sales_schema = '''
   order_ID int(30) NOT NULL primary key,
   customers_ID INT(20) ,
   item_type VARCHAR(30),
   sales_channel VARCHAR(20),
   order_priority VARCHAR(5),
   order_date DATE,
   ship_date DATE,
   units_sold INT(11),
   unit_price FLOAT,
   unit_cost FLOAT,
   total_revenue FLOAT,
   total_cost FLOAT
'''
#create_table(cursor, "Customers", cus_schema) # TODO: déinir le bon schema de Customers
#print("Table Customers created")
#create_table(cursor, "Sales", sales_schema) # TODO: déinir le bon schema de Sales
#print("Table Sales created")

# Populate tables
cos_path = "C:/Users/Hamida/Desktop/activity1/Customers.csv"
sale_path = "C:/Users/Hamida/Desktop/activity1/Sales.csv"
#populate_table(cursor, cos_path, 'Customers',
 #              'customers_ID INT,customer_name TEXT,country TEXT,country_Iso VARCHAR,region TEXT') # TODO: renseigner chauqe attribut avec son type
#print("Table Customers populated")
#populate_table(cursor, sale_path, 'Sales', 'order_ID int,customers_ID INT,item_type VARCHAR,'
 #                                                 'sales_channel VARCHAR,order_priority VARCHAR,order_date DATE,'
  #                                                'ship_date DATE,units_sold INT,unit_price FLOAT,unit_cost FLOAT,'
   #                                               'total_revenue FLOAT,total_cost FLOAT') # TODO: renseigner chauqe attribut avec son type
#print("Table Sales populated")


#afficher les tables
#for x in fetch_table(cursor, "Customers"):
    #  print(x)
#for x in fetch_table(cursor, "Sales"):
 #   print(x)




############################ACTIVITY3#################################

def populate_dw(c_cursor, table_name, attributes):

    if table_name =='Customer':
        df = pd.DataFrame(fetch_table(cursor, "Customers"))
    if table_name == "Orderr":
       df = pd.DataFrame(fetch_order_col(cursor, "Sales"))
    if table_name == "Date":
       df = pd.DataFrame(fetch_date_col(cursor, "Sales"))
       a = df.shape[0]
       x = range(1, a+1)
       df.insert(0, "date_ID", x, True)



    row_splt = ","
    ele_splt = " "

    temp = attributes.split(row_splt)
    # Using list comprehension as shorthand
    schema_matrix = [ele.split(ele_splt) for ele in temp]

    # Get attributes' list from schema_matrix
    attribute = np.array(schema_matrix)[:, 0]

    # Insert DataFrame records one by one.
    for row in df.itertuples():
        sql3 = "INSERT INTO " + table_name + " (" + attribute[0] + ") VALUES ('" + str(row[1]) + "')"

        c_cursor.execute(sql3)
        conn.commit()
        for i in range(1, len(attribute)):
            sql4 = "UPDATE " + table_name + \
                    " SET " + attribute[i] + "='" + str(row[i + 1]) + "' WHERE " + attribute[0] + "=" + str(row[1])

            c_cursor.execute(sql4)
            conn.commit()

def create_sales_fact (c_cusor):
    sql = "DROP TABLE IF EXISTS sales"
    c_cusor.execute(sql)
    sql = "CREATE TABLE sales (order_ID int(30), customers_ID INT(20), order_date DATE, ship_date DATE, " \
         " unitSold INT, unitPrice FLOAT, unitCost FLOAT, PRIMARY KEY (order_ID, customers_ID, order_date, ship_date)," \
         " FOREIGN KEY (order_ID) REFERENCES Orderr(order_ID), FOREIGN KEY (order_date) REFERENCES Date(order_date)," \
         " FOREIGN KEY (ship_date) REFERENCES Date(ship_date), FOREIGN KEY (customers_ID) REFERENCES Customer(customers_ID)) "
    c_cusor.execute(sql)




conn = pymysql.connect(host='127.0.0.1',
                             user='root',
                             password='root',
                             database='datawarehouse',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)
cursor2 = conn.cursor()
sales_dw = '''
   sale_id int(30) NOT NULL primary key,
   customers_ID INT(20),
   order_ID int(30),
   date_ID int(30),
   units_sold INT(11),
   unit_price FLOAT,
   unit_cost FLOAT,
   total_revenue FLOAT,
   total_cost FLOAT,
   Total_profit FLOAT,
   FOREIGN KEY (customers_ID) REFERENCES Customer(customers_ID),
   FOREIGN KEY (order_ID) REFERENCES Orderr(order_ID),
   FOREIGN KEY (date_ID) REFERENCES Date(date_ID)
'''
#cus_schema = '''
 #  customers_ID INT(20) primary key,
  # customer_name TEXT(30),
   #country TEXT(20),
   #country_Iso VARCHAR(20),
   #region TEXT(20)
#'''

order_dw = '''
   order_ID int(30) NOT NULL primary key,
   item_type VARCHAR(30),
   sales_channel VARCHAR(20),
   order_priority VARCHAR(5)
'''

date_dw = '''
   date_ID int(30) NOT NULL AUTO_INCREMENT primary key,
   order_date DATE,
   ship_date DATE
'''
#create_table(cursor2, "Orderr", order_dw) # TODO: déinir le bon schema de Customers
print("Table Order created")
#create_table(cursor2, "Customer", cus_schema) # TODO: déinir le bon schema de Sales
print("Table Customer created")
#create_table(cursor2, "Date", date_dw) # TODO: déinir le bon schema de Sales
print("Table Date created")
#create_table(cursor2, "Sales", sales_dw) # TODO: déinir le bon schema de Customers
print("Table Sales created")

#populate_dw(cursor2, 'Customer', 'customers_ID INT,customer_name TEXT,country TEXT,country_Iso VARCHAR,region TEXT') # TODO: renseigner chauqe attribut avec son type
print("Table Customers populated")
#populate_dw(cursor2, 'Orderr', 'order_ID INT,item_type VARCHAR,sales_channel VARCHAR,order_priority VARCHAR') # TODO: renseigner chauqe attribut avec son type
print("Table Order populated")
#populate_dw(cursor2, 'Date', 'date_ID INT,order_date DATE,ship_date DATE') # TODO: renseigner chauqe attribut avec son type
print("Table Date populated")
#populate_dw(cursor2, 'Sales', 'sale_id int, customers_ID INT, order_ID int, date_ID int, units_sold INT, '
                   #           'unit_price FLOAT, unit_cost FLOAT, total_revenue FLOAT, total_cost FLOAT, '
                    #          'Total_profit FLOAT')
print("Table Date populated")
create_sales_fact(cursor2)
print("Table Sales populated")




# Close the database connection
cursor.close()
connection.commit()
connection.close()

# Close the database connection
cursor2.close()
conn.commit()
conn.close()








