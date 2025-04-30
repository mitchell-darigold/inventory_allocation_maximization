#there are many steps that need to happen to evolve this further.
#I need to add a section that pulls the allowed_specs list automatically with a SQL query connection
#when there are multiple products how do I tell the computer which list to use against which row

import pandas as pd
import sqlite3
import datetime
from datetime import date, timedelta

##Choose the date to start the period table from.  This MUST be the date the data was pulled from the system.
start_date = datetime.date(2025, 4, 8)

#sqlite3 connection path
sqlite3_db = 'inventory_allocation_maximization.db'
sqlite3_conn_path = 'P:/sqlite3/sqlite-tools/'+sqlite3_db

#f strings
mvp_periods_table = 'mvp_periods'

#ended up not pulling the date from the orders table.  Ill just assign one variable at the beginning of the python code for the start date.  Ill have the user choose it in a dialogue box what date to start all this stuff from.
    #this query and the create sqlite tables.sql query both need the date selection
#sql queries:
#mvp_periods_query = 'select ''1'' as PERIOD_NUMBER, min(date(ship_date_formatted)) as DATE_FORMATTED from mvp_orders'

def drop_table(table_name):
    table = table_name
    query = f'drop table if exists {table}'
    return query

def create_mvp_periods(table_name):
    table = table_name
    query = f'''create table {table}(
    PERIOD_NUMBER INTEGER,
    DATE_FORMATTED TEXT
    );'''
    return query

try:
    sqlite3_connection = sqlite3.connect(sqlite3_conn_path)
    cursor = sqlite3_connection.cursor()
    print('Successfully connected to the database')

    #didnt end up using this
    #df = pd.read_csv('S:\Supply_Chain\Analytics\Inventory Allocation Maximization\MVP Data\inventory.csv')
    #df = pd.read_sql_query(mvp_periods_query, sqlite3_connection)

    ##i need to add something that will add the 120 rows of days starting with a specific value pulled in my sql query
    
    num_rows = 121
    start_date = start_date - timedelta(days=1)
    date_list = [start_date + datetime.timedelta(days=i) for i in range(num_rows)]
    number_list = list(range(0, num_rows))

    df = pd.DataFrame({'DATE_FORMATTED': date_list, 'PERIOD_NUMBER': number_list})

    try:
        cursor.execute(drop_table(mvp_periods_table))
        cursor.execute(create_mvp_periods(mvp_periods_table))
        df.to_sql(f'{mvp_periods_table}', sqlite3_connection, if_exists='append', index=False)
    except Exception as ex:
        print(ex)

except sqlite3.Error as error:
	print(error)

finally:
	if sqlite3_connection:
		sqlite3_connection.close()
		print("Connection to power_bi_data is closed")

#i need to export the sqlite table to a csv