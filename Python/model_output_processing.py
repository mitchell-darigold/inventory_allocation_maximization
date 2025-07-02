import pandas as pd
import sqlite3
from datetime import datetime, timedelta
from datetime import date
from dateutil.parser import parse
import tkinter
from tkinter import filedialog
import numpy as np
import math
import time


#Ask user for date to use with the loading.  It needs to be the date the data was pulled on

#while True:
#    date_string = input("Enter the date you pulled the data on in YYYY-MM-DD format: ")
#    try:
#        date_object = datetime.strptime(date_string, "%Y-%m-%d").date()
#        break
#    except ValueError:
#       print("Invalid date format. Please use YYYY-MM-DD.")
#print("You entered:", date_object)

#start_date = parse(date_string)

#these two lines are just so I dont have to enter a date everytime I run a test.  They should be commented out once the project is complete
date_string = '2025-05-07'
start_date = parse(date_string)

#Make a Sqlite connection

sqlite3_db = 'inventory_allocation_maximization.db'
sqlite3_conn_path = 'P:/sqlite3/sqlite-tools/'+sqlite3_db

sqlite3_connection = sqlite3.connect(sqlite3_conn_path)
cursor = sqlite3_connection.cursor()
print('Successfully connected to the database')

###############################################################Variable list############################################################

#Paths
#iam_inventory_path = 'S:\\Supply_Chain\\Analytics\\Inventory Allocation Maximization\\Master\\inventory20250605.csv'
iam_inventory_path = 'S:\\Supply_Chain\\Analytics\\Inventory Allocation Maximization\\Master\\inventory.csv'
#iam_inventory_path = 'S:\\Supply_Chain\\Analytics\\Inventory Allocation Maximization\\Comparison\\model\\inventory20250512.csv'
iam_item_path = 'S:\\Supply_Chain\\Analytics\\Inventory Allocation Maximization\\Master\\item_master.csv'
#iam_orders_path = 'S:\\Supply_Chain\\Analytics\\Inventory Allocation Maximization\\Master\\orders20250605.csv'
iam_orders_path = 'S:\\Supply_Chain\\Analytics\\Inventory Allocation Maximization\\Master\\orders.csv'
#iam_orders_path = 'S:\\Supply_Chain\\Analytics\\Inventory Allocation Maximization\\Comparison\\model\\orders20250512.csv'
iam_customer_flows_path = 'S:\\Supply_Chain\\Analytics\\Inventory Allocation Maximization\\Master\\outputs\\Product Allocation Maximization - Customer Flows.xlsx'

##SQL Queries##

inventory_gathering = '''select * from iam_inventory'''
period_gathering = '''select * from iam_periods'''

##Dataframe creation

df = pd.read_excel(iam_customer_flows_path)
inv_df = pd.read_sql_query(inventory_gathering, sqlite3_connection)
periods_df = pd.read_sql_query(period_gathering, sqlite3_connection)

##process the customer flows data to prepare it for the inventory joining

df[['item_number','production_facility','grade','spec','age']] = df['Product Name'].str.split('_', expand=True)

df['age'] = df['age'].str.replace('D','')
df['age'] = df['age'].astype(int)

df['Departing Period Name'] = df['Departing Period Name'].astype(int)

df = pd.merge(df, periods_df, left_on='Departing Period Name', right_on='PERIOD_NUMBER', how='left')
df['SHIP_DATE'] = df['DATE_FORMATTED'].str.split(' ').str[0]

#create a column for the starting age of the product in order to match it to the base inventory data
df['start_age'] = df['age'] - df['Departing Period Name'] + 1

#create a join column by concatenating the relevant columns 
df_cols = ['Source Name','item_number','production_facility','grade','spec','start_age']
df['join_col'] = df[df_cols].astype(str).agg(''.join, axis=1)
inv_cols = ['WHS_CODE','ITEM_NUMBER','PRODUCTION_PLANT','GRADE','CLEANED_SPEC','AGE']
inv_df['join_col'] = inv_df[inv_cols].astype(str).agg(''.join, axis=1)

#duplicate rows in the customer flows so there is one row for each pallet needed.
#if there are 15 units going into filling an order, I want 15 rows duplicated
df = df.loc[df.index.repeat(df['Flow Units'])].reset_index(drop=True)

#drop columns that arent relevant
#df = df.drop(['Arriving Period Name','Temperature Class','Hazardous Goods','Shipment Size Basis','Organization Name','Mode','Flow Cubic', 'Service Hours', 'Service Distance', 'Flow Revenue', 'Outbound Warehousing Policy Cost', 'Transportation Policy Cost', 'Sourcing Policy Cost', 'Duty Cost', 'In Transit Inventory', 'Intransit Inventory Holding Cost', 'CO2', 'CO2 Cost', 'Total Cost', 'Lead Time Cost', 'Sourcing Process Cost', 'Transportation Process Cost', 'Outbound Inventory Process Cost', 'Total Outbound Warehousing Cost', 'Total Sourcing Cost', 'Total Transportation Cost', 'Departing Period Number', 'Arriving Period Number', 'Scenario ID', 'Sub-Scenario ID'], axis=1)

df = df.drop(['Scenario', 'Departing Period Name','Arriving Period Name', 'Product Name', 'Temperature Class', 'Hazardous Goods', 'Organization Name', 'Mode', 'Total Demand', 'Total Demand Served', 'Shipment Size', 'Shipment Size Basis', 'Flow Units', 'Flow Weight', 'Flow Cubic', 'Service Hours', 'Service Distance', 'Flow Revenue', 'Outbound Warehousing Policy Cost', 'Transportation Policy Cost', 'Sourcing Policy Cost', 'Duty Cost', 'In Transit Inventory', 'Intransit Inventory Holding Cost', 'CO2', 'CO2 Cost', 'Total Cost', 'Lead Time Cost', 'Sourcing Process Cost', 'Transportation Process Cost', 'Outbound Inventory Process Cost', 'Total Outbound Warehousing Cost', 'Total Sourcing Cost', 'Total Transportation Cost', 'Departing Period Number', 'Arriving Period Number', 'Scenario ID', 'Sub-Scenario ID', 'PERIOD_NUMBER', 'DATE_FORMATTED', 'age'], axis=1)
inv_df = inv_df.drop(['AGE', 'TOTAL_WEIGHTS', 'TOTAL_PALLETS', 'ITEM_NUMBER', 'GRADE', 'PRODUCTION_PLANT', 'SPEC', 'WHS_CODE', 'JOINER', 'CLEANED_SPEC', 'SPEC_VALUE',], axis=1)


#remove this after testing
df.to_csv('customer_flows.csv')
inv_df.to_csv('inventory.csv')


#seperate the customer flows into a df for the any_whs scenario
#any_df = df[df['Scenario'] == 'any_whs']
#specific_df = df[df['Scenario'] == 'specified_whs']

#function to ascribe the inventory to the customer flows
joined_df = pd.DataFrame(columns=['join_col','Customer Name','Source Name','SHIP_DATE','item_number', 'production_facility', 'grade', 'spec', 'start_age','LOT_NO','SUBLOT_NO'])


'item_number', 'production_facility', 'grade', 'spec', 'start_age',

print('Connecting inventory lot and sublot to customer flows data.  This is slow...')
tic = time.perf_counter()
# Iterate through each row of df1
for index1, row1 in df.iterrows():
    match_found = False
    # Iterate through rows of df_pool to find a match
    for index_pool, row_pool in inv_df.iterrows():
        if row1['join_col'] == row_pool['join_col']:
            # Append the matched row to the joined_df
            joined_df = pd.concat([joined_df, pd.DataFrame([{'join_col': row1['join_col'], 
                                                              'Customer Name': row1['Customer Name'],
                                                              'Source Name': row1['Source Name'],
                                                              'item_number': row1['item_number'],
                                                              'production_facility': row1['production_facility'],
                                                              'grade': row1['grade'],
                                                              'spec': row1['spec'],
                                                              'start_age': row1['start_age'],
                                                              'SHIP_DATE': row1['SHIP_DATE'], 
                                                              'LOT_NO': row_pool['LOT_NO'],
                                                              'SUBLOT_NO': row_pool['SUBLOT_NO']
                                                              }]),
                                  ], ignore_index=True)
            
            # Remove the matched row from df_pool
            inv_df = inv_df.drop(index_pool)
            match_found = True
            break  # Move to the next row in df1 after finding a match
toc = time.perf_counter()
print(f"Joined inventory and customer flows in {toc - tic:0.4f} seconds")

joined_df.to_csv('joined_df.csv', index=False)