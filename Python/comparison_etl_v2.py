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

#Ask user for the run num
run_num = input('Enter the model run iteration.  Day 1 is with inventory snapshot 6/1: ')
run_num = int(run_num)
run_num_plus_one = run_num + 1
run_num = str(run_num)
run_num_plus_one = str(run_num_plus_one)
if len(run_num) == 1:
    run_num = '0' + run_num
if len(run_num_plus_one) == 1:
    run_num_plus_one = '0' + run_num_plus_one

#these two lines are just so I dont have to enter a date everytime I run a test.  They should be commented out once the project is complete
#date_string = '2025-05-07'
#start_date = parse(date_string)

#Make a Sqlite connection

sqlite3_db = 'inventory_allocation_maximization.db'
sqlite3_conn_path = 'P:/sqlite3/sqlite-tools/'+sqlite3_db

sqlite3_connection = sqlite3.connect(sqlite3_conn_path)
cursor = sqlite3_connection.cursor()
print('Successfully connected to the database')

#path variables
#inventory pool
inv_pool_path = 'S:\\Supply_Chain\\Analytics\Inventory Allocation Maximization\\Comparison\\part 2\\model\\inventory_pool202506' + run_num + '.csv'

#datawarehouse inventory
inv_dw_path = 'S:\\Supply_Chain\\Analytics\\Inventory Allocation Maximization\\Comparison\\part 2\\model\\pbi outputs\\inventory202506' + run_num_plus_one + '.csv'

#datawarehouse orders
orders_dw_path = 'S:\\Supply_Chain\\Analytics\\Inventory Allocation Maximization\\Comparison\\part 2\\model\\pbi outputs\\orders202506' + run_num_plus_one + '.csv'

#customer flows
cf_path = 'S:\\Supply_Chain\\Analytics\\Inventory Allocation Maximization\\Comparison\\part 2\\model\\pbi outputs\\Product Allocation Maximization - Customer Flows202506' + run_num + '.xlsx'

#model output inventory
inv_model_path = 'S:\\Supply_Chain\\Analytics\\Inventory Allocation Maximization\\Comparison\\part 2\model\\table snapshots\\inventory202506' + run_num + '.csv'

#model output periods
periods_path = 'S:\\Supply_Chain\\Analytics\\Inventory Allocation Maximization\\Comparison\\part 2\model\\table snapshots\\periods202506' + run_num + '.csv'

#load dataframes
inv_pool_df = pd.read_csv(inv_pool_path)
inv_dw_df = pd.read_csv(inv_dw_path)
cf_df = pd.read_excel(cf_path)
inv_model_df = pd.read_csv(inv_model_path)
periods_df = pd.read_csv(periods_path)

#add a big concat of item grade spec whs sublot and lot
inv_dw_df = inv_dw_df.astype(str)
inv_dw_df['JOINER'] = inv_dw_df['WHSE_CODE'] + inv_dw_df['ITEM_NUMBER'] + inv_dw_df['PRODUCTION_PLANT'] + inv_dw_df['QC_GRADE'] + inv_dw_df['SPEC'] + inv_dw_df['LOT_NO'] + inv_dw_df['SUBLOT_NO']

#Find the rows not in the pool and then add them into the pool
#it takes the pool from the model run date and compares it against the inventory from the datawarehouse from the next day
inv_pool_add = inv_dw_df[~inv_dw_df['JOINER'].isin(inv_pool_df['JOINER'])]

inv_pool_df = pd.concat([inv_pool_df, inv_pool_add])

inv_pool_df.to_csv('test.csv')

cf_df[['item_number','production_facility','grade','spec','age']] = cf_df['Product Name'].str.split('_', expand=True)

cf_df['age'] = cf_df['age'].str.replace('D','')
cf_df['age'] = cf_df['age'].astype(int)

cf_df['Departing Period Name'] = cf_df['Departing Period Name'].astype(int)

cf_df = pd.merge(cf_df, periods_df, left_on='Departing Period Name', right_on='PERIOD_NUMBER', how='left')
cf_df['SHIP_DATE'] = cf_df['DATE_FORMATTED'].str.split(' ').str[0]

#create a column for the starting age of the product in order to match it to the base inventory data
cf_df['start_age'] = cf_df['age'] - cf_df['Departing Period Name'] + 1

#create a join column by concatenating the relevant columns 
cf_df_cols = ['Source Name','item_number','production_facility','grade','spec','start_age']
cf_df['join_col'] = cf_df[cf_df_cols].astype(str).agg(''.join, axis=1)
inv_cols = ['WHS_CODE','ITEM_NUMBER','PRODUCTION_PLANT','GRADE','CLEANED_SPEC','AGE']
inv_model_df['join_col'] = inv_model_df[inv_cols].astype(str).agg(''.join, axis=1)

#duplicate rows in the customer flows so there is one row for each pallet needed.
#if there are 15 units going into filling an order, I want 15 rows duplicated
cf_df = cf_df.loc[cf_df.index.repeat(cf_df['Flow Units'])].reset_index(drop=True)

#seperate the customer flows into only period 1 since thats the only one we will be taking out of the datawarehouse inventory
run_num_int = int(run_num)
cf_df = cf_df[cf_df['Departing Period Name'] <= 13 + run_num_int]


#drop columns and rows that arent relevant
cf_df = cf_df[cf_df['Customer Name'] != 'Trash']
cf_df = cf_df.drop(['Departing Period Name','Arriving Period Name', 'Product Name', 'Temperature Class', 'Hazardous Goods', 'Organization Name', 'Mode', 'Total Demand', 'Total Demand Served', 'Shipment Size', 'Shipment Size Basis', 'Flow Units', 'Flow Weight', 'Flow Cubic', 'Service Hours', 'Service Distance', 'Flow Revenue', 'Outbound Warehousing Policy Cost', 'Transportation Policy Cost', 'Sourcing Policy Cost', 'Duty Cost', 'In Transit Inventory', 'Intransit Inventory Holding Cost', 'CO2', 'CO2 Cost', 'Total Cost', 'Lead Time Cost', 'Sourcing Process Cost', 'Transportation Process Cost', 'Outbound Inventory Process Cost', 'Total Outbound Warehousing Cost', 'Total Sourcing Cost', 'Total Transportation Cost', 'Departing Period Number', 'Arriving Period Number', 'Scenario ID', 'Sub-Scenario ID', 'PERIOD_NUMBER', 'DATE_FORMATTED', 'age'], axis=1)
inv_model_df = inv_model_df.drop(['AGE', 'TOTAL_WEIGHTS', 'TOTAL_PALLETS', 'ITEM_NUMBER', 'GRADE', 'PRODUCTION_PLANT', 'SPEC', 'WHS_CODE', 'JOINER', 'CLEANED_SPEC', 'SPEC_VALUE'], axis=1)

#seperate the customer flows into a df for the any_whs scenario
any_df = cf_df[cf_df['Scenario'] == 'any_whs']
specific_df = cf_df[cf_df['Scenario'] == 'specified_whs']

#function to ascribe the inventory to the customer flows
allocated_lots_df = pd.DataFrame(columns=['join_col','Customer Name','Source Name','SHIP_DATE','item_number', 'production_facility', 'grade', 'spec', 'start_age','LOT_NO','SUBLOT_NO'])

print('Connecting inventory lot and sublot to customer flows data.  This is slow...')
tic = time.perf_counter()
# Iterate through each row of df1
for index1, row1 in specific_df.iterrows():
    match_found = False
    # Iterate through rows of df_pool to find a match
    for index_pool, row_pool in inv_model_df.iterrows():
        if row1['join_col'] == row_pool['join_col']:
            # Append the matched row to the allocated_lots_df
            allocated_lots_df = pd.concat([allocated_lots_df, pd.DataFrame([{'join_col': row1['join_col'], 
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
            inv_model_df = inv_model_df.drop(index_pool)
            match_found = True
            break  # Move to the next row in df1 after finding a match
toc = time.perf_counter()
print(f"Joined inventory and customer flows in {toc - tic:0.4f} seconds")

#a little cleaning of the joined df
allocated_lots_df['Order Number'] = allocated_lots_df['Customer Name'].str.split('_').str[1]
allocated_lots_df = allocated_lots_df.drop(['join_col','Customer Name'], axis=1)
allocated_lots_df = allocated_lots_df.rename(columns={'Source Name': 'whs code', 'SHIP_DATE':'ship date', 'item_number':'sku', 'start_age':'age', 'LOT_NO':'lot no','SUBLOT_NO':'sublot no','production_facility':'production facility'})
column_order = ['Order Number','ship date','whs code','lot no','sublot no','sku','production facility','grade','spec','age']
allocated_lots_df = allocated_lots_df[column_order]


#I need to remove the inventory that got allocated by the first model run


#remove the rows in the inventory dw df that are in the allocated date from period 1 based on sublot no
inv_pool_df = inv_pool_df[~inv_pool_df['SUBLOT_NO'].isin(allocated_lots_df['sublot no'])]

allocated_lots_df.to_csv('S:\Supply_Chain\Analytics\Inventory Allocation Maximization\Comparison\part 2\model\\allocated lot and sublot files\\allocated_lots202506' + run_num + '.csv')
inv_pool_df.to_csv('S:\\Supply_Chain\\Analytics\\Inventory Allocation Maximization\\Comparison\\part 2\\model\\inventory_pool202506' + run_num_plus_one + '.csv')
#ord_pool_df.to_csv('S:\\Supply_Chain\\Analytics\\Inventory Allocation Maximization\\Comparison\\part 2\\model\\orders_pool202506' + run_num_plus_one + '.csv')