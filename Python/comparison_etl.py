import pandas as pd
import sqlite3
from datetime import datetime, timedelta
from datetime import date
from dateutil.parser import parse
import tkinter
from tkinter import filedialog
import numpy as np
import math

#Ask user for date to use with the loading.  It needs to be the date the data was pulled on

while True:
    date_string = input("Enter the date you pulled the data on in YYYY-MM-DD format: ")
    try:
        date_object = datetime.strptime(date_string, "%Y-%m-%d").date()
        break
    except ValueError:
       print("Invalid date format. Please use YYYY-MM-DD.")
print("You entered:", date_object)

start_date = parse(date_string)
#subtracting one day from the entered date
start_date_minus_1_day = start_date - timedelta(days=1)
#converting the start date minus 1 to a string
date_string_minus_1 = start_date_minus_1_day.strftime('%Y-%m-%d')
#variable for use in paths 
date_string_minus_1_cleaned = date_string_minus_1.replace('-','')
date_string_cleaned = date_string.replace('-','')

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

inv_dw = 'S:\\Supply_Chain\\Analytics\\Inventory Allocation Maximization\\Comparison\\part 2\\model\\pbi outputs\\inventory' + date_string_cleaned + '.csv'

ord_path = 'S:\\Supply_Chain\\Analytics\\Inventory Allocation Maximization\\Comparison\\part 2\\model\\pbi outputs\\orders' + date_string_cleaned + '.csv'

inv_model = 'S:\\Supply_Chain\\Analytics\\Inventory Allocation Maximization\\Comparison\\part 2\model\\table snapshots\\inventory' + date_string_minus_1_cleaned + '.csv'

periods = 'S:\\Supply_Chain\\Analytics\\Inventory Allocation Maximization\\Comparison\\part 2\model\\table snapshots\\periods' + date_string_minus_1_cleaned + '.csv'

cust_flow = 'S:\\Supply_Chain\\Analytics\\Inventory Allocation Maximization\\Comparison\\part 2\\model\\Product Allocation Maximization - Customer Flows' + date_string_minus_1_cleaned + '.xlsx'

inv_dw_df = pd.read_csv(inv_dw)

ord_df = pd.read_csv(ord_path)

cf_df = pd.read_excel(cust_flow)

inv_model_df = pd.read_csv(inv_model)

periods_df = pd.read_csv(periods)

######inventory manipulations

#pull out the inventory that doesnt have any allocation
inv_no_allocation = inv_dw_df[inv_dw_df['ALLOCATED_ORDER_NUM'].isna()]
#this compares the inventory allocated order number against the orders, if there are allocated order numbers in inventory but not in orders we remove them.  This is the logic to find transfer orders according to aaron
inv_no_transfers = inv_dw_df[inv_dw_df['ALLOCATED_ORDER_NUM'].isin(ord_df['Order#'])]

#this pulls out the inventory from the model's allocations


#mush the two dfs together to get the complete set of inventory we want to use in the model run
#there will need to be a step where we remove the inventory that gets allocated by the model, but this is for day 1 where we havent run the model yet.
inv_df = pd.concat([inv_no_allocation, inv_no_transfers], ignore_index=True, axis=0)

######order manipulations
ord_df = ord_df.drop(['Request Date','Order Date'],axis=1)





########this is ported from the model_output_processing.py script and adapted for the use case of comaprison

##process the customer flows data to prepare it for the inventory joining

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

#drop columns and rows that arent relevant
cf_df = cf_df[cf_df['Customer Name'] != 'Trash']
cf_df = cf_df.drop(['Departing Period Name','Arriving Period Name', 'Product Name', 'Temperature Class', 'Hazardous Goods', 'Organization Name', 'Mode', 'Total Demand', 'Total Demand Served', 'Shipment Size', 'Shipment Size Basis', 'Flow Units', 'Flow Weight', 'Flow Cubic', 'Service Hours', 'Service Distance', 'Flow Revenue', 'Outbound Warehousing Policy Cost', 'Transportation Policy Cost', 'Sourcing Policy Cost', 'Duty Cost', 'In Transit Inventory', 'Intransit Inventory Holding Cost', 'CO2', 'CO2 Cost', 'Total Cost', 'Lead Time Cost', 'Sourcing Process Cost', 'Transportation Process Cost', 'Outbound Inventory Process Cost', 'Total Outbound Warehousing Cost', 'Total Sourcing Cost', 'Total Transportation Cost', 'Departing Period Number', 'Arriving Period Number', 'Scenario ID', 'Sub-Scenario ID', 'PERIOD_NUMBER', 'DATE_FORMATTED', 'age'], axis=1)
inv_model_df = inv_model_df.drop(['AGE', 'TOTAL_WEIGHTS', 'TOTAL_PALLETS', 'ITEM_NUMBER', 'GRADE', 'PRODUCTION_PLANT', 'SPEC', 'WHS_CODE', 'JOINER', 'CLEANED_SPEC', 'SPEC_VALUE',], axis=1)


#remove this after testing
#df.to_csv('customer_flows.csv')
#inv_df.to_csv('inventory.csv')

#seperate the customer flows into only period 1 since thats the only one we will be taking out of the datawarehouse inventory
cf_df = cf_df[cf_df['Departing Period Name'] == '1']

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
allocated_lots_df = allocated_lots_df.drop(['join_col','Customer Name'])
allocated_lots_df = allocated_lots_df.rename(columns={'Source Name': 'whs code', 'SHIP_DATE':'ship date', 'item_number':'sku', 'start_age':'age', 'LOT_NO':'lot no','SUBLOT_NO':'sublot no','production_facility':'production facility'})
column_order = ['order number','ship date','whs code','lot no','sublot no','sku','production facility','grade','spec','age']
allocated_lots_df = allocated_lots_df[column_order]

#output files
#this is the lot and sublot data that needs to be taken out of the datawarehouse inventory.
#this file is from the model run of the previous day and needs to be taken from the datawarehouse for the date_string_cleaned date
allocated_lots_df.to_csv('S:\\Supply_Chain\\Analytics\\Inventory Allocation Maximization\\Comparison\part 2\\model\\allocated lot and sublot files\\allocated_lots_df' + date_string_cleaned + '.csv', index=False)


inv_df = inv_df[inv_df['SUBLOT_NO'].isin(allocated_lots_df['SUBLOT_NO'])]



