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

df = pd.read_excel(iam_customer_flows_path)

df[['item_number','production_facility','grade','spec','age']] = df['Product Name'].str.split('_', expand=True)

any_df = df[df['Scenario'] == 'any_whs']

specific_df = df[df['Scenario'] == 'specified_whs']

inv_df = pd.read_excel(iam_inventory_path)

