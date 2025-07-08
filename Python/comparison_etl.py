import pandas as pd
import sqlite3
from datetime import datetime, timedelta
from datetime import date
from dateutil.parser import parse
import tkinter
from tkinter import filedialog
import numpy as np
import math

inv_path_day1 = 'S:\\Supply_Chain\\Analytics\\Inventory Allocation Maximization\\Comparison\\part 2\\model\\inventory20250601.csv'
inv_path_day2 = 'S:\\Supply_Chain\\Analytics\\Inventory Allocation Maximization\\Comparison\\part 2\\model\\inventory20250602.csv'
inv_path_day3 = 'S:\\Supply_Chain\\Analytics\\Inventory Allocation Maximization\\Comparison\\part 2\\model\\inventory20250603.csv'
inv_path_day4 = 'S:\\Supply_Chain\\Analytics\\Inventory Allocation Maximization\\Comparison\\part 2\\model\\inventory20250604.csv'
inv_path_day5 = 'S:\\Supply_Chain\\Analytics\\Inventory Allocation Maximization\\Comparison\\part 2\\model\\inventory20250605.csv'

ord_path_day1 = 'S:\\Supply_Chain\\Analytics\\Inventory Allocation Maximization\\Comparison\\part 2\\model\\orders20250601.csv'
ord_path_day2 = 'S:\\Supply_Chain\\Analytics\\Inventory Allocation Maximization\\Comparison\\part 2\\model\\orders20250602.csv'
ord_path_day3 = 'S:\\Supply_Chain\\Analytics\\Inventory Allocation Maximization\\Comparison\\part 2\\model\\orders20250603.csv'
ord_path_day4 = 'S:\\Supply_Chain\\Analytics\\Inventory Allocation Maximization\\Comparison\\part 2\\model\\orders20250604.csv'
ord_path_day5 = 'S:\\Supply_Chain\\Analytics\\Inventory Allocation Maximization\\Comparison\\part 2\\model\\orders20250605.csv'

inv_d1_df = pd.read_csv(inv_path_day1)

ord_d1_df = pd.read_csv(ord_path_day1)

######inventory manipulations

#pull out the inventory that doesnt have any allocation
inv_d1_no_allocation = inv_d1_df[inv_d1_df['ALLOCATED_ORDER_NUM'].isna()]
#this compares the inventory allocated order number against the orders, if there are allocated order numbers in inventory but not in orders we remove them.  This is the logic to find transfer orders according to aaron
inv_d1_no_transfers = inv_d1_df[inv_d1_df['ALLOCATED_ORDER_NUM'].isin(ord_d1_df['Order#'])]

#mush the two dfs together to get the complete set of inventory we want to use in the model run
#there will need to be a step where we remove the inventory that gets allocated by the model, but this is for day 1 where we havent run the model yet.
inv_d1_df = pd.concat([inv_d1_no_allocation, inv_d1_no_transfers], ignore_index=True, axis=0)


######order manipulations
ord_d1_df = ord_d1_df.drop(['Request Date','Actual Ship Date','Order Date'],axis=1)



inv_d1_df.to_csv('S:\\Supply_Chain\\Analytics\\Inventory Allocation Maximization\\Comparison\\part 2\\model\\model_inventory_20250601.csv')
ord_d1_df.to_csv('S:\\Supply_Chain\\Analytics\\Inventory Allocation Maximization\\Comparison\\part 2\\model\\model_order_20250601.csv')