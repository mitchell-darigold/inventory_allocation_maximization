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


while True:
    date_string = input("Enter the date you pulled the data on in YYYY-MM-DD format: ")
    try:
        date_object = datetime.strptime(date_string, "%Y-%m-%d").date()
        break
    except ValueError:
       print("Invalid date format. Please use YYYY-MM-DD.")
print("You entered:", date_object)

start_date = parse(date_string)

start_date_minus_1_day = start_date - timedelta(days=1)

date_string_minus_1 = start_date_minus_1_day.strftime('%Y-%m-%d')

print(date_string_minus_1)

date_string_minus_1_cleaned = date_string_minus_1.replace('-','')

print(date_string_minus_1_cleaned)

date_string_cleaned = date_string.replace('-','')

print(date_string_cleaned)


path = 'S:\Supply_Chain\Analytics\Inventory Allocation Maximization\Python\joined_df.csv'
output_path = 'S:\Supply_Chain\Analytics\Inventory Allocation Maximization\Python\cleaned_df.csv'

ord_path_day1 = 'S:\\Supply_Chain\\Analytics\\Inventory Allocation Maximization\\Comparison\\part 2\\model\\orders20250601.csv'

#df = pd.read_csv(ord_path_day1)

#add an additional column formatting the date correctly
#df['SHIP_DATE_FORMATTED'] = df['Ship Date'].str.split(' ').str[0]

#df['SHIP_DATE_FORMATTED_dtobject'] = pd.to_datetime(df['SHIP_DATE_FORMATTED'])


#df.to_csv('S:\\Supply_Chain\\Analytics\\Inventory Allocation Maximization\\Comparison\\part 2\\model\\test.csv',index=False)

#print(df.head())


#iam_orders_df['SHIP_DATE_FORMATTED'] = datetime.strptime(iam_orders_df['SHIP_DATE_FORMATTED'], '%Y-%m-%d')
#iam_orders_df['SHIP_DATE_FORMATTED'] = pd.to_datetime(iam_orders_df['SHIP_DATE_FORMATTED'], format='%#m/%#d/%Y')
#iam_orders_df['SHIP_DATE_FORMATTED'] = iam_orders_df['SHIP_DATE_FORMATTED'].dt.strftime('%Y-%m-%d')