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

path = 'S:\Supply_Chain\Analytics\Inventory Allocation Maximization\Python\joined_df.csv'
output_path = 'S:\Supply_Chain\Analytics\Inventory Allocation Maximization\Python\cleaned_df.csv'

joined_df = pd.read_csv(path)

joined_df['order number'] = joined_df['Customer Name'].str.split('_').str[1]

joined_df = joined_df.rename(columns={'Source Name': 'whs code', 'SHIP_DATE':'ship date', 'item_number':'sku', 'start_age':'age', 'LOT_NO':'lot no','SUBLOT_NO':'sublot no','production_facility':'production facility'})
column_order = ['order number','ship date','whs code','lot no','sublot no','sku','production facility','grade','spec','age']
joined_df = joined_df[column_order]

#print(df.head())

joined_df.to_csv(output_path, index=False)