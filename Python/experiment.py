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
run_num = input('Enter the model run iteration.  Day 1 is with inventory snapshot 6/1: ')
str(run_num)
if len(run_num) == 1:
    run_num = '0' + run_num

print(run_num)

run_num = int(run_num)

print(run_num)