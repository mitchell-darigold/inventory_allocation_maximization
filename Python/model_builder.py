import pandas as pd
import sqlite3
from datetime import date, timedelta, datetime

#Ask user for date to use with the loading.  It needs to be the date the data was pulled on

#while True:
#    date_string = input("Enter a date in YYYY-MM-DD format: ")
#    try:
#        date_object = datetime.strptime(date_string, "%Y-%m-%d").date()
#        break
#    except ValueError:
#        print("Invalid date format. Please use YYYY-MM-DD.")

#print("You entered:", date_object)
date_string = '2025-05-01'

#Make a Sqlite connection

sqlite3_db = 'inventory_allocation_maximization.db'
sqlite3_conn_path = 'P:/sqlite3/sqlite-tools/'+sqlite3_db

sqlite3_connection = sqlite3.connect(sqlite3_conn_path)
cursor = sqlite3_connection.cursor()
print('Successfully connected to the database')

###############################################################Variable list############################################################3

#Paths
iam_inventory_path = 'S:\\Supply_Chain\\Analytics\\Inventory Allocation Maximization\\Master\\inventory.csv'
iam_item_path = 'S:\\Supply_Chain\\Analytics\\Inventory Allocation Maximization\\Master\\item.csv'
iam_orders_path = 'S:\\Supply_Chain\\Analytics\\Inventory Allocation Maximization\\Master\\orders.csv'

#f strings
iam_inventory_table = 'iam_inventory'
iam_item_table = 'iam_item'
iam_orders_table = 'iam_orders'
iam_age_joiner_table = 'iam_age_joiner'
iam_distinct_inventory_products_table = 'iam_distinct_inventory_products'
iam_distinct_whs_products_table = 'iam_distinct_whs_products'

###############sql queries#####################

#create tables

def drop_table(table_name):
    table = table_name
    query = f'drop table if exists {table}'
    return query

def create_iam_inventory(table_name):
    table = table_name
    query = f'''create table {table}(
    AGE REAL,
    TOTAL_WEIGHT REAL,
    TOTAL_PALLETS REAL,
    ITEM_NUMBER TEXT,
    LOT_NO TEXT,
    GRADE TEXT,
    PRODUCTION_PLANT TEXT,
    SPEC TEXT,
    WHS_CODE TEXT,
    SUBLOT_NO TEXT,
    JOINER TEXT
    );'''
    return query

iam_orders_table_date_clean_min = 'delete from iam_orders where date(SHIP_DATE_FORMATTED) < date(?);'
iam_orders_table_date_clean_max = '''delete from iam_orders where date(SHIP_DATE_FORMATTED) > date(?, '+120 days');'''
create_table_iam_distinct_inventory_products = '''create table iam_distinct_inventory_products as
    select distinct
    p.item_number || "_" || p.production_plant || "_" ||  p.grade || "_" ||  p.cleaned_spec || "_" ||  p.age_joiner || "D" as MODEL_NAME
    ,p.age_joiner as age
    ,p.production_plant
    ,p.grade
    ,p.cleaned_spec
    ,p.item_number

    from (
        --this section joins the raw inventory data with a joiner table to produce 1 row for every possible age for each sku_prod facility_grade_spec combination at or above the current age of the product in inventory
        --max age for a product is 120 as defined in the mvp_age_joiner table
        select mi.*
        ,aj.age as age_joiner

        from iam_inventory mi

        left join iam_age_joiner aj
        on mi.joiner=aj.joiner

        where 1=1
        and aj.age >= mi.age
    ) p
    ;'''

create_table_products = '''select 
a.model_name as Name
,'' as 'Unit Value'
,'' as 'Unit Price'
,'' as 'Unit Weight'
,'' as 'Unit Volume'
,'' as 'Type'
,'' as 'Class'
,'1' as 'Shelf Life'
,'' as 'Lead Time Cost/Day/Unit'
,'' as 'Maximum Lead Time x Flow'
,'' as 'Start Date'
,'' as 'End Date'
,'' as 'Description'
,'include' as 'Status'
,'' as 'Notes'
,'' as 'Custom 1'
,'' as 'Custom 2'
,'' as 'Custom 3'
,'' as 'Custom 4'
,'' as 'Temperature Class'
,'' as 'Hazardous Goods'
,'' as 'Maximum Risk'

from (
    select distinct model_name
    from iam_distinct_inventory_products

    union

    select distinct
    m.item_number || "_" || m.production_plant || "_" ||  m.grade || "_" ||  m.cleaned_spec || "_" ||  m.age_minus_one || 'D'

    from (
        select *
        ,cast(age as int) - 1 as age_minus_one
        from iam_distinct_whs_products
    ) m
) a
;'''

create_table_iam_distinct_whs_products = '''create table iam_distinct_whs_products as
    select distinct
    p.item_number || "_" || p.production_plant || "_" ||  p.grade || "_" ||  p.cleaned_spec || "_" ||  p.age_joiner || "D" as MODEL_NAME
    ,p.age_joiner as age
    ,p.production_plant
    ,p.grade
    ,p.cleaned_spec
    ,p.item_number
    ,p.whs_code

    from (
        --this section joins the raw inventory data with a joiner table to produce 1 row for every possible age for each sku_prod facility_grade_spec combination at or above the current age of the product in inventory
        --max age for a product is 120 as defined in the mvp_age_joiner table
        select mi.*
        ,aj.age as age_joiner

        from iam_inventory mi

        left join iam_age_joiner aj
        on mi.joiner=aj.joiner

        where 1=1
        and aj.age >= mi.age
    ) p
    ;'''

###############################################Import data section###################################################

#load inventory
iam_inventory_df = pd.read_csv(iam_inventory_path)
#add an additional column for joining age data later
iam_inventory_df['JOINER'] = 1
iam_inventory_df['SPEC'] = iam_inventory_df['SPEC'].fillna('9999A')
#load item
iam_item_df = pd.read_csv(iam_item_path)
#fill null grade values with 101
iam_item_df['Grade'] = iam_item_df['Grade'].fillna('101')
#fill null spec values with 9999A
iam_item_df['Spec'] = iam_item_df['Spec'].fillna('9999A')
#load orders
iam_orders_df = pd.read_csv(iam_orders_path)
#add an additional column formatting the date correctly
iam_orders_df['SHIP_DATE_FORMATTED'] = iam_orders_df['Ship Date'].str.split(' ').str[0]
#load an age joiner table
iam_age_joiner_df = pd.DataFrame({'AGE': range(1, 121), 'JOINER':1})

#clean column names
iam_inventory_df = iam_inventory_df.rename(columns={'Sum of AGE': 'AGE', 'Sum of Inventory LBS':'TOTAL_WEIGHTS', 'Sum of Inventory Pallets':'TOTAL_PALLETS', 'QC_GRADE':'GRADE', 'WHSE_CODE':'WHS_CODE'})
iam_item_df = iam_item_df.rename(columns={'Grade': 'GRADE', 'Item#':'ITEM_NUMBER', 'Sum of Lbs per Pallet':'PALLET_WEIGHT', 'Sum of Max Age at Shipment':'MAX_AGE', 'Spec':'SPEC'})
iam_orders_df = iam_orders_df.rename(columns={'Approved Plant 1': 'APPROVED_PLANT_1', 'Approved Plant 2':'APPROVED_PLANT_2', 'Approved Plant 3':'APPROVED_PLANT_3', 'Demand Type':'DEMAND_TYPE', 'Grade':'GRADE', 'Sum of Max Age at Time of Shipment':'MAX_AGE', 'Item#':'ITEM_NUMBER', 'Order#':'ORDER_NUMBER', 'Sum of Ordered LBS':'ORDERED_WEIGHT', 'Sum of Ordered Pallets':'ORDERED_PALLETS', 'Ship Date':'SHIP_DATE', 'Shipping Warehouse Code':'SHIPPING_WHS_CODE', 'Spec':'SPEC'})

#################################################Clean up the specs######################################################

#convert the item df into a item spec list format
spec_df = iam_item_df.groupby('ITEM_NUMBER')['SPEC'].apply(list).reset_index()
spec_df = spec_df.rename(columns={'SPEC': 'ALLOWED_SPEC'})
#convert the spec column from the inventory df into a column of lists
iam_inventory_df['SPEC_LISTED'] = iam_inventory_df['SPEC'].str.split('-')
#left join the allowed specs onto the inventory df
iam_inventory_spec_fix_df = pd.merge(iam_inventory_df, spec_df, on='ITEM_NUMBER', how='left')
#create a function to remove specs that dont show up in the allowed spec lists
def filter_specs(df, col_getting_filtered, allowed_specs):
    df[col_getting_filtered] = df.apply(lambda row: [x for x in row[col_getting_filtered] if x in row[allowed_specs]], axis=1)
    return df
#run the function
iam_inventory_spec_fix_df = filter_specs(iam_inventory_spec_fix_df, 'SPEC_LISTED', 'ALLOWED_SPEC')
#create a new column to hold the functions result
iam_inventory_spec_fix_df['CLEANED_SPEC'] = iam_inventory_spec_fix_df['SPEC_LISTED'].apply(lambda x: '-'.join(map(str,x)))
#remove the unneeded columns
iam_inventory_spec_fix_df = iam_inventory_spec_fix_df.drop('SPEC_LISTED', axis=1)
iam_inventory_spec_fix_df = iam_inventory_spec_fix_df.drop('ALLOWED_SPEC', axis=1)


###################################################Load data into sqlite##################################################
try:
    cursor.execute(drop_table(iam_inventory_table))
    iam_inventory_spec_fix_df.to_sql(iam_inventory_table, sqlite3_connection, if_exists='replace', index=False)
except Exception as ex:
    print(ex)

try:
    cursor.execute(drop_table(iam_item_table))
    iam_item_df.to_sql(iam_item_table, sqlite3_connection, if_exists='replace', index=False)
except Exception as ex:
    print(ex)

try:
    cursor.execute(drop_table(iam_orders_table))
    iam_orders_df.to_sql(iam_orders_table, sqlite3_connection, if_exists='replace', index=False)
    cursor.execute(iam_orders_table_date_clean_min, (date_string,))
    cursor.execute(iam_orders_table_date_clean_max, (date_string,))
except Exception as ex:
    print(ex)

try:
    cursor.execute(drop_table(iam_age_joiner_table))
    iam_age_joiner_df.to_sql(iam_age_joiner_table, sqlite3_connection, if_exists='replace', index=False)
except Exception as ex:
    print(ex)

try:
    cursor.execute(drop_table(iam_distinct_inventory_products_table))
    cursor.execute(create_table_iam_distinct_inventory_products)
except Exception as ex:
    print(ex)

try:
    cursor.execute(drop_table(iam_distinct_whs_products_table))
    cursor.execute(create_table_iam_distinct_whs_products)
except Exception as ex:
    print(ex)

#Commit changes

sqlite3_connection.commit()


#Create meta tables




#Output coupa model tables
products_table_df = pd.read_sql_query(create_table_products, sqlite3_connection)


with pd.ExcelWriter('S:\\Supply_Chain\\Analytics\\Inventory Allocation Maximization\\Master\\model.xlsx') as writer:
    products_table_df.to_excel(writer, sheet_name='Products', index=False)


#Close the sqlite connection
sqlite3_connection.close()
print("Connection to power_bi_data is closed")