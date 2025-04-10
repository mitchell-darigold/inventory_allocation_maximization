#there are many steps that need to happen to evolve this further.
#I need to add a section that pulls the allowed_specs list automatically with a SQL query connection
#when there are multiple products how do I tell the computer which list to use against which row

import pandas as pd
import sqlite3

#sqlite3 connection path
sqlite3_db = 'inventory_allocation_maximization.db'
sqlite3_conn_path = 'P:/sqlite3/sqlite-tools/'+sqlite3_db

#f strings
mvp_inventory_table = 'mvp_inventory'

#sql queries:
mvp_inventory_query = 'select * from mvp_inventory'

def drop_table(table_name):
    table = table_name
    query = f'drop table if exists {table}'
    return query

def create_mvp_inventory(table_name):
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
    JOINER TEXT,
    CLEANED_SPEC TEXT
    );'''
    return query

try:
    sqlite3_connection = sqlite3.connect(sqlite3_conn_path)
    cursor = sqlite3_connection.cursor()
    print('Successfully connected to the database')


    #df = pd.read_csv('S:\Supply_Chain\Analytics\Inventory Allocation Maximization\MVP Data\inventory.csv')
    df = pd.read_sql_query(mvp_inventory_query, sqlite3_connection)

    #this will need to be updated to pull the data from the mvp_product table each product should get an allowed spec list
    allowed_specs = ['3002A', '3007DRYGUM', '3007FO', '3013A', '3025S', '3035A', '9999E', '9999A', '9999E']

    df['spec_listed'] = df['SPEC'].str.split('-')

    #this function removes the bad specs basically
    def filter_specs(df, column_name, allowed_specs):
        df[column_name] = df[column_name].apply(lambda x: [val for val in x if val in allowed_specs])
        return df

    df = filter_specs(df, 'spec_listed', allowed_specs)

    #this recombines the spec list into the same format as they started in (SPEC-SPEC-SPEC-etc)
    df['CLEANED_SPEC'] = df['spec_listed'].apply(lambda x: '-'.join(map(str,x)))

    df = df.drop('spec_listed', axis=1)

    try:
        cursor.execute(drop_table(mvp_inventory_table))
        cursor.execute(create_mvp_inventory(mvp_inventory_table))
        df.to_sql(f'{mvp_inventory_table}', sqlite3_connection, if_exists='append', index=False)
    except Exception as ex:
        print(ex)

    #df.to_csv('spec_test.csv',index=False)

except sqlite3.Error as error:
	print(error)

finally:
	if sqlite3_connection:
		sqlite3_connection.close()
		print("Connection to power_bi_data is closed")