#there are many steps that need to happen to evolve this further.
#I need to add a section that pulls the allowed_specs list automatically with a SQL query connection
#when there are multiple products how do I tell the computer which list to use against which row

import pandas as pd
import sqlite3

#sqlite3 connection path
sqlite3_db = 'inventory_allocation_maximization.db'
sqlite3_conn_path = 'P:/sqlite3/sqlite-tools/'+sqlite3_db

#f strings
mvp_distinct_inventory_products_table = 'mvp_distinct_inventory_products'

#sql queries:
mvp_distinct_inventory_products_query = 'select * from mvp_distinct_inventory_products'

def drop_table(table_name):
    table = table_name
    query = f'drop table if exists {table}'
    return query

def create_mvp_distinct_inventory_products(table_name):
    table = table_name
    query = f'''create table {table}(
    MODEL_NAME TEXT,
    AGE TEXT,
    PRODUCTION_PLANT TEXT,
    GRADE TEXT,
    CLEANED_SPEC TEXT,
    ITEM_NUMBER TEXT
    );'''
    return query

try:
    sqlite3_connection = sqlite3.connect(sqlite3_conn_path)
    cursor = sqlite3_connection.cursor()
    print('Successfully connected to the database')


    #df = pd.read_csv('S:\Supply_Chain\Analytics\Inventory Allocation Maximization\MVP Data\inventory.csv')
    df = pd.read_sql_query(mvp_distinct_inventory_products_query, sqlite3_connection)

    df['spec_listed'] = df['CLEANED_SPEC'].str.split('-')

    df = pd.concat([df, pd.DataFrame(df['spec_listed'].to_list(), index=df.index)], axis=1)

    df = df.rename(columns={0: 'spec1', 1: 'spec2', 2: 'spec3',3: 'spec4', 4: 'spec5', 5: 'spec6', 6: 'spec7', 7: 'spec8', 8: 'spec9', 9: 'spec10', 10: 'spec11'})
    
    df = df.drop('spec_listed', axis=1)

    try:
        cursor.execute(drop_table(mvp_distinct_inventory_products_table))
#        cursor.execute(create_mvp_distinct_inventory_products(mvp_distinct_inventory_products_table))
        df.to_sql(f'{mvp_distinct_inventory_products_table}', sqlite3_connection, if_exists='replace', index=False)
        sqlite3_connection.commit()
    except Exception as ex:
        print(ex)

except sqlite3.Error as error:
	print(error)

finally:
	if sqlite3_connection:
		sqlite3_connection.close()
		print("Connection to power_bi_data is closed")