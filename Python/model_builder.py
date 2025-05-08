import pandas as pd
import sqlite3
from datetime import datetime, timedelta
from datetime import date
from dateutil.parser import parse


#Ask user for date to use with the loading.  It needs to be the date the data was pulled on

while True:
    date_string = input("Enter a date in YYYY-MM-DD format: ")
    try:
        date_object = datetime.strptime(date_string, "%Y-%m-%d").date()
        break
    except ValueError:
       print("Invalid date format. Please use YYYY-MM-DD.")
print("You entered:", date_object)

#date_string = '2025-05-01'
start_date = parse(date_string)

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
iam_bom_table = 'iam_bom'
iam_bom_assignment_table = 'iam_bom_assignments'
iam_periods_table = 'iam_periods'

###############sql queries#####################

#create tables

def drop_table(table_name):
    table = table_name
    query = f'drop table if exists {table}'
    return query

def create_iam_periods(table_name):
    table = table_name
    query = f'''create table {table}(
    PERIOD_NUMBER INTEGER,
    DATE_FORMATTED TEXT
    );'''
    return query

#queries

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
        --max age for a product is 120 as defined in the iam_age_joiner table
        select mi.*
        ,aj.age as age_joiner

        from iam_inventory mi

        left join iam_age_joiner aj
        on mi.joiner=aj.joiner

        where 1=1
        and aj.age >= mi.age
    ) p
    ;'''

coupa_products = '''select 
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
        --max age for a product is 120 as defined in the iam_age_joiner table
        select mi.*
        ,aj.age as age_joiner

        from iam_inventory mi

        left join iam_age_joiner aj
        on mi.joiner=aj.joiner

        where 1=1
        and aj.age >= mi.age
    ) p
    ;'''

create_table_iam_bom = '''create table iam_bom as 
select
z.Product
,z.BOM
from (
    select distinct
    m.item_number || "_" || m.production_plant || "_" ||  m.grade || "_" ||  m.cleaned_spec || "_" ||  m.age_minus_one || "D" as product
    ,"BOM_" || m.model_name as BOM
    --,m.whs_code
    from (
        select *
        ,cast(age as int) - 1 as age_minus_one
        from iam_distinct_whs_products
    ) m
) z
;'''

coupa_boms = '''select 
BOM as Name
,product as Product
,'Component' as Type
,'1' as Quantity
from iam_bom;'''

create_table_iam_bom_assignments = '''create table iam_bom_assignments as
    select
    'REAL_PERIODS' as Period
    ,z.whs_code as Site
    ,z.product as Product
    ,z.BOM
    ,'' as 'Assignment Policy'
    ,'' as 'Policy Parameter'
    ,'' as 'Unit BOM Cost'
    ,'include' as 'Status'
    ,'' as Notes
    from (
        select
        m.item_number || "_" || m.production_plant || "_" ||  m.grade || "_" ||  m.cleaned_spec || "_" ||  m.age || "D" as product
        ,"BOM_" || m.model_name as BOM
        ,m.whs_code
        from (
            select *
            ,cast(age as int) - 1 as age_minus_one
            from iam_distinct_whs_products
        ) m
    ) z
    ;'''

coupa_bom_assignments = '''select * from iam_bom_assigments;'''

coupa_production_policies = '''select mba.site
,mba.product
,'' as 'Production Policy'
,'' as 'Unit Production Cost'
,'' as 'Fixed Order Cost'
,'1.1' as 'Fixed Order Time'
,'' as 'CO2'
,'' as 'Minimum Order Quantity'
,'' as 'Production Frequency'
,'include' as 'Status'
,'' as 'Notes'
from iam_bom_assigments mba

union

--these are all the production constraints products we need to add
--these items dont have a bom_assignment because they are whats in inventory at the start of the model
select distinct z.whs_code as Site
,z.item_number || "_" || z.production_plant || "_" ||  z.grade || "_" ||  z.cleaned_spec || "_" ||  z.age || "D" as Product
,'' as 'Production Policy'
,'' as 'Unit Production Cost'
,'' as 'Fixed Order Cost'
,'1.1' as 'Fixed Order Time'
,'' as 'CO2'
,'' as 'Minimum Order Quantity'
,'' as 'Production Frequency'
,'include' as 'Status'
,'' as 'Notes'
from (
    select
    mi.item_number
    ,mi.production_plant
    ,mi.grade
    ,mi.cleaned_spec 
    ,cast(cast(mi.age as real) as integer) age
    ,mi.total_pallets
    ,mi.whs_code
    from iam_inventory mi
) z
;'''

coupa_transportation_policies = '''select distinct mi.whs_code as Source
,'(ALL_Customers)' as 'Destination'
,'' as 'Ship To'
,'(ALL_Products)' as 'Product'
,'' as 'CollectionBasisProductName'
,'' as 'Mode'
,'' as 'Transportation Policy'
,'' as 'Policy Parameter'
,'' as 'Review Period'
,'' as 'Mode Function'
,'' as 'Shipment Period'
,'' as 'Replenishment Frequency'
,'' as 'Transport Time'
,'' as 'Distance'
,'' as 'Asset'
,'' as 'Variable Transportation Cost'
,'' as 'Variable Cost Basis'
,'' as 'Fixed Shipment Cost'
,'' as 'Shipment Size'
,'' as 'Shipment Rule'
,'' as 'Product Class'
,'' as 'Duty Rate'
,'' as 'Discount Rate'
,'' as 'Minimum Charge'
,'' as 'Fuel Surcharge'
,'' as 'Fuel Surcharge Basis'
,'' as 'CO2'
,'' as 'CO2 Basis'
,'' as 'Require Return Trip'
,'' as 'Return Trip Time'
,'' as 'Return Trip Cost'
,'' as 'Load Resource'
,'' as 'Unload Resource'
,'' as 'Auto-release Load Resource'
,'' as 'Auto-release Unload Resource'
,'' as 'Fixed Load Cost'
,'' as 'Unit Load Cost'
,'' as 'Fixed Load Time'
,'' as 'Unit Load Time'
,'' as 'Fixed Unload Cost'
,'' as 'Unit Unload Cost'
,'' as 'Fixed Unload Time'
,'' as 'Unit Unload Time'
,'' as 'Requirement Type'
,'' as 'Requirement Value'
,'' as 'Requirement Period'
,'' as 'Lane Consignment Queue Basis'
,'' as 'Mode Consignment Queue Basis'
,'' as 'Chance For Ambush'
,'' as 'Ambush Delay'
,'' as 'Ambush Damage'
,'' as 'Reorder When Destroyed'
,'' as 'Unit Delay Factor'
,'' as 'Delay Factor'
,'' as 'Minimum Replenishment Quantity'
,'' as 'Minimum Service Time'
,'' as 'Maximum Service Time'
,'' as 'Guaranteed Service Time'
,'' as 'Responsible Party'
,'' as 'Lane Name'
,'' as 'Path Name'
,'include' as 'Status'
,'' as 'Notes'
,'' as 'Load Schedule Rule'
,'' as 'Unload Schedule Rule'
,'' as 'Asset Departure Schedule Rule'
from iam_inventory mi
;'''

coupa_inventory_policies = '''select
m.whs_code as 'Site'
,m.product_model_name as 'Product'
,'' as 'Initial Inventory'
--,m.total_pallets as 'Initial Inventory'
,'' as 'Inventory Policy'
,'' as 'Reorder Point'
,'' as 'Reorder/Order Up To Qty'
,'' as 'Review Period'
,'' as 'Forecast Name'
,'' as 'Forecast Aggregation Period'
,'' as 'Forecast Disaggregation Pattern'
,'' as 'DOS Window'
,'' as 'DOS Planning Lead Time'
,'' as 'Unit Inbound Cost'
,'' as 'Fixed Inbound Shipment Cost'
,'' as 'Unit Outbound Cost'
,'' as 'Fixed Outbound Shipment Cost'
,'' as 'Unit Storage Cost'
,m.unit_disposal_cost as 'Unit Disposal Cost'
,'' as 'Unit Consignment Cost'
,'' as 'Fixed Consignment Cost'
,'' as 'Inv Carrying Cost%'
,'' as 'Stocking Site'
,'' as 'Product Inventory Value'
,'' as 'Product Taxable Value'
,'' as 'Inventory Turns'
,'' as 'Unhealthy Turn Percent'
,'' as 'Fixed Excess Stock'
,'' as 'Excess Move Percent'
,'' as 'Inbound Capacity'
,'' as 'Outbound Capacity'
,'' as 'Constraint Period'
,'' as 'Maximum Inventory'
,'' as 'Minimum Inventory'
,'' as 'Safety Stock'
,'' as 'Minimum Safety Stock'
,'' as 'Maximum Safety Stock'
,'' as 'Allow Partial Fill'
,'' as 'Service Requirement'
,'' as 'Minimum Service Time'
,'' as 'Maximum Service Time'
,'' as 'Minimum Dwell Time'
,'' as 'Maximum Dwell Time'
,'' as 'Unit Delay Factor'
,'' as 'Fixed Order Delay Factor'
,'' as 'Service Type'
,'include' as 'Status'
,'' as 'Notes'

--this subselect just seperates creating all the column names for the model and the actual SQL code below
from (
    select
    p.product_model_name
    --,x.total_pallets
    --after troubleshooting using the initial inventory column in the inventory policies is not working as expected.  I had to make production constraints to produce the initial inventory for the model to "see" it.  Im leaving this here for posterity
    ,p.whs_code
    ,p.unit_disposal_cost

    from (
        select
        n.product_model_name
        ,n.whs_code
        ,row_number() over (partition by n.product_model_name_no_age, n.whs_code order by n.age_joiner desc) * 50 unit_disposal_cost
        from (
        --this select creates one row for every product for every whs
            select
            k.product_model_name
            ,k.product_model_name_no_age
            ,k.age_joiner
            ,sj.whs_code
            from (
                --this select grabs the distinct list of products with some dimensions to allow for the row_num creation later
                select distinct
                mi.item_number || "_" || mi.production_plant || "_" ||  mi.grade || "_" ||  mi.cleaned_spec || "_" ||  aj.age || "D" as product_model_name
                ,mi.item_number || "_" || mi.production_plant || "_" ||  mi.grade || "_" ||  mi.cleaned_spec as product_model_name_no_age
                ,cast(cast(aj.age as real) as integer) as age_joiner
                ,'1' as site_joiner

                from iam_inventory mi

                left join iam_age_joiner aj
                on mi.joiner=aj.joiner

                where 1=1
                and aj.age >= mi.age
            ) k
                --this left join allows me to duplicate the distinct list of products as many times as there are unique whs in the iam_inventory table
                left join (
                    select distinct whs_code
                    ,'1' as site_joiner
                    from iam_inventory
                ) sj
                on k.site_joiner=sj.site_joiner
        ) n
    ) p
    
    left join (
        --this left join grabs data out of the iam_inventory table to join it to the list of distinct products.  Whereever there is a product actually at a warehouse it will get attached to the version in the distinct list of products along with the total pallets and whs code
        select z.item_number || "_" || z.production_plant || "_" ||  z.grade || "_" ||  z.cleaned_spec || "_" ||  z.age || "D" as product_model_name
        ,z.whs_code
        ,sum(z.total_pallets) as total_pallets
        from (
            select
            mi.item_number
            ,mi.production_plant
            ,mi.grade
            ,mi.cleaned_spec 
            ,cast(cast(mi.age as real) as integer) as age
            ,mi.total_pallets
            ,mi.whs_code
            --,cast(cast(mi.age as real) as integer)
            from iam_inventory mi
        ) z
        group by
        z.item_number || "_" || z.production_plant || "_" ||  z.grade || "_" ||  z.cleaned_spec || "_" ||  z.age || "D"
        ,z.whs_code
    ) x
    on p.product_model_name=x.product_model_name

) m
;'''

coupa_sites = '''select distinct whs_code as Name from iam_inventory;'''

coupa_customers = '''select distinct order_number as Name from iam_orders;'''

spec_enumerate = '''select * from iam_distinct_inventory_products'''

coupa_group_members = '''select distinct g.group_name as 'Group'
,dp.model_name as 'Member'

from (
    --the stuff in this from statement creates the group.  We pull the groups necessary from the orders data.  Then we will fill the groups with inventory data above.  AKA the orders tell us what groups we need and the inventory is what we can actually put into the groups
    --keeping the subselect seperate from the joins that happen above for clarity
    select mo.item_number || "_" || mo.approved_plant_concat || "_" || mo.grade || "_" || mo.spec as group_name
    ,mo.item_number
    ,mo.approved_plant_1
    ,mo.approved_plant_2
    ,mo.approved_plant_3
    ,mo.grade
    ,mo.spec as spec

    from (
        --just doing the case when in a subselect for clarity
        select item_number
        ,approved_plant_1
        ,approved_plant_2
        ,approved_plant_3
        ,case when approved_plant_2 = '' then approved_plant_1
            when approved_plant_3 = '' then approved_plant_1 || "-" || approved_plant_2
            else approved_plant_1
            end as approved_plant_concat
        ,grade
        ,spec
        from iam_orders
    ) mo
) g

left join (
--    select model_name
--    ,age
--    ,production_plant
--    ,cast(grade as INT) as grade
--    ,cleaned_spec
--    ,item_number
--    ,spec1
--    ,spec2
--    ,spec3
--    ,spec4
--    ,spec5
--    ,spec6
--    ,spec7
    select *
    from iam_distinct_inventory_products
 ) dp
on g.item_number = dp.item_number
and (g.grade = dp.grade or g.grade = dp.grade+1)
and (g.approved_plant_1 = dp.production_plant or g.approved_plant_2 = dp.production_plant or g.approved_plant_3 = dp.production_plant)
--this part is tricky and requires manual intervention.  I wont know how many specs will exist in the iam_distinct_inventory_products table unless I look manually
and (g.spec = dp.spec1 or g.spec = dp.spec2 or g.spec = dp.spec3 or g.spec = dp.spec4 or g.spec = dp.spec5 or g.spec = dp.spec6 or g.spec = dp.spec7 or g.spec = dp.spec8  or g.spec = dp.spec9)

--i need to include the period group for the production constraint trickery

union all

select
'REAL_PERIODS'  as 'Group'
,period_number as 'Member'
from iam_periods
where period_number <> 0
;'''


coupa_groups = '''select distinct g.group_name as 'Group Name'
,'Product' as 'Group Type'

from (
    --the stuff in this from statement creates the group.  We pull the groups necessary from the orders data.  Then we will fill the groups with inventory data above.  AKA the orders tell us what groups we need and the inventory is what we can actually put into the groups
    --keeping the subselect seperate from the joins that happen above for clarity
    select mo.item_number || "_" || mo.approved_plant_concat || "_" || mo.grade || "_" || mo.spec as group_name
    ,mo.item_number
    ,mo.approved_plant_1
    ,mo.approved_plant_2
    ,mo.approved_plant_3
    ,mo.grade
    ,mo.spec as spec

    from (
        --just doing the case when in a subselect for clarity
        select item_number
        ,approved_plant_1
        ,approved_plant_2
        ,approved_plant_3
        ,case when approved_plant_2 = '' then approved_plant_1
            when approved_plant_3 = '' then approved_plant_1 || "-" || approved_plant_2
            else approved_plant_1
            end as approved_plant_concat
        ,grade
        ,spec
        from iam_orders
    ) mo
) g

left join (
    select model_name
    ,age
    ,production_plant
    ,cast(grade as INT) as grade
    ,cleaned_spec
    ,item_number
    ,spec1
    ,spec2
    ,spec3
    ,spec4
    ,spec5
    ,spec6
    ,spec7
    from iam_distinct_inventory_products
 ) dp
on g.item_number = dp.item_number
and (g.grade = dp.grade or g.grade = dp.grade+1)
and (g.approved_plant_1 = dp.production_plant or g.approved_plant_2 = dp.production_plant or g.approved_plant_3 = dp.production_plant)
--this part is tricky and requires manual intervention.  I wont know how many specs will exist in the iam_distinct_inventory_products table unless I look manually
and (g.spec = dp.spec1 or g.spec = dp.spec2 or g.spec = dp.spec3 or g.spec = dp.spec4 or g.spec = dp.spec5 or g.spec = dp.spec6 or g.spec = dp.spec7)

--i need to include the period group for the production constraint trickery

union all

select
'REAL_PERIODS' as 'Group Name'
,'Periods' as 'Group Type'
from iam_periods
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
    cursor.execute(drop_table(iam_periods_table))

    num_rows = 121
    start_date = start_date - timedelta(days=1)
    #date_list = [start_date + datetime.timedelta(days=i) for i in range(num_rows)]
    date_list = [start_date + timedelta(days=i) for i in range(num_rows)]
    number_list = list(range(0, num_rows))
    df = pd.DataFrame({'DATE_FORMATTED': date_list, 'PERIOD_NUMBER': number_list})

    cursor.execute(create_iam_periods(iam_periods_table))
    df.to_sql(f'{iam_periods_table}', sqlite3_connection, if_exists='append', index=False)
except Exception as ex:
    print(ex)

try:
    cursor.execute(drop_table(iam_distinct_inventory_products_table))
    cursor.execute(create_table_iam_distinct_inventory_products)

    df = pd.read_sql_query(spec_enumerate, sqlite3_connection)
    df['spec_listed'] = df['CLEANED_SPEC'].str.split('-')
    df = pd.concat([df, pd.DataFrame(df['spec_listed'].to_list(), index=df.index)], axis=1)
    df = df.rename(columns={0: 'spec1', 1: 'spec2', 2: 'spec3',3: 'spec4', 4: 'spec5', 5: 'spec6', 6: 'spec7', 7: 'spec8', 8: 'spec9', 9: 'spec10', 10: 'spec11'})
    df = df.drop('spec_listed', axis=1)
    
    cursor.execute(drop_table(iam_distinct_inventory_products_table))
    df.to_sql(iam_distinct_inventory_products_table, sqlite3_connection, if_exists='replace', index=False)

except Exception as ex:
    print(ex)

try:
    cursor.execute(drop_table(iam_distinct_whs_products_table))
    cursor.execute(create_table_iam_distinct_whs_products)
except Exception as ex:
    print(ex)

try:
    cursor.execute(drop_table(iam_bom_table))
    cursor.execute(create_table_iam_bom)
except Exception as ex:
    print(ex)

try:
    cursor.execute(drop_table(iam_bom_assignment_table))
    cursor.execute(create_table_iam_bom_assignments)
except Exception as ex:
    print(ex)

#Commit changes

sqlite3_connection.commit()


#Create meta tables




#Output coupa model tables
products_table_df = pd.read_sql_query(coupa_products, sqlite3_connection)
boms_table_df = pd.read_sql_query(coupa_boms, sqlite3_connection)
bom_assignment_table_df = pd.read_sql_query(coupa_bom_assignments, sqlite3_connection)
production_policies_table_df = pd.read_sql_query(coupa_production_policies, sqlite3_connection)
transportation_policies_table_df = pd.read_sql_query(coupa_transportation_policies, sqlite3_connection)
inventory_policies_table_df = pd.read_sql_query(coupa_inventory_policies, sqlite3_connection)
sites_table_df = pd.read_sql_query(coupa_sites, sqlite3_connection)
customers_table_df = pd.read_sql_query(coupa_customers, sqlite3_connection)
group_members_table_df = pd.read_sql_query(coupa_group_members, sqlite3_connection)
group_table_df = pd.read_sql_query(coupa_groups, sqlite3_connection)





#Create a single excel file with each relevant sheet
#I need to come back through and update the sheet names correctly
with pd.ExcelWriter('S:\\Supply_Chain\\Analytics\\Inventory Allocation Maximization\\Master\\model.xlsx') as writer:
    products_table_df.to_excel(writer, sheet_name='Products', index=False)
    boms_table_df.to_excel(writer, sheet_name='BOM', index=False)
    bom_assignment_table_df.to_excel(writer, sheet_name='BOM_Assignments', index=False)
    production_policies_table_df.to_excel(writer, sheet_name='Production_Policies', index=False)
    transportation_policies_table_df.to_excel(writer, sheet_name='Transportation_Policies', index=False)
    inventory_policies_table_df.to_excel(writer, sheet_name='Inventory_Policies', index=False)
    sites_table_df.to_excel(writer, sheet_name='Sites', index=False)
    customers_table_df.to_excel(writer, sheet_name='Customers', index=False)
    group_members_table_df.to_excel(writer, sheet_name='Group Members', index=False)
    group_table_df.to_excel(writer, sheet_name='Groups', index=False)

#Close the sqlite connection
sqlite3_connection.close()
print("Connection to power_bi_data is closed")