drop table if exists mvp_inventory;

create table mvp_inventory(
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
    );

.mode csv
.headers off

.import 'S:\Supply_Chain\Analytics\Inventory Allocation Maximization\MVP Data\no headers\inventory.csv' mvp_inventory


drop table if exists mvp_items;

create table mvp_items(
    GRADE TEXT,
    ITEM_NUMBER TEXT,
    PALLET_WEIGHT REAL,
    MAX_AGE REAL,
    SPEC TEXT
    );

.mode csv
.headers off

.import 'S:\Supply_Chain\Analytics\Inventory Allocation Maximization\MVP Data\no headers\item.csv' mvp_items

--i do a lot of faffing with the mvp orders tables.  Basically I need to remove rows that are for orders prior to the date I pulled the data AND orders greater than 120 days after the date of data pull.
--I have to convert the dates to SQLite dates which is the main part of the faff
drop table if exists mvp_orders;

create table mvp_orders(
    APPROVED_PLANT_1 TEXT,
    APPROVED_PLANT_2 TEXT,
    APPROVED_PLANT_3 TEXT,
    DEMAND_TYPE TEXT,
    GRADE TEXT,
    MAX_AGE REAL,
    ITEM_NUMBER TEXT,
    ORDER_NUMBER TEXT,
    ORDERED_WEIGHT REAL,
    ORDERED_PALLETS REAL,
    SHIP_DATE TEXT,
    SHIPPING_WHS_CODE TEXT,
    SPEC TEXT
    );

.mode csv
.headers off

.import 'S:\Supply_Chain\Analytics\Inventory Allocation Maximization\MVP Data\no headers\orders.csv' mvp_orders


create table mvp_orders_formatted as
    select
    APPROVED_PLANT_1,
    APPROVED_PLANT_2,
    APPROVED_PLANT_3,
    DEMAND_TYPE,
    GRADE,
    MAX_AGE,
    ITEM_NUMBER,
    ORDER_NUMBER,
    ORDERED_WEIGHT,
    ORDERED_PALLETS,
    SHIP_DATE,
    SHIPPING_WHS_CODE,
    SPEC,
    SHIP_DATE_FORMATTED
    from (
        select 
        *
        ,year || '-' || month || '-' || day as SHIP_DATE_FORMATTED
        from (
            select
            *
            ,substr(SHIP_DATE, -4) year
            ,case when substr(SHIP_DATE, instr(SHIP_DATE, '/'), -2) not in ('10', '11', '12')
                then '0' || substr(SHIP_DATE, instr(SHIP_DATE, '/'), -2)
                else substr(SHIP_DATE, instr(SHIP_DATE, '/'), -2)
                end as month
            ,case when substr(SHIP_DATE, instr(SHIP_DATE, '/')+1, 2) like ('%/')
                then '0' || substr(substr(SHIP_DATE, instr(SHIP_DATE, '/')+1, 2), 1, 1)
                else substr(SHIP_DATE, instr(SHIP_DATE, '/')+1, 2)
                end as day    
            from
            mvp_orders
        )
    );

--the date MUST be the date the data was pulled.  The date the data was pulled has a unique inventory profile at our warehouses.  We must match the orders starting from that day forward to the inventory available that day forward.
--I also need to cut off the orders that are older than 120 days from the day the data was pulled
delete from mvp_orders_formatted where date(ship_date_formatted) < date('now');
delete from mvp_orders_formatted where date(ship_date_formatted) > date('now', '+120 days');

drop table if exists mvp_orders;

alter table mvp_orders_formatted rename to mvp_orders;

drop table if exists mvp_age_joiner;

create table mvp_age_joiner(
    AGE TEXT,
    JOINER TEXT
    );

.mode csv
.headers off

.import 'S:\Supply_Chain\Analytics\Inventory Allocation Maximization\MVP Data\no headers\age_joiner.csv' mvp_age_joiner

drop table if exists mvp_periods;

create table mvp_periods(
    DATES TEXT,
    PERIODS TEXT
    );

.mode csv
.headers off

.import 'S:\Supply_Chain\Analytics\Inventory Allocation Maximization\MVP Data\no headers\periods.csv' mvp_periods