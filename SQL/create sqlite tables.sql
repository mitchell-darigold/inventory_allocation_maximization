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