--i have two views in the powerbi file that I extract the data out of
--I save them directly under the naming conventions in the two paths referenced in this file

drop table if exists inventory_history;

create table inventory_history (
W_INSERT_DT TEXT,
WHSE_CODE TEXT,
ITEM_NUMBER TEXT,
PRODUCTION_PLANT TEXT,
QC_GRADE TEXT,
SPEC TEXT,
AGE TEXT,
ALLOCATED_ORDER_NUM TEXT,
AllocationDate TEXT);

.mode csv
.headers off


.import 'S:/Supply_Chain/Analytics/Inventory Allocation Maximization/Comparison/manual planners/inventory history.csv' inventory_history


.headers on
select count(*) from inventory_history;
delete from inventory_history where W_INSERT_DT = 'W_INSERT_DT';
select count(*) from inventory_history;


-----------------

drop table if exists open_orders;

create table open_orders (
Order_Num TEXT,
DemandType TEXT,
ShipDate TEXT,
Item TEXT,
ApprovedPlant1 TEXT,
ApprovedPlant2 TEXT,
ApprovedPlant3 TEXT,
Grade TEXT,
Spec TEXT,
ShippingWarehouseCode TEXT,
SumofOrderedPallets TEXT,
OrderDate TEXT);

.mode csv
.headers off


.import 'S:/Supply_Chain/Analytics/Inventory Allocation Maximization/Comparison/manual planners/open orders.csv' open_orders


.headers on
select count(*) from open_orders;
delete from open_orders where DemandType = 'Demand Type';
select count(*) from open_orders;


------------------
--this creates the complete list of all the allocations the planners made by date
.mode csv
.headers on
.once test.csv

select DATE(substr(ihms.ALLOCATED_DATE_PLUS1,instr(ihms.ALLOCATED_DATE_PLUS1, ' '),-99),'-1 day') as ALLOCATED_DATE
,ih.WHSE_CODE
,ih.ITEM_NUMBER
,ih.PRODUCTION_PLANT
,ih.QC_GRADE
,ih.SPEC
,ih.AGE
,ih.ALLOCATED_ORDER_NUM
from inventory_history ih

inner join (
select WHSE_CODE
,ITEM_NUMBER
,PRODUCTION_PLANT
,QC_GRADE
,SPEC
,ALLOCATED_ORDER_NUM
,min(W_INSERT_DT) as ALLOCATED_DATE_PLUS1
from inventory_history
where ALLOCATED_ORDER_NUM is not null
and ALLOCATED_ORDER_NUM <> ''
group by
WHSE_CODE
,ITEM_NUMBER
,PRODUCTION_PLANT
,QC_GRADE
,SPEC
,ALLOCATED_ORDER_NUM
) ihms
on ih.WHSE_CODE=ihms.WHSE_CODE
and ih.ITEM_NUMBER=ihms.ITEM_NUMBER
and ih.PRODUCTION_PLANT=ihms.PRODUCTION_PLANT
and ih.QC_GRADE=ihms.QC_GRADE
and ih.SPEC=ihms.SPEC
and ih.ALLOCATED_ORDER_NUM=ihms.ALLOCATED_ORDER_NUM
and ih.W_INSERT_DT=ihms.ALLOCATED_DATE_PLUS1
;

-----------
--this gathers all the open order
--im not processing the data at all, i could just be opening the original excel sheet extracted from powerbi
.mode csv
.headers on
.once open_orders.csv

select * from open_orders

-------------------
--the last step is to manually join these together in sqlite.
--I want all the allocations data.  I want to join any of the orders data that matches an allocated order number.  
--But I also want all the open orders that dont join to be included at the bottom with a null value for all the inventory columns.

--i vlookedup all the orders data onto my inventory allocations data
--then I vlookedup the inventory allocations onto my orders to see which ones already had allocations
--I filtered the orders data down to the rows without an allocation for them, aka the still open orders
--I pasted all those order rows at the bottom of the inventory allocations data in the correct spot
--i put 'open orders' in all the inventory related rows
--i created a new column called filter date - if the allocated_date = open orders then ship date else allocated_date.  This lets me filter the rows to may 12th to June 6th.  These are the orders and allocations I am concerned with
--i removed the contract orders from the final dataset as the planners should not be allocating to contracts (ill do the same in the dataset used in the model)