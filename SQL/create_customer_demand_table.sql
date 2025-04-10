.mode csv
.headers on
.once create_customer_demand_table.csv

--this selects the relevant columns from the orders and adds in the period from the periods table
--I need to validate that the periods table I created works as expected
----I have =today() in the first row and =today() + 1 in the second.  Will the result be today one week from now if I just reloaded the table one week from now?.  I need today to be whatever day I load the table not right now (4/8/25)
select
mo.order_number
,mo.ordered_pallets
--,the product, which will be a group name
----the product group will get created in a later step and I can join it here.
,mp.periods
from mvp_orders mo

left join mvp_periods mp
on mo.ship_date=mp.dates