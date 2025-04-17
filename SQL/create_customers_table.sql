.mode csv
.headers on
.once 'S:\Supply_Chain\Analytics\Inventory Allocation Maximization\MVP Data\input tables\Customers.csv'


select distinct order_number as Name
from mvp_orders;