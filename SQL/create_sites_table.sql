.mode csv
.headers on
.once 'S:\Supply_Chain\Analytics\Inventory Allocation Maximization\MVP Data\input tables\Sites.csv'


select distinct whs_code as Name
from mvp_inventory;