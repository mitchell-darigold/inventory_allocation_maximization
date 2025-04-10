.mode csv
.headers on
.once 'S:\Supply_Chain\Analytics\Inventory Allocation Maximization\MVP Data\input tables\Production_Policies.csv'

select mba.site
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
from mvp_bom_assigments mba
;