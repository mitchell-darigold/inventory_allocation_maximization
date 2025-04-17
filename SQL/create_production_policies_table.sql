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
    from mvp_inventory mi
) z

;