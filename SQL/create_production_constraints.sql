drop table if exists mvp_production_constraints;

create table mvp_production_constraints as
select '1' as Period
,z.whs_code as Site
,z.item_number || "_" || z.production_plant || "_" ||  z.grade || "_" ||  z.cleaned_spec || "_" ||  z.age || "D" as Product
,'Min' as 'Constraint Type'
,sum(z.total_pallets) as 'Constraint Value'
,'Include' as Status
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
    from mvp_inventory mi
) z
group by
z.whs_code
,z.item_number || "_" || z.production_plant || "_" ||  z.grade || "_" ||  z.cleaned_spec || "_" ||  z.age || "D"
having sum(z.total_pallets) > 0
;

.mode csv
.headers on
.once 'S:\Supply_Chain\Analytics\Inventory Allocation Maximization\MVP Data\input tables\Production Constraints.csv'
select * from mvp_production_constraints;