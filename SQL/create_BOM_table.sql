drop table if exists mvp_bom;

create table mvp_bom as 
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
        from mvp_distinct_whs_products
    ) m
) z
;

.mode csv
.headers on
.once 'S:\Supply_Chain\Analytics\Inventory Allocation Maximization\MVP Data\input tables\BOMs.csv'

select 
BOM as Name
,product as Product
,'Component' as Type
,'1' as Quantity
from mvp_bom;
