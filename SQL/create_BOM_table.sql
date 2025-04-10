.mode csv
.headers on
.once 'S:\Supply_Chain\Analytics\Inventory Allocation Maximization\MVP Data\input tables\BOM.csv'



create table bom as


.mode csv
.headers on
.once bom.csv


select
z.bom_name as Name
,z.model_product_name as Product
,lag(z.Type,1,0) over (partition by z.model_product_name_no_age order by z.age asc) as Type
,z.Quantity
,z.Notes
,z.model_product_name_no_age
,z.age
from (
    select mdip.model_name || '_aging' as bom_name
    ,mdip.model_name as model_product_name
    ,mdip.item_number || "_" || mdip.production_plant || "_" ||  mdip.grade || "_" ||  mdip.cleaned_spec as model_product_name_no_age
    ,mdip.age
    ,'End Product' as Type
    ,'1' as Quantity
    ,'' as Notes
    from mvp_distinct_inventory_products mdip
    union all
    select mdip.model_name || '_aging'
    ,mdip.model_name
    ,mdip.item_number || "_" || mdip.production_plant || "_" ||  mdip.grade || "_" ||  mdip.cleaned_spec
    ,mdip.age
    ,'Component'
    ,'1' as Quantity
    ,'' as Notes
    from mvp_distinct_inventory_products mdip
) z
;

.mode csv
.headers on
.once bom.csv
select * from bom;