.mode csv
.headers on
.once 'S:\Supply_Chain\Analytics\Inventory Allocation Maximization\MVP Data\input tables\BOMs.csv'

--the query pulls one extra row for each product.  The first age of the product, one of the rows from the union I didnt remove cause its seemed hard and it wont impact the model anyway
select
x.Name
,x.Product
,x.Type
,x.Quantity
,x.Notes
from (
    select 
    --I lag two columns to offset them to get the values where I want them
    lag(z.model_product_name || " aging",1,0) over (partition by z.model_product_name_no_age) as Name
    ,z.model_product_name as Product
    ,lag(z.Type,1,0) over (partition by model_product_name_no_age) as Type
    ,z.Quantity
    ,'' as Notes
    from (
        --gather the data from the distinct inventory products table
        select mdip.model_name as model_product_name
        ,mdip.item_number || "_" || mdip.production_plant || "_" ||  mdip.grade || "_" ||  mdip.cleaned_spec as model_product_name_no_age
        ,cast(mdip.age as int) as age
        ,'End Product' as Type
        ,'1' as Quantity
        from mvp_distinct_inventory_products mdip
        union all
        select mdip.model_name
        ,mdip.item_number || "_" || mdip.production_plant || "_" ||  mdip.grade || "_" ||  mdip.cleaned_spec
        ,cast(mdip.age as int)
        ,'Component'
        ,'1' as Quantity
        from mvp_distinct_inventory_products mdip
        order by mdip.item_number || "_" || mdip.production_plant || "_" ||  mdip.grade || "_" ||  mdip.cleaned_spec, cast(mdip.age as int) desc
    ) z
) x
where 1=1
and x.Name <> '0'
and x.Name <> 0
;

-----------------
--i spent all the time writing the above query but I think its cleaner to follow Alec's advice and just not include the end product.  That will be on the BOM assignment anyway
create table mvp_bom as 
    select x.model_product_name || ' aging' as Name
    ,x.model_product_name as Product
    ,x.Type
    ,x.Quantity
    ,'' as Notes
    --the subselect from just allows me to do the lag an filter out an undeeded row
    from (
        select mdip.model_name as model_product_name
        ,cast(mdip.age as int) as age
        ,'Component' as Type
        ,'1' as Quantity
        --this lag function just allows me to find the row where the product is oldest (120days old), I dont need that row in the BOM table.  Product at 120 days old will not get older so we dont need a bom to age it
        ,lag('1',1,0) over (partition by mdip.item_number || "_" || mdip.production_plant || "_" ||  mdip.grade || "_" ||  mdip.cleaned_spec order by cast(mdip.age as int) desc) as remover
        from mvp_distinct_inventory_products mdip
        order by mdip.item_number || "_" || mdip.production_plant || "_" ||  mdip.grade || "_" ||  mdip.cleaned_spec, cast(mdip.age as int) desc
    ) x
    where x.remover <> 0
    ;

.mode csv
.headers on
.once 'S:\Supply_Chain\Analytics\Inventory Allocation Maximization\MVP Data\input tables\BOMs.csv'
select * from mvp_bom