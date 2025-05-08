drop table if exists mvp_distinct_inventory_products;

create table mvp_distinct_inventory_products as
    select distinct
    p.item_number || "_" || p.production_plant || "_" ||  p.grade || "_" ||  p.cleaned_spec || "_" ||  p.age_joiner || "D" as MODEL_NAME
    ,p.age_joiner as age
    ,p.production_plant
    ,p.grade
    ,p.cleaned_spec
    ,p.item_number

    from (
        --this section joins the raw inventory data with a joiner table to produce 1 row for every possible age for each sku_prod facility_grade_spec combination at or above the current age of the product in inventory
        --max age for a product is 120 as defined in the mvp_age_joiner table
        select mi.*
        ,aj.age as age_joiner

        from mvp_inventory mi

        left join mvp_age_joiner aj
        on mi.joiner=aj.joiner

        where 1=1
        and aj.age >= mi.age
    ) p
    ;

.mode csv
.headers on
.once 'S:\Supply_Chain\Analytics\Inventory Allocation Maximization\MVP Data\input tables\Products.csv'

select 
a.model_name as Name
,'' as 'Unit Value'
,'' as 'Unit Price'
,'' as 'Unit Weight'
,'' as 'Unit Volume'
,'' as 'Type'
,'' as 'Class'
,'1' as 'Shelf Life'
,'' as 'Lead Time Cost/Day/Unit'
,'' as 'Maximum Lead Time x Flow'
,'' as 'Start Date'
,'' as 'End Date'
,'' as 'Description'
,'include' as 'Status'
,'' as 'Notes'
,'' as 'Custom 1'
,'' as 'Custom 2'
,'' as 'Custom 3'
,'' as 'Custom 4'
,'' as 'Temperature Class'
,'' as 'Hazardous Goods'
,'' as 'Maximum Risk'

from (
    select distinct model_name
    from mvp_distinct_inventory_products

    union

    select distinct
    m.item_number || "_" || m.production_plant || "_" ||  m.grade || "_" ||  m.cleaned_spec || "_" ||  m.age_minus_one || 'D'

    from (
        select *
        ,cast(age as int) - 1 as age_minus_one
        from mvp_distinct_whs_products
    ) m
) a
;