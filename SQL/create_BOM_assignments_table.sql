--i need a table thats the same as mvp_distinct_inventory_products (like I made in the create_products_tables) but with a whs_code column.  I am creating that below
create table mvp_distinct_whs_products as
    select distinct
    p.item_number || "_" || p.production_plant || "_" ||  p.grade || "_" ||  p.cleaned_spec || "_" ||  p.age_joiner || "D" as MODEL_NAME
    ,p.age_joiner as age
    ,p.production_plant
    ,p.grade
    ,p.cleaned_spec
    ,p.item_number
    ,p.whs_code

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

create table mvp_bom_assigments as
    select
    '(ALL_Periods)' as Period
    ,z.whs_code as Site
    ,z.model_name as Product
    ,z.BOM
    ,'' as 'Assignment Policy'
    ,'' as 'Policy Parameter'
    ,'' as 'Unit BOM Cost'
    ,'include' as 'Status'
    ,'' as Notes
    from (
        select mdip.model_name
        ,mdip.whs_code
        ,lag(mdip.model_name || ' aging',-1,'0') over (partition by mdip.item_number || "_" || mdip.production_plant || "_" ||  mdip.grade || "_" ||  mdip.cleaned_spec order by mdip.item_number || "_" || mdip.production_plant || "_" ||  mdip.grade || "_" ||  mdip.cleaned_spec, cast(mdip.age as int) desc) as BOM
        from mvp_distinct_whs_products mdip
        order by mdip.item_number || "_" || mdip.production_plant || "_" ||  mdip.grade || "_" ||  mdip.cleaned_spec, cast(mdip.age as int) desc
    ) z
    where BOM <> 0
    and BOM <> '0'
    ;

.mode csv
.headers on
.once 'S:\Supply_Chain\Analytics\Inventory Allocation Maximization\MVP Data\input tables\BOM_Assignments.csv'
select * from mvp_bom_assigments;