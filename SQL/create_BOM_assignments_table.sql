drop table if exists mvp_distinct_whs_products;
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

drop table if exists mvp_bom_assigments;

create table mvp_bom_assigments as
    select
    'REAL_PERIODS' as Period
    ,z.whs_code as Site
    ,z.product as Product
    ,z.BOM
    ,'' as 'Assignment Policy'
    ,'' as 'Policy Parameter'
    ,'' as 'Unit BOM Cost'
    ,'include' as 'Status'
    ,'' as Notes
    from (
        select
        m.item_number || "_" || m.production_plant || "_" ||  m.grade || "_" ||  m.cleaned_spec || "_" ||  m.age || "D" as product
        ,"BOM_" || m.model_name as BOM
        ,m.whs_code
        from (
            select *
            ,cast(age as int) - 1 as age_minus_one
            from mvp_distinct_whs_products
        ) m
    ) z
    ;

.mode csv
.headers on
.once 'S:\Supply_Chain\Analytics\Inventory Allocation Maximization\MVP Data\input tables\BOM_Assignments.csv'
select * from mvp_bom_assigments;