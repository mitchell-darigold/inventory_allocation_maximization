.mode csv
.headers on
.once 'S:\Supply_Chain\Analytics\Inventory Allocation Maximization\MVP Data\input tables\Production Constraints.csv'

select
'0' as 'Period'
,'' as 'CollectionBasisPeriodName'
,m.whs_code as 'Site'
,'' as 'CollectionBasisSiteName'
,m.product_model_name as 'Product'
,'' as 'CollectionBasisProductName'
,'' as 'BOM'
,'' as 'CollectionBasisBOMName'
,'Fixed' as 'Constraint Type'
,case when m.total_pallets is null then 0 else m.total_pallets end as 'Constraint Value'
,'' as 'Constraint Period'
,'Include' as 'Status'
,'' as 'Notes'

--this subselect just seperates creating all the column names for the model and the actual SQL code below
from (
    select
    p.product_model_name
    ,x.total_pallets
    --after troubleshooting using the initial inventory column in the inventory policies is not working as expected.  I had to make production constraints to produce the initial inventory for the model to "see" it.  Im leaving this here for posterity
    ,p.whs_code
    ,p.unit_disposal_cost

    from (
        select
        n.product_model_name
        ,n.whs_code
        ,row_number() over (partition by n.product_model_name_no_age, n.whs_code order by n.age_joiner desc) * 50 unit_disposal_cost
        from (
        --this select creates one row for every product for every whs
            select
            k.product_model_name
            ,k.product_model_name_no_age
            ,k.age_joiner
            ----,sj.whs_code
            ,k.whs_code
            from (
                --this select grabs the distinct list of products with some dimensions to allow for the row_num creation later
                select distinct
                mi.item_number || "_" || mi.production_plant || "_" ||  mi.grade || "_" ||  mi.cleaned_spec || "_" ||  aj.age || "D" as product_model_name
                ,mi.item_number || "_" || mi.production_plant || "_" ||  mi.grade || "_" ||  mi.cleaned_spec as product_model_name_no_age
                ,cast(cast(aj.age as real) as integer) as age_joiner
                --,'1' as site_joiner
                ,mi.whs_code

                from mvp_inventory mi

                left join mvp_age_joiner aj
                on mi.joiner=aj.joiner

                where 1=1
                and aj.age >= mi.age
            ) k
                --this left join allows me to duplicate the distinct list of products as many times as there are unique whs in the mvp_inventory table
                ----left join (
                ----    select distinct whs_code
                ----    ,'1' as site_joiner
                ----    from mvp_inventory
                ----) sj
                ---on k.site_joiner=sj.site_joiner
        ) n
    ) p
    
    left join (
        --this left join grabs data out of the mvp_inventory table to join it to the list of distinct products.  Whereever there is a product actually at a warehouse it will get attached to the version in the distinct list of products along with the total pallets and whs code
        select z.item_number || "_" || z.production_plant || "_" ||  z.grade || "_" ||  z.cleaned_spec || "_" ||  z.age || "D" as product_model_name
        ,z.whs_code
        ,sum(z.total_pallets) as total_pallets
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
        z.item_number || "_" || z.production_plant || "_" ||  z.grade || "_" ||  z.cleaned_spec || "_" ||  z.age || "D"
        ,z.whs_code
    ) x
    on p.product_model_name=x.product_model_name
    and p.whs_code = x.whs_code

) m
;