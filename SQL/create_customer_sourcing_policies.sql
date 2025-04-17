.mode csv
.headers on
.once 'S:\Supply_Chain\Analytics\Inventory Allocation Maximization\MVP Data\input tables\customer_sourcing_policies_simplified.csv'

select distinct 
'(ALL_Customers)' as Customer
,'(ALL_Products)' as Product
,whs_code as Source
from mvp_inventory
;



.mode csv
.headers on
.once 'S:\Supply_Chain\Analytics\Inventory Allocation Maximization\MVP Data\input tables\customer_sourcing_policies.csv'

select
'(ALL_Customers)' as Customer
,n.product_model_name as Product
,n.whs_code as Source
,row_number() over (partition by n.product_model_name_no_age, n.whs_code order by n.age_joiner desc) * 50 'Unit Sourcing Cost'
from (
--this select creates one row for every product for every whs
    select
    k.product_model_name
    ,k.product_model_name_no_age
    ,k.age_joiner
    ,sj.whs_code
    from (
        --this select grabs the distinct list of products with some dimensions to allow for the row_num creation later
        select distinct
        mi.item_number || "_" || mi.production_plant || "_" ||  mi.grade || "_" ||  mi.cleaned_spec || "_" ||  aj.age || "D" as product_model_name
        ,mi.item_number || "_" || mi.production_plant || "_" ||  mi.grade || "_" ||  mi.cleaned_spec as product_model_name_no_age
        ,cast(cast(aj.age as real) as integer) as age_joiner
        ,'1' as site_joiner

        from mvp_inventory mi

        left join mvp_age_joiner aj
        on mi.joiner=aj.joiner

        where 1=1
        and aj.age >= mi.age
    ) k
        --this left join allows me to duplicate the distinct list of products as many times as there are unique whs in the mvp_inventory table
        left join (
            select distinct whs_code
            ,'1' as site_joiner
            from mvp_inventory
        ) sj
        on k.site_joiner=sj.site_joiner
) n
;