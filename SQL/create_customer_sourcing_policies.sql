.mode csv
.headers on
.once 'S:\Supply_Chain\Analytics\Inventory Allocation Maximization\MVP Data\input tables\customer_sourcing_policies.csv'

select
'(ALL_Customers)' as Customer
,n.model_name as Product
,n.whs_code as Source
,row_number() over (partition by n.product_model_name_no_age, n.whs_code order by n.age desc) * 10 'Unit Sourcing Cost'
from (
--this select creates one row for every product for every whs
    select
    k.model_name
    ,k.whs_code
    ,k.age
    ,k.item_number || '_' || k.production_plant || '_' || k.grade || '_' || k.cleaned_spec as product_model_name_no_age
    from iam_distinct_whs_products k
) n
;