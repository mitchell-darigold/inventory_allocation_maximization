.mode csv
.headers on
.once 'S:\Supply_Chain\Analytics\Inventory Allocation Maximization\MVP Data\input tables\Customer_Demand.csv'

select mp.period_number as Period
,mo.order_number as Customer
,g.group_name as Product
,'Set' as CollectionBasisProductName
,'0' as 'Minimum Quantity'
,'10000' as 'Unit Penalty Cost'
--the 'set' should, i think, make these groups use the All not Each setting.  Meaning the demand will fill with any one product in the group not try to fill all the products in the group.
,mo.ordered_pallets as Quantity

from mvp_orders mo

left join mvp_periods mp
on mo.ship_date_formatted=mp.date_formatted

left join (
    --the stuff in this statement creates the group.
    --keeping the subselect seperate from the joins that happen above for clarity
    select mo.item_number || "_" || mo.approved_plant_concat || "_" || mo.grade || "_" || mo.spec as group_name
    ,mo.item_number
    ,mo.approved_plant_1
    ,mo.approved_plant_2
    ,mo.approved_plant_3
    ,mo.grade
    ,mo.spec as spec
    ,mo.order_number
    from (
        --just doing the case when in a subselect for clarity
        select item_number
        ,approved_plant_1
        ,approved_plant_2
        ,approved_plant_3
        ,case when approved_plant_2 = '' then approved_plant_1
            when approved_plant_3 = '' then approved_plant_1 || "-" || approved_plant_2
            else approved_plant_1
            end as approved_plant_concat
        ,grade
        ,spec
        ,order_number
        from mvp_orders
    ) mo
) g
on mo.order_number=g.order_number
and mo.item_number=g.item_number
;