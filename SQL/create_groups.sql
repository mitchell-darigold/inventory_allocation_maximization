.mode csv
.headers on
.once 'S:\Supply_Chain\Analytics\Inventory Allocation Maximization\MVP Data\input tables\Group Members.csv'


select distinct g.group_name as 'Group'
,dp.model_name as 'Member'

from (
    --the stuff in this from statement creates the group.  We pull the groups necessary from the orders data.  Then we will fill the groups with inventory data above.  AKA the orders tell us what groups we need and the inventory is what we can actually put into the groups
    --keeping the subselect seperate from the joins that happen above for clarity
    select mo.item_number || "_" || mo.approved_plant_concat || "_" || mo.grade || "_" || mo.spec as group_name
    ,mo.item_number
    ,mo.approved_plant_1
    ,mo.approved_plant_2
    ,mo.approved_plant_3
    ,mo.grade
    ,mo.spec as spec

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
        from mvp_orders
    ) mo
) g

left join (
    select model_name
    ,age
    ,production_plant
    ,cast(grade as INT) as grade
    ,cleaned_spec
    ,item_number
    ,spec1
    ,spec2
    ,spec3
    ,spec4
    ,spec5
    ,spec6
    ,spec7
    from mvp_distinct_inventory_products
 ) dp
on g.item_number = dp.item_number
and (g.grade = dp.grade or g.grade = dp.grade+1)
and (g.approved_plant_1 = dp.production_plant or g.approved_plant_2 = dp.production_plant or g.approved_plant_3 = dp.production_plant)
--this part is tricky and requires manual intervention.  I wont know how many specs will exist in the mvp_distinct_inventory_products table unless I look manually
and (g.spec = dp.spec1 or g.spec = dp.spec2 or g.spec = dp.spec3 or g.spec = dp.spec4 or g.spec = dp.spec5 or g.spec = dp.spec6 or g.spec = dp.spec7)
;

.mode csv
.headers on
.once 'S:\Supply_Chain\Analytics\Inventory Allocation Maximization\MVP Data\input tables\Groups.csv'


select distinct g.group_name as 'Group Name'
,'Product' as 'Group Type'

from (
    --the stuff in this from statement creates the group.  We pull the groups necessary from the orders data.  Then we will fill the groups with inventory data above.  AKA the orders tell us what groups we need and the inventory is what we can actually put into the groups
    --keeping the subselect seperate from the joins that happen above for clarity
    select mo.item_number || "_" || mo.approved_plant_concat || "_" || mo.grade || "_" || mo.spec as group_name
    ,mo.item_number
    ,mo.approved_plant_1
    ,mo.approved_plant_2
    ,mo.approved_plant_3
    ,mo.grade
    ,mo.spec as spec

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
        from mvp_orders
    ) mo
) g

left join (
    select model_name
    ,age
    ,production_plant
    ,cast(grade as INT) as grade
    ,cleaned_spec
    ,item_number
    ,spec1
    ,spec2
    ,spec3
    ,spec4
    ,spec5
    ,spec6
    ,spec7
    from mvp_distinct_inventory_products
 ) dp
on g.item_number = dp.item_number
and (g.grade = dp.grade or g.grade = dp.grade+1)
and (g.approved_plant_1 = dp.production_plant or g.approved_plant_2 = dp.production_plant or g.approved_plant_3 = dp.production_plant)
--this part is tricky and requires manual intervention.  I wont know how many specs will exist in the mvp_distinct_inventory_products table unless I look manually
and (g.spec = dp.spec1 or g.spec = dp.spec2 or g.spec = dp.spec3 or g.spec = dp.spec4 or g.spec = dp.spec5 or g.spec = dp.spec6 or g.spec = dp.spec7)

;