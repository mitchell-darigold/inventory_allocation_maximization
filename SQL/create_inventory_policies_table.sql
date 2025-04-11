.mode csv
.headers on
.once 'S:\Supply_Chain\Analytics\Inventory Allocation Maximization\MVP Data\input tables\Inventory_Policies.csv'

select
m.whs_code as 'Site'
,m.product_model_name as 'Product'
,m.total_pallets as 'Initial Inventory'
,'' as 'Inventory Policy'
,'' as 'Reorder Point'
,'' as 'Reorder/Order Up To Qty'
,'' as 'Review Period'
,'' as 'Forecast Name'
,'' as 'Forecast Aggregation Period'
,'' as 'Forecast Disaggregation Pattern'
,'' as 'DOS Window'
,'' as 'DOS Planning Lead Time'
,'' as 'Unit Inbound Cost'
,'' as 'Fixed Inbound Shipment Cost'
,'' as 'Unit Outbound Cost'
,'' as 'Fixed Outbound Shipment Cost'
,'' as 'Unit Storage Cost'
,m.unit_disposal_cost as 'Unit Disposal Cost'
,'' as 'Unit Consignment Cost'
,'' as 'Fixed Consignment Cost'
,'' as 'Inv Carrying Cost%'
,'' as 'Stocking Site'
,'' as 'Product Inventory Value'
,'' as 'Product Taxable Value'
,'' as 'Inventory Turns'
,'' as 'Unhealthy Turn Percent'
,'' as 'Fixed Excess Stock'
,'' as 'Excess Move Percent'
,'' as 'Inbound Capacity'
,'' as 'Outbound Capacity'
,'' as 'Constraint Period'
,'' as 'Maximum Inventory'
,'' as 'Minimum Inventory'
,'' as 'Safety Stock'
,'' as 'Minimum Safety Stock'
,'' as 'Maximum Safety Stock'
,'' as 'Allow Partial Fill'
,'' as 'Service Requirement'
,'' as 'Minimum Service Time'
,'' as 'Maximum Service Time'
,'' as 'Minimum Dwell Time'
,'' as 'Maximum Dwell Time'
,'' as 'Unit Delay Factor'
,'' as 'Fixed Order Delay Factor'
,'' as 'Service Type'
,'include' as 'Status'
,'' as 'Notes'

--this subselect just seperates creating all the column names for the model and the actual SQL code below
from (
    select
    p.product_model_name
    ,x.total_pallets
    ,x.whs_code
    ,p.unit_disposal_cost

    from (
        select
        n.product_model_name
        ,row_number() over (partition by product_model_name_no_age order by n.age_joiner desc) * 50 unit_disposal_cost
        from (
        --this section joins the raw inventory data with a joiner table to produce 1 row for every possible age for each sku_prod facility_grade_spec combination at or above the current age of the product in inventory
        --max age for a product is 120 as defined in the mvp_age_joiner table
        select distinct
        mi.item_number || "_" || mi.production_plant || "_" ||  mi.grade || "_" ||  mi.cleaned_spec || "_" ||  aj.age || "D" as product_model_name
        ,mi.item_number || "_" || mi.production_plant || "_" ||  mi.grade || "_" ||  mi.cleaned_spec as product_model_name_no_age
        ,cast(cast(aj.age as real) as integer) as age_joiner

        from mvp_inventory mi

        left join mvp_age_joiner aj
        on mi.joiner=aj.joiner

        where 1=1
        and aj.age >= mi.age
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
) m
;