.mode csv
.headers on
.once 'S:\Supply_Chain\Analytics\Inventory Allocation Maximization\MVP Data\input tables\Periods.csv'

select period_number as Name
,date_formatted as 'Start Date'
,'' as Notes
 from mvp_periods;