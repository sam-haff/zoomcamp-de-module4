{{
    config(materialized="table")
 }}

with tripdata as (
    select *
    from {{ ref('dim_taxi_trips') }}
)
select 
    pickup_zone as revenue_zone,
    {{ dbt.date_trunc("month", "pickup_datetime") }} as revenue_month,
    service_type as revenue_service,
    sum(fare_amount) as revenue_monthly_fare,
    sum(extra) as revenue_monthly_extra,
    sum(mta_tax) as revenue_monthly_tax,
    sum(tip_amount) as revenue_monthly_tip,
    sum(tolls_amount) as revenue_monthly_tolls,
    sum(ehail_fee) as revenue_monthly_ehail_fee,
    sum(improvement_surcharge) as revenue_monthly_improvement_surcharge,
    sum(total_amount) as revenue_monthly_total,

    count(tripid) as total_monthly_trips,
    avg(passenger_count) as avg_monthly_passenger_count,
    avg(trip_distance) as avg_monthly_trip_distance
from tripdata
group by 1,2,3
