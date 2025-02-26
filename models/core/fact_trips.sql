{{
    config(materialized="table")
}}

with green_tripdata as (
    select *, 'Green' as service_type
    from {{ ref('stg_green_tripdata') }}
),
yellow_tripdata as (
    select *, 'Yellow' as service_type
    from {{ ref('stg_yellow_tripdata') }}
),
trips_combined as (
    select * from green_tripdata
    union all
    select * from yellow_tripdata
),
dim_zones as (
    select * from {{ref("dim_zones")}}
    where borough != 'Unknown'
)
select trips_combined.tripid,
    trips_combined.vendorid,
    trips_combined.ratecodeid,
    trips_combined.service_type,
    trips_combined.pickup_locationid,
    pickup_zone.borough as pickup_borough,
    pickup_zone.zone as pickup_zone,
    trips_combined.dropoff_locationid,
    dropoff_zone.borough as dropoff_borough,
    dropoff_zone.zone as dropoff_zone,
    trips_combined.pickup_datetime,
    trips_combined.dropoff_datetime,
    trips_combined.store_and_fwd_flag,
    trips_combined.passenger_count,
    trips_combined.trip_distance,
    trips_combined.trip_type,
--
    trips_combined.fare_amount,
    trips_combined.extra,
    trips_combined.mta_tax,
    trips_combined.tip_amount,
    trips_combined.tolls_amount,
    trips_combined.ehail_fee,
    trips_combined.improvement_surcharge,
    trips_combined.total_amount,
    trips_combined.congestion_surcharge,
    trips_combined.payment_type,
    trips_combined.payment_type_description
from trips_combined 
join {{ ref('taxi_zone_lookup') }} pickup_zone
    on trips_combined.pickup_locationid=pickup_zone.locationid
join {{ ref('taxi_zone_lookup') }} dropoff_zone
    on trips_combined.dropoff_locationid=dropoff_zone.locationid