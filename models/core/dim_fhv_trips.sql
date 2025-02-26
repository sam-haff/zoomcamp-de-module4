{{
    config(
        materialized="table"
    )
}}

select td.dispatch_base,
    EXTRACT(YEAR FROM td.pickup_datetime) as year, 
    EXTRACT(MONTH FROM td.pickup_datetime) as month,
    td.pickup_datetime,
    td.dropoff_datetime,
    td.pulocationid,
    pickup_zone.borough as pickup_borough,
    pickup_zone.zone as pickup_zone,
    td.dolocationid,
    dropoff_zone.borough as dropoff_borough,
    dropoff_zone.zone as dropoff_zone,
    td.SR_Flag,
    td.affiliated_base,
    TIMESTAMP_DIFF(td.dropoff_datetime, td.pickup_datetime, SECOND) as trip_duration
from {{ ref("stg_fhv_tripdata") }} td
join {{ ref('taxi_zone_lookup') }} pickup_zone
    on td.pulocationid=pickup_zone.locationid
join {{ ref('taxi_zone_lookup') }} dropoff_zone
    on td.dolocationid=dropoff_zone.locationid