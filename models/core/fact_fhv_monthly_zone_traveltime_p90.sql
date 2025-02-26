{{
    config(
        materialized="table"
    )
}}

select distinct year, month, pulocationid as pickup_locationid, pickup_zone, dolocationid as dropoff_locationid, dropoff_zone,
    PERCENTILE_CONT(trip_duration, 0.9) OVER (PARTITION BY year, month, pulocationid, dolocationid) as p90
from {{ref("dim_fhv_trips")}}