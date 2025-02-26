{{
    config(
        materialized="table"
    )
}}

select distinct service_type, 
    year,
    month,
    PERCENTILE_CONT(fare_amount, 0.9)
        OVER (PARTITION BY service_type, year, month) as p90,
    PERCENTILE_CONT(fare_amount, 0.95)
        OVER (PARTITION BY service_type, year, month) as p95,
    PERCENTILE_CONT(fare_amount, 0.97)
        OVER (PARTITION BY service_type, year, month) as p97
from {{ref("dim_taxi_trips")}}
where fare_amount > 0 and trip_distance > 0 and payment_type_description in ('Cash', 'Credit card')