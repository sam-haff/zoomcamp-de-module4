{{
    config(
        materialized="table"
    )
}}

select distinct service_type, 
    EXTRACT(YEAR FROM pickup_datetime) as year,
    EXTRACT(MONTH FROM pickup_datetime) as month,
    PERCENTILE_CONT(fare_amount, 0.9)
        OVER (PARTITION BY service_type, EXTRACT(YEAR FROM pickup_datetime), EXTRACT(MONTH FROM pickup_datetime)) as p90,
    PERCENTILE_CONT(fare_amount, 0.95)
        OVER (PARTITION BY service_type, EXTRACT(YEAR FROM pickup_datetime), EXTRACT(MONTH FROM pickup_datetime)) as p95,
    PERCENTILE_CONT(fare_amount, 0.97)
        OVER (PARTITION BY service_type, EXTRACT(YEAR FROM pickup_datetime), EXTRACT(MONTH FROM pickup_datetime)) as p97
from {{ref("fact_trips")}}
where fare_amount > 0 and trip_distance > 0 and payment_type_description in ('Cash', 'Credit card')