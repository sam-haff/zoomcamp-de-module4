{{
    config(materialized="view")
}}

with trip_data as (
    -- WARNING: it's clearly incorrect way to filter out the data.
    -- VendorID is a brand of taxis which has many taxis operating at the same time,
    -- so it CAN have several same pickup times(there are only like 3 vendors out there).
    -- You can verify it yourself, by investigating records manually. 
    -- For example you can query for all the records with pickup time of '2021-06-10 20:52:43'.
    -- You will there are two records at this time with the same vendor but different passenger count
    -- and trip distance.
    select *, row_number() over (partition by vendorid, tpep_pickup_datetime) as rn 
    from {{ source('staging', 'yellow_tripdata') }}
    where vendorid is not null
),

renamed as (

    select
        {{ dbt_utils.generate_surrogate_key(['vendorid', 'tpep_pickup_datetime']) }} as tripid,
        cast(vendorid as integer) as vendorid,
        cast (ratecodeid as integer) as ratecodeid,
        cast(pulocationid as integer) as pickup_locationid,
        cast(dolocationid as integer) as dropoff_locationid,
--
        cast(tpep_pickup_datetime as timestamp) as pickup_datetime,
        cast(tpep_dropoff_datetime as timestamp) as dropoff_datetime,
--
        store_and_fwd_flag as store_and_fwd_flag,
        cast(passenger_count as integer) as passenger_count,
        cast(trip_distance as numeric) as trip_distance,
        cast(1 as integer) as trip_type,
--
        cast(fare_amount as numeric) as fare_amount,
        cast(extra as numeric) as extra,
        cast(mta_tax as numeric) as mta_tax,
        cast(tip_amount as numeric) as tip_amount,
        cast(tolls_amount as numeric) as tolls_amount,
        cast(0 as numeric) as ehail_fee,
        cast(improvement_surcharge as numeric) as improvement_surcharge,
        cast(total_amount as numeric) as total_amount,
        cast(congestion_surcharge as numeric) as congestion_surcharge,
        cast(payment_type as integer) as payment_type,
        {{get_payment_type_desc('payment_type')}} as payment_type_description
    from trip_data
    where rn=1

)

select * from renamed
{% if var('is_test_run', default=true) %}
limit {{var('test_run_limit')}}
{% endif %}
