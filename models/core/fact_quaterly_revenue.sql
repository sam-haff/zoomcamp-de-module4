{{
    config(materialized="table")
}}

with quat_rev_raw AS (
    select service_type, EXTRACT(YEAR FROM pickup_datetime) as year, {{get_date_quater("pickup_datetime")}} as quater, SUM(total_amount) as revenue
    from {{ ref("fact_trips") }}
    where EXTRACT(YEAR FROM pickup_datetime) = 2019 or EXTRACT(YEAR FROM pickup_datetime) = 2020
    group by 1, 2, 3
)
select service_type, year, quater, revenue,
revenue/(LAG(revenue, 1, NULL) OVER (PARTITION BY service_type, quater ORDER BY year)) - 1.0 as yoy_growth
FROM quat_rev_raw
