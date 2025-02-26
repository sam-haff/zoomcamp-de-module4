{{
    config(materialized="table")
}}

with quat_rev_raw AS (
    select service_type, year, quater, SUM(total_amount) as revenue
    from {{ ref("dim_taxi_trips") }}
    where year = 2019 or year = 2020
    group by 1, 2, 3
)
select service_type, year, quater, revenue,
revenue/(LAG(revenue, 1, NULL) OVER (PARTITION BY service_type, quater ORDER BY year)) - 1.0 as yoy_growth
FROM quat_rev_raw
