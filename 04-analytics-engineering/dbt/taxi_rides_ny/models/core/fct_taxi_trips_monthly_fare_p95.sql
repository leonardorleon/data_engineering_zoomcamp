{{
    config(
        materialized='table'
    )
}}

with valid as (

    select
        service_type,
        pickup_year,
        pickup_month,
        fare_amount
    from {{ ref("fact_trips") }}
    where fare_amount > 0
        and trip_distance > 0
        and lower(payment_type_description) in ('cash', 'credit card') 

)

select distinct
    service_type,
    pickup_year,
    pickup_month,
    PERCENTILE_CONT(fare_amount, 0.97) OVER (partition by service_type, pickup_year, pickup_month) AS PERCENTILE_97,
    PERCENTILE_CONT(fare_amount, 0.95) OVER (partition by service_type, pickup_year, pickup_month) AS PERCENTILE_95,
    PERCENTILE_CONT(fare_amount, 0.90) OVER (partition by service_type, pickup_year, pickup_month) AS PERCENTILE_90
from valid
where pickup_year = 2020
    and pickup_month = 04
order by service_type