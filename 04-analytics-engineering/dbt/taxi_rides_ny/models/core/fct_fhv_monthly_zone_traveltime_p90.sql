{{
    config(
        materialized='view'
    )
}}

with duration as (
    
    select
        YR
        ,MO
        ,PULocationID
        ,DOLocationID
        ,TIMESTAMP_DIFF(DropOff_datetime,Pickup_datetime, second) AS trip_duration 
        ,pu_borough 
        ,pu_zone
        ,pu_service_zone  
        ,do_borough 
        ,do_zone
        ,do_service_zone    
    from {{ ref("dim_fhv_trips") }}
    WHERE YR = 2019 and MO = 11

)

select distinct
    YR
    ,MO
    ,PULocationID
    ,DOLocationID
    ,trip_duration 
    ,pu_borough 
    ,pu_zone
    ,pu_service_zone  
    ,do_borough 
    ,do_zone
    ,do_service_zone    
    ,PERCENTILE_CONT(trip_duration, 0.9) OVER (partition by YR, MO, PULocationID, DOLocationID) AS percentile_90_trip_duration
from duration