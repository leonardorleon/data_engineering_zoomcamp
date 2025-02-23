{{ config(materialized='table') }}


select 
    main.unique_row_id
    ,main.filename
    ,main.Dispatching_base_num
    ,main.Pickup_datetime
    ,main.DropOff_datetime
    ,main.PULocationID
    ,main.DOLocationID
    ,main.SR_Flag
    ,main.Affiliated_base_number
    ,EXTRACT(YEAR FROM Pickup_datetime)     AS YR
    ,EXTRACT(MONTH FROM Pickup_datetime)    AS MO
    ,pickup_zone.locationid     AS pu_locationid 
    ,pickup_zone.borough        AS pu_borough 
    ,pickup_zone.zone           AS pu_zone
    ,pickup_zone.service_zone   AS pu_service_zone  
    ,dropoff_zone.locationid    AS do_locationid 
    ,dropoff_zone.borough       AS do_borough 
    ,dropoff_zone.zone          AS do_zone
    ,dropoff_zone.service_zone  AS do_service_zone    
from {{ ref('stg_fhv') }} as main
inner join {{ ref('dim_zones') }} as pickup_zone
    on main.PULocationID = pickup_zone.locationid
inner join {{ ref('dim_zones') }} as dropoff_zone
    on main.DOLocationID = dropoff_zone.locationid