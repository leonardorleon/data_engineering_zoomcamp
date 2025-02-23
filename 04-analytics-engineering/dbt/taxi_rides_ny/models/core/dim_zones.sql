{{ config(materialized='table') }}

select 
    CAST(locationid AS STRING)  AS locationid, 
    borough, 
    zone, 
    replace(service_zone,'Boro','Green') as service_zone 
from {{ ref('taxi_zone_lookup') }}