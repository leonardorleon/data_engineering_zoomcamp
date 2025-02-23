{{
    config(
        materialized='view'
    )
}}

select  
    *
from {{ source('staging','fhv_tripdata') }}
where Dispatching_base_num is not null