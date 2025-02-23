{{
    config(
        materialized='table'
    )
}}

with quarterly_revenue as (
    select
        service_type,
        year_quarter,
        sum(total_amount)   as quarterly_revenue
    from {{ ref('fact_trips') }}
    group by 
        service_type,
        year_quarter
)

select
    service_type,
    year_quarter,
    quarterly_revenue,
    ,lag(year_quarter,1) 
      OVER (partition by service_type
      order by service_type, year_quarter asc) as yoy_comparison 
    ,lag(quarterly_revenue,1) 
        OVER (partition by service_type
        order by service_type, year_quarter asc) as yoy_comparison_rev 
    ,quarterly_revenue - lag(quarterly_revenue,1) 
                            OVER (partition by service_type
                            order by service_type, year_quarter asc) as yoy_revenue
from quarterly_revenue
order by service_type, year_quarter asc
