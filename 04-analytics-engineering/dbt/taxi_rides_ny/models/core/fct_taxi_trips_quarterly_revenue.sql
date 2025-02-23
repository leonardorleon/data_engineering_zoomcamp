{{
    config(
        materialized='table'
    )
}}

with qt_revenue as (
    select
        a.service_type,
        a.year_quarter,
        sum(a.total_amount)   as quarterly_revenue
    from {{ ref('fact_trips') }} as a
    where a.pickup_year in (2019,2020)
    group by 
        a.service_type,
        a.year_quarter
), yoy_comp as (

    select
        service_type
        ,year_quarter
        ,quarterly_revenue
        ,lag(year_quarter,4) 
            OVER (partition by service_type
            order by year_quarter asc)          as yoy_comparison 
        ,lag(quarterly_revenue,4) 
            OVER (partition by service_type
            order by year_quarter asc)          as yoy_comparison_rev 
    from qt_revenue
    order by service_type, year_quarter asc

)

select
    service_type
    ,year_quarter
    ,quarterly_revenue
    ,yoy_comparison 
    ,yoy_comparison_rev 
    ,CASE 
        WHEN yoy_comparison_rev = 0 THEN null
        ELSE (quarterly_revenue / yoy_comparison_rev) -1 
    END AS yoy_rev_growth
from yoy_comp
order by service_type, year_quarter asc