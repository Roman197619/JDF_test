{{
    config(
        materialized='table',
        order_by='craft'
    )
}}

select
    craft,
    count(*) as astronauts_count
from {{ source('JDF_DATABASE', 'PARSED_TABLE') }}
group by craft