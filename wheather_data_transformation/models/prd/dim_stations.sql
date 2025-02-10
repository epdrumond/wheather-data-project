with stg_stations as (
    select *
    from {{ ref("stg_stations") }}
)

select distinct 
    station_id,
    station_name,
    latitude,
    longitude
from stg_stations