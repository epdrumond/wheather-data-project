with stg_wheather as (
    select *
    from {{ ref("stg_wheather") }}
),

seed_cities as (
    select *
    from {{ ref("cities") }}
),

cities_coordinates as (
  select distinct 
    city_name,
    latitude,
    longitude,
    timezone_offset
  from stg_wheather 
)

select 
  ct.*,
  coord.latitude,
  coord.longitude,
  coord.timezone_offset
from 
  seed_cities as ct 
  left join cities_coordinates as coord on (
    ct.city = coord.city_name
  )