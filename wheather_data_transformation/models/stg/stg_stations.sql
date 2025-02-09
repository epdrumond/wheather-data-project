with src_stations as(
    select *
    from {{ source("wheather_data", "stations") }}
)

select 
  id as station_id,
  name as station_name,
  distance as distance_from_location,
  latitude,
  longitude,
  cast(datetime as timestamp) as snapshot_date
from src_stations 