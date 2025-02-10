with fwm_stations as (
  select distinct station
  from 
      {{ ref("fact_wheather_measurement") }} as fwm
      left join unnest(fwm.stations) as station
)

select *
from 
  fwm_stations as fwm
  left join {{ ref("dim_stations") }} as st on (
    fwm.station = st.station_id
  )
where st.station_id is null