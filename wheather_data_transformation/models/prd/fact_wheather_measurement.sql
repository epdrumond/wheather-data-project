with stg_wheather as (
    select *
    from {{ ref("stg_wheather") }}
)

select 
    {{ dbt_utils.generate_surrogate_key(['measurement_date', 'city_name']) }} as fact_wheather_measurement_id,
    measurement_date,
    max_temperature,
    min_temperature,
    mean_temperature,
    max_apparent_temperarature,
    min_apparent_temperarature,
    mean_apparent_temperature,
    relative_humidity,
    precipitation,
    precipitation_probability,
    precipitation_coverage,
    precipitation_types,
    snow,
    snow_depth,
    wind_gust,
    wind_speed,
    wind_direction,
    sea_level_pressure,
    cloud_coverage,
    visibility,
    solar_radiation,
    solar_energy,
    uv_index,
    sunrise_time,
    sunset_time,
    case
        when moonphase = 0 then 'New Moon'
        when moonphase > 0 and moonphase < 0.25 then 'Waxing Crescent'
        when moonphase = 0.25 then 'First Quarter'
        when moonphase > 0.25 and moonphase < 0.5 then 'Waxing Gibbous'
        when moonphase = 0.5 then 'Full Moon'
        when moonphase > 0.5  and moonphase < 0.75 then 'Waning Gibbous'
        when moonphase = 0.75 then 'Last Quarter'
        when moonphase > 0.75 and moonphase <= 1 then 'Waning Crescent'
    end as moon_phase,
    conditions,
    stations,
    city_name
from stg_wheather