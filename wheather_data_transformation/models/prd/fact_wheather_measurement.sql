with stg_wheather as (
    select *
    from {{ ref("stg_wheather") }}
)

select 
    {{ dbt_utils.generate_surrogate_key(['measurement_date', 'city_name'])}} as fact_wheather_measurement_id,
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
    sea_level_pressue,
    cloud_cover,
    visibility,
    solar_radiation,
    solar_energy,
    uv_index,
    sunrise_time,
    sunset_time,
    moonphase,
    conditions,
    stations,
    city_name
from stg_wheather