{{
    config(
        materialized = 'incremental',
        unique_key = 'fact_wheather_measurement_id',
        on_schema_change = 'fail'
    )
}}
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
where true 
{% if is_incremental() %}
    {% if var("start_date", false) and var("end_date", false) %}
        and measurement_date >= '{{ var("start_date") }}'
        and measurement_date <= '{{ var("end_date") }}'
    {% else %}
        and measurement_date > (select max(measurement_date from {{ this }}))
    {% endif %}
{% endif %}