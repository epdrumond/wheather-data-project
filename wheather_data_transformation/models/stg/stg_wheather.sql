{{
    config(
        materialized = 'incremental',
        unique_key = ['measurement_date', 'city_name'],
        on_schema_change = 'fail'
    )
}}

with src_wheather as (
    select *
    from {{ source("wheather_data", "wheather") }}
),

dim_stations as (
    select *
    from {{ ref("dim_stations") }}
),

first_level_processing as (
    select 
        datetime(datetime) as measurement_date,
        tempmax as max_temperature,
        tempmin as min_temperature,
        `temp` as mean_temperature,
        feelslikemax as max_apparent_temperarature,
        feelslikemin as min_apparent_temperarature,
        feelslike as mean_apparent_temperature,
        humidity as relative_humidity,
        precip as precipitation,
        precipprob as precipitation_probability,
        precipcover as precipitation_coverage,
        preciptype as precipitation_types,
        snow,
        snowdepth as snow_depth,
        windgust as wind_gust,
        windspeed as wind_speed,
        winddir as wind_direction,
        pressure as sea_level_pressure,
        cloudcover as cloud_coverage,
        visibility,
        solarradiation as solar_radiation,
        solarenergy as solar_energy,
        uvindex as uv_index,
        sunrise as sunrise_time,
        sunset as sunset_time,
        moonphase,
        split(conditions, ', ') as conditions,
        stations,
        latitude,
        longitude,
        address as full_city_name,
        split(address)[0] as city_name,
        cast(tzoffset as int64) as timezone_offset
    from src_wheather
    -- Handle files with overlapping data by selecting the last extraction
    qualify row_number() over (
        partition by datetime, address
        order by extraction_date desc
    ) = 1
)

-- Remove stations listed for which there is no correspondend in the stations table (eg. remote)
select 
    flp.* except(stations),
    array_agg(
        dim.station_id ignore nulls order by dim.station_id
    ) as stations
from 
    first_level_processing as flp
    left join unnest(flp.stations) as station
    left join dim_stations as dim on (
        station = dim.station_id
    )
where true
{% if is_incremental() %}
    {% if var("start_date", false) and var("end_date", false) %}
        and measurement_date >= '{{ var("start_date") }}'
        and measurement_date <= '{{ var("end_date") }}'
    {% else %}
        and measurement_date > (select max(measurement_date) from {{ this }})
    {% endif %}
{% endif %}
group by all