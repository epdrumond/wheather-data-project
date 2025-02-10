with src_wheather as (
    select *
    from {{ source("wheather_data", "wheather") }}
)

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
    split(
        regexp_replace(
            replace(
                replace(
                    preciptype, 
                    '[', ''
                ), 
                ']', ''
            ),
            r'([\'\"])', ''
        ),
        ','
    ) as precipitation_types,
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
    split(
        regexp_replace(
            replace(
                replace(
                    stations, 
                    '[', ''
                ), 
                ']', ''
            ),
            r'([\'\"])', ''
        ),
        ','
    ) as stations,
    latitude,
    longitude,
    address as full_city_name,
    split(address)[0] as city_name,
    cast(tzoffset as int64) as timezone_offset
from src_wheather