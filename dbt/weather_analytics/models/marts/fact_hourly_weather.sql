WITH
stg_weather AS (
    SELECT *
    FROM {{ ref('stg_weather_observations') }}
),
dim_location AS (
    SELECT *
    FROM {{ ref('dim_location') }}
),
dim_date AS (
    SELECT *
    FROM {{ ref('dim_date') }}
),
dim_weather AS (
    SELECT *
    FROM {{ ref('dim_weather')}}
)

SELECT
    l.location_key,
    d.date_key,
    w.weather_key,

    stg.observation_hour,

    AVG(stg.temperature) AS avg_temperature_celsius,
    AVG(stg.apparent_temperature) AS avg_feels_like_celsius,
    AVG(stg.humidity) AS avg_humidity_percent,
    MAX(stg.wind_speed) AS max_wind_speed_ms,
    AVG(stg.cloud_cover) AS avg_cloud_coverage_percent,
    SUM(stg.precipitation) AS total_precipitation_mmh,

    COUNT(stg.id) AS observation_count
FROM stg_weather stg
LEFT JOIN dim_location l ON stg.place = l.place
LEFT JOIN dim_date d ON CAST(stg.observed_at AS DATE) = d.full_date
LEFT JOIN dim_weather w ON stg.weather_code = w.weather_code AND stg.weather_description = w.weather_description
GROUP BY
    l.location_key,
    d.date_key,
    w.weather_key,
    stg.observation_hour
