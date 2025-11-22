WITH weather_conditions AS (
    SELECT DISTINCT
        weather_code,
        weather_description
    FROM {{ ref('stg_weather_observations') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['weather_code', 'weather_description']) }} AS weather_key,
    weather_code,
    weather_description
FROM weather_conditions
