WITH stg_data AS (
    SELECT
        place,
        api_city,
        country,
        latitude,
        longitude,
        ROW_NUMBER() OVER(PARTITION BY place ORDER BY ingested_at DESC) as rn
    FROM
        {{ ref('stg_weather_observations') }}
    WHERE place IS NOT NULL
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['place']) }} AS location_key,
    place,
    api_city,
    country,
    latitude,
    longitude
FROM stg_data
WHERE rn = 1
