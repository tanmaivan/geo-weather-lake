SELECT *
FROM {{ source('silver', 'weather_observations') }}
