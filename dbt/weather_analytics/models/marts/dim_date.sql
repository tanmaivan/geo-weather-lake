WITH all_dates AS (
    SELECT DISTINCT
        -- Truncate the timestamp to just the date part.
        CAST(observed_at AS DATE) AS full_date
    FROM
        {{ ref('stg_weather_observations') }}
)

SELECT
    -- Create a numeric primary key in YYYYMMDD format.
    CAST(strftime(full_date, '%Y%m%d') AS INTEGER) AS date_key,
    full_date,
    EXTRACT(YEAR FROM full_date) AS year,
    EXTRACT(QUARTER FROM full_date) AS quarter,
    EXTRACT(MONTH FROM full_date) AS month,
    strftime(full_date, '%B') AS month_name,
    EXTRACT(DAY FROM full_date) AS day,
    EXTRACT(DAYOFWEEK FROM full_date) AS day_of_week_sun_0,
    strftime(full_date, '%A') AS day_name,
    EXTRACT(DAYOFWEEK FROM full_date) IN (0, 6) AS is_weekend
FROM
    all_dates
