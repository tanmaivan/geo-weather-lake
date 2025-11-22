# Guide 05: Visualization & Dashboard (Metabase)

This guide covers the final phase: Visualization. We will connect Metabase to our Gold Layer (DuckDB) and build the "Vietnam Weather Overview" dashboard to answer our business questions.

## 1. Database Connection

Before creating any charts, Metabase needs to know how to read our Data Warehouse.

### Step 1.1: Access Admin Settings

1.  Log in to Metabase (`http://localhost:3000`).
2.  Click the Gear Icon (⚙️) in the top right $\to$ Admin settings.
3.  Go to the Databases tab.
4.  Click Add database.

### Step 1.2: Configure DuckDB

Enter the following connection details.

> Crucial: You must use the internal container path, not the path on your local machine.

| Field              | Value                                   | Notes                                                               |
| :----------------- | :-------------------------------------- | :------------------------------------------------------------------ |
| Database type      | DuckDB                                  |                                                                     |
| Display name       | `Weather Data Warehouse`                | Or any name you prefer.                                             |
| Database file path | `/app/dbt/weather_analytics/dbt.duckdb` | This path corresponds to the volume mount in `docker-compose.yaml`. |

Click Save. Metabase will sync with the database file.

---

## 2. Data Modeling (Metadata)

To enable smart filtering and drilling, we need to define relationships between our Fact and Dimension tables.

1.  In Admin settings, go to the Table Metadata tab.
2.  Select Weather Data Warehouse.

### Define Relationships (Foreign Keys)

Go to the `marts.fact_hourly_weather` table and configure the columns:

- `location_key`: Set "Field Type" to Foreign Key $\to$ Target: `dim_location.location_key`.
- `date_key`: Set "Field Type" to Foreign Key $\to$ Target: `dim_date.date_key`.
- `weather_key`: Set "Field Type" to Foreign Key $\to$ Target: `dim_weather.weather_key`.

### Define Entity Types

Go to the Dimension tables and configure the business keys:

- `dim_location.place`: Set "Field Type" to City (enables map features).
- `dim_location.latitude`: Set "Field Type" to Latitude.
- `dim_location.longitude`: Set "Field Type" to Longitude.

---

## 3. Creating Questions (Charts)

Use this reference table to build the visualization layer:

| Question Name             | Visualization    | Data Source           | Metric / Logic                                                             | Grouping / Dimensions                                   | Settings                                             |
| :------------------------ | :--------------- | :-------------------- | :------------------------------------------------------------------------- | :------------------------------------------------------ | :--------------------------------------------------- |
| Total Locations           | Number           | `dim_location`        | Count of rows                                                              | -                                                       | -                                                    |
| Average Temperature       | Number           | `fact_hourly_weather` | Avg of `avg_temperature_celsius`                                           | -                                                       | Suffix: °C                                           |
| Average Humidity          | Number           | `fact_hourly_weather` | Avg of `avg_humidity_percent`                                              | -                                                       | Suffix: %                                            |
| Highest Wind              | Number           | `fact_hourly_weather` | Max of `max_wind_speed_ms`                                                 | -                                                       | Suffix: m/s                                          |
| Average Cloud Coverage    | Number           | `fact_hourly_weather` | Avg of `avg_cloud_coverage_percent`                                        | -                                                       | Suffix: %                                            |
| Temp and Rainfall Trends  | Line + Bar       | `fact_hourly_weather` | 1. Avg of `avg_temperature_celsius`<br>2. Sum of `total_precipitation_mmh` | `dim_date.full_date`                                    | Display `Precipitation` as Bar. Enable Split Y-Axis. |
| Map of Locations          | Pin Map          | `dim_location`        | -                                                                          | -                                                       | Map columns: Lat/Lon.<br>Tooltip: Place.             |
| Top 5 Highest Temp        | Bar (Horizontal) | `fact_hourly_weather` | Avg of `avg_temperature_celsius`                                           | `dim_location.place`                                    | Sort: Descending.<br>Row Limit: 5.                   |
| Top 5 Lowest Temp         | Bar (Horizontal) | `fact_hourly_weather` | Avg of `avg_temperature_celsius`                                           | `dim_location.place`                                    | Sort: Ascending.<br>Row Limit: 5.                    |
| Top 5 Most Humid          | Bar (Horizontal) | `fact_hourly_weather` | Avg of `avg_humidity_percent`                                              | `dim_location.place`                                    | Sort: Descending.<br>Row Limit: 5.                   |
| Top 5 Least Humid         | Bar (Horizontal) | `fact_hourly_weather` | Avg of `avg_humidity_percent`                                              | `dim_location.place`                                    | Sort: Ascending.<br>Row Limit: 5.                    |
| Top 5 Strongest Wind      | Bar (Horizontal) | `fact_hourly_weather` | Max of `max_wind_speed_ms`                                                 | `dim_location.place`                                    | Sort: Descending.<br>Row Limit: 5.                   |
| Top 5 Weakest Wind        | Bar (Horizontal) | `fact_hourly_weather` | Max of `max_wind_speed_ms`                                                 | `dim_location.place`                                    | Sort: Ascending.<br>Row Limit: 5.                    |
| Weather Type Distribution | Stacked Bar      | `fact_hourly_weather` | Sum of `observation_count`                                                 | 1. `dim_location.place`<br>2. `dim_weather.description` | Display: Stacked (100%).                             |
| Detailed Data Table       | Table            | SQL Query             | (See SQL Code below)                                                       | -                                                       | -                                                    |

### SQL Code for "Detailed Data Table"

Since this requires complex joins and specific variable filters, use the SQL Editor:

```sql
SELECT
  d.full_date AS "Date",
  f.observation_hour AS "Hour",
  l.place AS "Location",
  ROUND(f.avg_temperature_celsius, 1) AS "Temperature (°C)",
  w.weather_description AS "Weather Description",
  f.observation_count AS "Observation Count"
FROM
  main_marts.fact_hourly_weather AS f
  LEFT JOIN main_marts.dim_location AS l ON f.location_key = l.location_key
  LEFT JOIN main_marts.dim_date AS d ON f.date_key = d.date_key
  LEFT JOIN main_marts.dim_weather AS w ON f.weather_key = w.weather_key
WHERE
  1 = 1 [[ AND {{date}} ]] [[ AND {{place}} ]]
ORDER BY
  d.full_date DESC,
  f.observation_hour DESC
LIMIT
  200;
```

#### Configuration for SQL Variables (Variables Panel)

In the SQL Editor, open the **Variables** panel (right sidebar) to configure the parameters used in the `WHERE` clause.

**1. Variable: `date`**

- Variable type: Field Filter
- Field to map to: `Dim Date` $\to$ `Full Date`
- Table and field alias: `date` _(Important: must match `{{date}}` in SQL)_
- Filter widget type: Date Range (or All Options)
- Filter widget label: Date

**2. Variable: `place`**

- Variable type: Field Filter
- Field to map to: `Dim Location` $\to$ `Place`
- Table and field alias: `place` _(Important: must match `{{place}}` in SQL)_
- Filter widget type: Location
- Filter widget label: Location
- How should users filter on this variable?
  - Widget type: dropdown list
  - Configuration: multiple values

---

## 4. Building the Dashboard

Goal: Assemble "Vietnam Weather Overview".

1.  Create Dashboard: Click + New $\to$ Dashboard. Name it "Vietnam Weather Overview".
2.  Add Questions: Click the + (Add) button and select all the questions created in Step 3.
3.  Arrange: Drag and drop to create a logical layout (KPIs at top, Trends in middle, Details at bottom).
4.  Add Filters: Click the Filter Icon. Select a filter type (e.g., Time, Location). Then, map this filter to the corresponding fields on the cards/charts you want to filter.
5.  Save: Click Save.

---

## 5. Final Result

You now have a fully interactive dashboard.

- Monitoring: Check the top KPIs for immediate status.
- Analysis: Use the Time filter to see historical trends.
- Comparison: Select multiple cities in the Location filter to compare specific provinces.

![](../../assets/images/dashboard/1.png)
![](../../assets/images/dashboard/2.png)
![](../../assets/images/dashboard/3.png)
![](../../assets/images/dashboard/4.png)
![](../../assets/images/dashboard/5.png)

The pipeline is officially live. From raw API calls to actionable dashboard insights!

---

< [Back to Data Modeling](04-data-modeling.md) | [Home](../../README.md) >
