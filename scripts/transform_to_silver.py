import os
import logging
import json
import pandas as pd
import pyarrow as pa
from pathlib import Path
from deltalake import DeltaTable, write_deltalake
from dotenv import load_dotenv

# --- 1. CONFIGURATION ---
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
project_root = Path(__file__).resolve().parents[1]
load_dotenv(dotenv_path=project_root / ".env")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
STORAGE_OPTIONS = {
    "AWS_ACCESS_KEY_ID": AWS_ACCESS_KEY_ID,
    "AWS_SECRET_ACCESS_KEY": AWS_SECRET_ACCESS_KEY,
    "AWS_ENDPOINT_URL": MINIO_ENDPOINT,
    "AWS_ALLOW_HTTP": "true",
    "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
}

BRONZE_TABLE_PATH = "s3://weather-bronze/weather_events"
SILVER_TABLE_PATH = "s3://weather-silver/weather_observations"


# --- 2. DATA READER ---
def read_from_bronze() -> pd.DataFrame:
    try:
        logging.info(f"Reading data from Bronze layer at {BRONZE_TABLE_PATH}...")
        dt = DeltaTable(BRONZE_TABLE_PATH, storage_options=STORAGE_OPTIONS)
        df = dt.to_pandas()
        logging.info(f"Successfully read {len(df)} records from Bronze layer.")
        return df
    except Exception as e:
        logging.error(f"Failed to read from Bronze layer: {e}")
        return pd.DataFrame()


# --- 3. TRANSFORMATION ---
def transform_bronze_to_silver(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        logging.warning("Input DataFrame is empty, skipping transformation.")
        return df

    logging.info("Starting the transformation process...")

    # parse the 'raw_payload' JSON
    logging.info("Parsing 'raw_payload' JSON...")
    payload_df = df["raw_payload"].apply(lambda x: json.loads(x)).apply(pd.Series)

    # combine initial dataframes
    df = pd.concat(
        [df[["observation_id", "_ingested_at", "source"]], payload_df], axis=1
    )

    # handle nested 'weather' object safely
    if "weather" in df.columns:
        weather_df = (
            df["weather"]
            .apply(lambda x: x if isinstance(x, dict) else {})
            .apply(pd.Series)
            .rename(
                columns={
                    "description": "weather_description",
                    "code": "weather_code",
                    "icon": "weather_icon",
                }
            )
        )
        df = pd.concat([df, weather_df], axis=1)
        df = df.drop(columns=["weather"])

    # define the schema
    final_silver_columns = [
        "id",
        "source",
        "ingested_at",
        "place",
        "api_city",
        "country",
        "latitude",
        "longitude",
        "observed_at",
        "temperature",
        "apparent_temperature",
        "humidity",
        "pressure",
        "sea_level_pressure",
        "wind_speed",
        "wind_direction",
        "wind_gust",
        "precipitation",
        "cloud_cover",
        "uv_index",
        "visibility",
        "aqi",
        "weather_code",
        "weather_description",
    ]

    # map raw column names to the desired final names
    column_mapping = {
        "observation_id": "id",
        "_ingested_at": "ingested_at",
        "place_name": "place",
        "city_name": "api_city",
        "country_code": "country",
        "lat": "latitude",
        "lon": "longitude",
        "ts": "observed_at",
        "temp": "temperature",
        "app_temp": "apparent_temperature",
        "rh": "humidity",
        "pres": "pressure",
        "slp": "sea_level_pressure",
        "wind_spd": "wind_speed",
        "wind_dir": "wind_direction",
        "gust": "wind_gust",
        "precip": "precipitation",
        "clouds": "cloud_cover",
        "uv": "uv_index",
        "vis": "visibility",
        "aqi": "aqi",
        "weather_code": "weather_code",
        "weather_description": "weather_description",
    }

    df.rename(columns=column_mapping, inplace=True)

    for col in final_silver_columns:
        if col not in df.columns:
            df[col] = None

    df = df[final_silver_columns]

    # cast data types and handle time
    df["observed_at"] = pd.to_datetime(
        df["observed_at"], unit="s", utc=True, errors="coerce"
    )

    # deduplication
    logging.info("Deduplicating records based on 'place' and observation hour...")

    num_rows_before = len(df)

    df.dropna(subset=["observed_at", "place"], inplace=True)

    df["observation_ymdh"] = df["observed_at"].dt.strftime("%Y-%m-%d-%H")

    df.sort_values(
        by=["place", "observation_ymdh", "observed_at"], ascending=True, inplace=True
    )

    df.drop_duplicates(subset=["place", "observation_ymdh"], keep="first", inplace=True)

    df.drop(columns=["observation_ymdh"], inplace=True)
    num_rows_after = len(df)

    logging.info(
        f"Deduplication removed {num_rows_before - num_rows_after} duplicate records."
    )

    # feature engineering
    logging.info("Performing feature engineering...")

    df["observation_hour"] = df["observed_at"].dt.hour
    df["day_of_week"] = df["observed_at"].dt.day_name()
    df["month"] = df["observed_at"].dt.month

    bins = [-0.1, 0.2, 1.5, 3.3, 5.5, 7.9, 10.7, 13.8]
    labels = [
        "Calm",
        "Light air",
        "Light breeze",
        "Gentle breeze",
        "Moderate breeze",
        "Fresh breeze",
        "Strong breeze",
    ]
    df["wind_category"] = pd.cut(df["wind_speed"], bins=bins, labels=labels, right=True)

    uv_bins = [-0.1, 2, 5, 7, 10, 15]
    uv_labels = ["Low", "Moderate", "High", "Very High", "Extreme"]
    df["uv_category"] = pd.cut(
        df["uv_index"], bins=uv_bins, labels=uv_labels, right=True
    )

    logging.info("Transformation completed.")

    return df


def write_to_silver(df: pd.DataFrame):
    if df.empty:
        logging.warning("Transformed DataFrame is empty. Nothing to write to Silver.")

        return

    logging.info(f"Writing {len(df)} records to Silver layer at {SILVER_TABLE_PATH}...")

    silver_schema = pa.schema(
        [
            ("id", pa.string()),
            ("source", pa.string()),
            ("ingested_at", pa.timestamp("us", tz="UTC")),
            ("place", pa.string()),
            ("api_city", pa.string()),
            ("country", pa.string()),
            ("latitude", pa.float64()),
            ("longitude", pa.float64()),
            ("observed_at", pa.timestamp("us", tz="UTC")),
            ("temperature", pa.float64()),
            ("apparent_temperature", pa.float64()),
            ("humidity", pa.float64()),
            ("pressure", pa.float64()),
            ("sea_level_pressure", pa.float64()),
            ("wind_speed", pa.float64()),
            ("wind_direction", pa.float64()),
            ("wind_gust", pa.float64()),
            ("precipitation", pa.float64()),
            ("cloud_cover", pa.float64()),
            ("uv_index", pa.float64()),
            ("visibility", pa.float64()),
            ("aqi", pa.float64()),
            ("weather_code", pa.float64()),
            ("weather_description", pa.string()),
            ("observation_hour", pa.int32()),
            ("day_of_week", pa.string()),
            ("month", pa.int32()),
            ("wind_category", pa.string()),
            ("uv_category", pa.string()),
        ]
    )

    arrow_table = pa.Table.from_pandas(df, schema=silver_schema, preserve_index=False)

    write_deltalake(
        SILVER_TABLE_PATH,
        arrow_table,
        mode="overwrite",
        schema_mode="overwrite",
        storage_options=STORAGE_OPTIONS,
    )

    logging.info("Successfully wrote data to Silver layer.")


# -- 4. ORCHESTRATION
def run_transformation():
    """
    This is the main function that orchestrates the Bronze to Silver transformation.
    """
    bronze_df = read_from_bronze()
    silver_df = transform_bronze_to_silver(bronze_df)
    write_to_silver(silver_df)

    logging.info("Bronze to Silver transformation pipeline finished successfully.")


if __name__ == "__main__":
    run_transformation()
