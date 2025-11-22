import os
import logging
from pathlib import Path
from dotenv import load_dotenv
import hashlib
import requests
import pandas as pd
import json
import time
import pyarrow as pa
from datetime import datetime, timezone
from deltalake import write_deltalake


# -- 1. SET UP AND CONFIGURATION
# ==============================

# configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logging.info("Initialising configuration...")

# load the .env file
project_root = Path(__file__).resolve().parents[1]
load_dotenv(Path(__file__).resolve().parent.parent / ".env")
logging.info(f"Loaded environment variables from {project_root}/.env")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
WEATHERBIT_API_KEY = os.getenv("WEATHERBIT_API_KEY")

# define constants
BRONZE_BUCKET = "weather-bronze"
BRONZE_TABLE_PATH = f"s3://{BRONZE_BUCKET}/weather_events"


# load locations from CSV
def load_locations_from_csv():
    """
    This function loads location data from a CSV file
    """
    locations_path = project_root / "resources" / "vietnam_provinces_geocoded.csv"

    try:
        df = pd.read_csv(locations_path)
        logging.info(f"Successfully loaded {len(df)} locations from {locations_path}.")
        return df.to_dict("records")

    except FileNotFoundError:
        logging.error(f"Location file not found at: {locations_path}")
        return []


LOCATIONS = load_locations_from_csv()

logging.info("Configuration setup complete.")


# -- 2. GENERATE UNIQUE ID FOR EACH RECORD
# ========================================
def make_observation_id(source: str, lat: float, lon: float, ts: int) -> str:
    """
    This functions creates a unique observation ID.
    - source: the data source (e.g. weatherbit)
    - lat, lon: coordinates of the observation
    - ts: unix timestamp of the observation
    """
    key = f"{source}|{lat:.6}|{lon:.6}|{ts}"
    return hashlib.sha256(key.encode()).hexdigest()


# test the function
# test_id = make_observation_id("weatherbit", 10.7769, 106.7009, 1672531200)
# logging.info(f"Test ID generated: {test_id}")


# -- 3. API FETCHER FUNCTION
# ==========================
def fetch_weather_data(api_key: str, locations: list) -> list:
    """
    Fetches current weather data for a list of locations.
    """
    logging.info(f"Fetching weather data for {len(locations)} locations...")

    BASE_URL = "http://api.weatherbit.io/v2.0/current"
    all_observations = []

    for i, location in enumerate(locations):
        params = {
            "key": api_key,
            "lat": location["latitude"],
            "lon": location["longitude"],
        }

        try:
            response = requests.get(BASE_URL, params=params, timeout=10)

            if response.status_code == 200:
                data = response.json()
                observations = data.get("data", [])

                if observations:
                    for obs in observations:
                        obs["place_name"] = location["name"]
                    all_observations.extend(observations)

            else:
                logging.error(
                    f"Failed to fetch data for {location['name']}. "
                    f"Status Code: {response.status_code}, Response: {response.text}"
                )

            if i < len(locations) - 1:
                time.sleep(1)

        except Exception as e:
            logging.error(f"Failed to fetch data for {location['name']}: {e}")

    logging.info(f"Successfully fetched {len(all_observations)} observations.")

    return all_observations


# -- 4. DATA TRANSFORMATION TO BRONZE DATA
# ========================================
def transform_to_bronze_df(observations: list) -> pd.DataFrame:
    """
    Transform raw JSON data into a DataFrame matching the Bronze schema.
    """
    if not observations:
        logging.info("No new observations to transform.")
        return pd.DataFrame()

    logging.info("Transforming raw data to Bronze schema...")
    transformed_records = []

    ingested_at = datetime.now(timezone.utc)
    ingest_date = ingested_at.strftime("%Y-%m-%d")

    for observation_data in observations:
        lat = observation_data.get("lat")
        lon = observation_data.get("lon")
        ts = observation_data.get("ts")

        record = {
            "observation_id": make_observation_id("weatherbit", lat, lon, ts),
            "source": "weatherbit_current",
            "raw_payload": json.dumps(observation_data, ensure_ascii=False),
            "lat": lat,
            "lon": lon,
            "_ingested_at": ingested_at,
            "ingest_date": ingest_date,
        }

        transformed_records.append(record)

    df = pd.DataFrame(transformed_records)

    df["_ingested_at"] = pd.to_datetime(df["_ingested_at"])
    df["ingest_date"] = pd.to_datetime(df["ingest_date"]).dt.date

    return df


# -- 5. DATA WRITER FUNCTION
# ==========================
def write_to_bronze(df: pd.DataFrame):
    """
    This function writes a DatFrame to a Delta table on MinIO, partitioned by ingest_date.
    """
    if df.empty:
        logging.info("DataFrame is empty. Skipping write to MinIO.")
        return

    storage_options = {
        "AWS_ACCESS_KEY_ID": AWS_ACCESS_KEY_ID,
        "AWS_SECRET_ACCESS_KEY": AWS_SECRET_ACCESS_KEY,
        "AWS_ENDPOINT_URL": MINIO_ENDPOINT,
        "AWS_ALLOW_HTTP": "true",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    }

    logging.info(f"Writing {len(df)} records to Delta table: {BRONZE_TABLE_PATH}")

    arrow_table = pa.Table.from_pandas(df)

    write_deltalake(
        table_or_uri=BRONZE_TABLE_PATH,
        data=arrow_table,
        mode="append",
        partition_by=["ingest_date"],
        schema_mode="merge",
        storage_options=storage_options,
    )

    logging.info("Write operation completed successfully.")


# -- 6. ORCHESTRATION FUNCTION
# ============================
def run_ingestion():
    """
    This is the main function that orchestrates the entire ingestion process.
    """
    if not WEATHERBIT_API_KEY:
        logging.error("WEATHERBIT_API_KEY is not set. Please check your .env file.")
        raise ValueError("API Key is missing.")

    raw_observations = fetch_weather_data(WEATHERBIT_API_KEY, LOCATIONS)
    bronze_df = transform_to_bronze_df(raw_observations)
    write_to_bronze(bronze_df)

    logging.info("Weather data ingestion process finished.")


if __name__ == "__main__":
    run_ingestion()
