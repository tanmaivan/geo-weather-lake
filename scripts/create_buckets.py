import os
import logging
from dotenv import load_dotenv
from pathlib import Path
import boto3
from botocore.exceptions import ClientError

# load .env from the project root
project_root = Path(__file__).resolve().parents[1]
load_dotenv(dotenv_path=project_root / ".env")

# MINIO_ENDPOINT=os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ENDPOINT = "http://localhost:9000"
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "minio")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "minio123")

# create s3 client
s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
)


def check_bucket_exists(bucket_name: str) -> bool:
    try:
        s3.head_bucket(Bucket=bucket_name)
        return True
    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code")
        if error_code in ("404", "NoSuchBucket"):
            return False
        raise


def create_bucket(bucket_name: str) -> None:
    if check_bucket_exists(bucket_name):
        print(f"Bucket exists: {bucket_name}")
        return

    try:
        s3.create_bucket(Bucket=bucket_name)
        print(f"Bucket created: {bucket_name}")
    except ClientError as e:
        logging.error(e)
        raise


def list_buckets():
    response = s3.list_buckets()

    # Output the bucket names
    print("Existing buckets:")
    for bucket in response["Buckets"]:
        print(f'  {bucket["Name"]}')


def delete_buckets(names):
    for name in names:
        resp = s3.list_objects_v2(Bucket=name)
        if "Contents" in resp:
            for obj in resp["Contents"]:
                s3.delete_object(Bucket=name, Key=obj["Key"])

        s3.delete_bucket(Bucket=name)


def main():
    buckets = ["weather-bronze", "weather-silver"]
    for b in buckets:
        create_bucket(b)


if __name__ == "__main__":
    main()
