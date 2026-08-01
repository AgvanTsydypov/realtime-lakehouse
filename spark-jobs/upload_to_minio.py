# upload_to_minio.py — create a bucket in MinIO and upload the local parquet file.
# Talks to MinIO through the port-forward tunnel on localhost:9000.
import boto3
from botocore.client import Config

s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:9000",   # the tunnel to MinIO
    aws_access_key_id="minioadmin",
    aws_secret_access_key="minioadmin",
    config=Config(signature_version="s3v4"),
)

bucket = "lakehouse"

# create the bucket (ignore error if it already exists)
existing = [b["Name"] for b in s3.list_buckets()["Buckets"]]
if bucket not in existing:
    s3.create_bucket(Bucket=bucket)
    print(f"created bucket: {bucket}")
else:
    print(f"bucket already exists: {bucket}")

# upload the raw parquet into a "raw/" prefix inside the bucket
local_file = "data/yellow_tripdata_2024-01.parquet"
key = "raw/yellow_tripdata_2024-01.parquet"
s3.upload_file(local_file, bucket, key)
print(f"uploaded {local_file} -> s3://{bucket}/{key}")

# confirm it landed
for obj in s3.list_objects_v2(Bucket=bucket).get("Contents", []):
    print("  ", obj["Key"], f'({obj["Size"]} bytes)')
