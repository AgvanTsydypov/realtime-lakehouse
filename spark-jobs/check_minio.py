# check_minio.py — list what the pipeline wrote into MinIO
import boto3
from botocore.client import Config

s3 = boto3.client("s3", endpoint_url="http://localhost:9000",
                  aws_access_key_id="minioadmin", aws_secret_access_key="minioadmin",
                  config=Config(signature_version="s3v4"))

# show the top-level "folders" (prefixes) in the bucket
resp = s3.list_objects_v2(Bucket="lakehouse", Delimiter="/")
print("layers in s3://lakehouse/:")
for p in resp.get("CommonPrefixes", []):
    print("  ", p["Prefix"])
