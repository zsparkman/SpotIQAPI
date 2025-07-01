# s3_utils.py

import boto3
import os

AWS_REGION = os.getenv("AWS_REGION", "us-east-2")
S3_BUCKET = os.getenv("S3_BUCKET_NAME")

aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")

if not all([aws_access_key, aws_secret_key, AWS_REGION, S3_BUCKET]):
    print("[S3_UTILS] Missing one or more required AWS environment variables.")

try:
    s3_client = boto3.client(
        "s3",
        region_name=AWS_REGION,
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
    )
except Exception as e:
    print(f"[S3_UTILS] Failed to initialize S3 client: {e}")
    raise

def upload_unhandled_log(filename: str, content: bytes) -> str:
    key = f"unhandled_logs/{filename}"
    try:
        s3_client.put_object(Bucket=S3_BUCKET, Key=key, Body=content)
        print(f"[S3] Uploaded to s3://{S3_BUCKET}/{key}")
        return f"s3://{S3_BUCKET}/{key}"
    except Exception as e:
        print(f"[S3_UTILS] Upload failed: {e}")
        raise

def upload_parser_module(filename: str, content: bytes) -> str:
    key = f"parser_modules/{filename}"
    try:
        s3_client.put_object(Bucket=S3_BUCKET, Key=key, Body=content)
        print(f"[S3] Uploaded parser module to s3://{S3_BUCKET}/{key}")
        return f"s3://{S3_BUCKET}/{key}"
    except Exception as e:
        print(f"[S3_UTILS] Failed to upload parser module: {e}")
        raise
