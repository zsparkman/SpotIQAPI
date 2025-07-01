# s3_utils.py

import boto3
import os

# Load AWS configuration from environment
AWS_REGION = os.getenv("AWS_REGION", "us-east-2")
S3_BUCKET = os.getenv("S3_BUCKET_NAME")

# Debug print to confirm credentials are being picked up
aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")

if not all([aws_access_key, aws_secret_key, AWS_REGION, S3_BUCKET]):
    print("[S3_UTILS] Missing one or more required AWS environment variables.")
    print("AWS_ACCESS_KEY_ID:", repr(aws_access_key))
    print("AWS_SECRET_ACCESS_KEY:", repr(aws_secret_key))
    print("AWS_REGION:", AWS_REGION)
    print("S3_BUCKET_NAME:", S3_BUCKET)

# Initialize S3 client
try:
    s3_client = boto3.client("s3", region_name=AWS_REGION)
except Exception as e:
    print(f"[S3_UTILS] Failed to initialize S3 client: {e}")
    raise

def upload_unhandled_log(filename: str, content: bytes) -> str:
    """
    Uploads an unhandled log file to the specified S3 bucket under the 'unhandled_logs/' prefix.
    """
    if not all([aws_access_key, aws_secret_key]):
        raise RuntimeError("AWS credentials are missing in environment.")

    key = f"unhandled_logs/{filename}"

    try:
        s3_client.put_object(Bucket=S3_BUCKET, Key=key, Body=content)
        print(f"[S3] Uploaded to s3://{S3_BUCKET}/{key}")
        return f"s3://{S3_BUCKET}/{key}"
    except Exception as e:
        print(f"[S3_UTILS] Upload failed: {e}")
        raise
