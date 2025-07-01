import boto3
import os

AWS_REGION = os.getenv("AWS_REGION", "us-west-2")
S3_BUCKET = os.getenv("S3_BUCKET_NAME")

s3_client = boto3.client("s3", region_name=AWS_REGION)

def upload_unhandled_log(filename: str, content: bytes) -> str:
    key = f"unhandled_logs/{filename}"
    s3_client.put_object(Bucket=S3_BUCKET, Key=key, Body=content)
    print(f"[S3] Uploaded to s3://{S3_BUCKET}/{key}")
    return f"s3://{S3_BUCKET}/{key}"
