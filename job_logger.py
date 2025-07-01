import os
import json
import boto3
from datetime import datetime

S3_BUCKET = os.getenv("S3_BUCKET_NAME")
EVENT_LOG_PREFIX = "event_logs/"

s3_client = boto3.client(
    "s3",
    region_name=os.getenv("AWS_REGION", "us-east-2"),
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
)

def log_event(event_type, job_id=None, details=None):
    now = datetime.utcnow().isoformat()
    event = {
        "timestamp": now,
        "event_type": event_type,
        "job_id": job_id,
        "details": details or {}
    }
    log_key = f"{EVENT_LOG_PREFIX}{now[:10]}.jsonl"
    try:
        existing = ""
        try:
            obj = s3_client.get_object(Bucket=S3_BUCKET, Key=log_key)
            existing = obj["Body"].read().decode("utf-8")
        except s3_client.exceptions.NoSuchKey:
            pass
        existing += json.dumps(event) + "\n"
        s3_client.put_object(Bucket=S3_BUCKET, Key=log_key, Body=existing.encode("utf-8"))
    except Exception as e:
        print(f"[event_logger] Failed to log event: {e}")
