import os
import json
import boto3
from datetime import datetime

S3_BUCKET = os.getenv("S3_BUCKET_NAME")
JOB_LOG_KEY = "job_logs/jobs.json"

s3_client = boto3.client(
    "s3",
    region_name=os.getenv("AWS_REGION", "us-east-2"),
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
)

def _load_job_log():
    try:
        obj = s3_client.get_object(Bucket=S3_BUCKET, Key=JOB_LOG_KEY)
        return json.loads(obj["Body"].read().decode("utf-8"))
    except s3_client.exceptions.NoSuchKey:
        return []
    except Exception as e:
        print(f"[job_logger] Failed to load job log: {e}")
        return []

def _save_job_log(jobs):
    try:
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=JOB_LOG_KEY,
            Body=json.dumps(jobs, indent=2).encode("utf-8")
        )
    except Exception as e:
        print(f"[job_logger] Failed to save job log: {e}")

def init_db():
    # No-op to preserve compatibility
    pass

def log_job(job_id, sender, subject, filename):
    jobs = _load_job_log()
    now = datetime.utcnow().isoformat()
    jobs.append({
        "job_id": job_id,
        "sender": sender,
        "subject": subject,
        "filen
