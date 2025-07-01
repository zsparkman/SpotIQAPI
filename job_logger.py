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
    # No-op to preserve compatibility with interface
    pass

def log_job(job_id, sender, subject, filename):
    jobs = _load_job_log()
    now = datetime.utcnow().isoformat()
    jobs.append({
        "job_id": job_id,
        "sender": sender,
        "subject": subject,
        "filename": filename,
        "status": "processing",
        "created_at": now,
        "updated_at": now,
        "last_rebuild": None,
        "error": None
    })
    _save_job_log(jobs)

def update_job_status(job_id, status, error_message=None, rebuilt=False):
    jobs = _load_job_log()
    now = datetime.utcnow().isoformat()
    for job in jobs:
        if job["job_id"] == job_id:
            job["status"] = status
            job["updated_at"] = now
            if error_message:
                job["error"] = error_message
            if rebuilt:
                job["last_rebuild"] = now
            break
    _save_job_log(jobs)

def get_all_jobs():
    return [
        (
            job["job_id"],
            job["sender"],
            job["subject"],
            job["filename"],
            job["status"],
            job["created_at"],
            job["updated_at"],
            job["error"],
            job.get("last_rebuild", "")
        )
        for job in _load_job_log()
    ]
