import os
import json
import boto3
from datetime import datetime

S3_BUCKET = os.getenv("S3_BUCKET_NAME")
JOB_LOG_KEY = "job_logs/jobs.json"
EVENT_LOG_PREFIX = "event_logs/"

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

def init_db():
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
        "error": None,
        "parsed_by": None,
        "parser_name": None,
        "duration_seconds": None
    })
    _save_job_log(jobs)
    log_event("job_created", job_id=job_id, details={"sender": sender, "filename": filename})

def update_job_status(job_id, status, error_message=None, rebuilt=False, parsed_by=None, parser_name=None):
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
            if parsed_by:
                job["parsed_by"] = parsed_by
            if parser_name:
                job["parser_name"] = parser_name
            if status == "completed" and job.get("created_at"):
                try:
                    start = datetime.fromisoformat(job["created_at"])
                    job["duration_seconds"] = round((datetime.utcnow() - start).total_seconds(), 2)
                except Exception:
                    job["duration_seconds"] = None
            break
    _save_job_log(jobs)
    log_event("job_status_updated", job_id=job_id, details={
        "status": status,
        "error_message": error_message,
        "parsed_by": parsed_by,
        "parser_name": parser_name
    })

def get_all_jobs():
    jobs = _load_job_log()
    jobs.sort(key=lambda j: j.get("updated_at", ""), reverse=True)
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
            job.get("last_rebuild", ""),
            job.get("parsed_by", ""),
            job.get("parser_name", ""),
            job.get("duration_seconds", "")
        )
        for job in jobs
    ]
