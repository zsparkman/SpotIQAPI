from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, HTMLResponse
from emailer import process_email_attachment, send_report, send_error_report
from job_logger import init_db, log_job, update_job_status, get_all_jobs
import uuid
import traceback
import boto3
import os
import json
import requests
from datetime import datetime, timedelta

app = FastAPI()
init_db()

@app.get("/")
def read_root():
    return {"message": "SpotIQ API is live"}

@app.get("/jobs")
def list_jobs(status: str = None, parsed_by: str = None):
    jobs = get_all_jobs()
    if status:
        jobs = [j for j in jobs if j[4] == status]
    if parsed_by:
        jobs = [j for j in jobs if j[9] == parsed_by]

    html = (
        "<h1>SpotIQ Job Log</h1><table border='1'>"
        "<tr><th>Job ID</th><th>Sender</th><th>Subject</th><th>Filename</th><th>Status</th>"
        "<th>Created At</th><th>Updated At</th><th>Error</th><th>Last Rebuild</th>"
        "<th>Parsed By</th><th>Parser Name</th><th>Duration (s)</th></tr>"
    )
    for job in jobs:
        html += "<tr>" + "".join(f"<td>{c or ''}</td>" for c in job) + "</tr>"
    html += "</table>"
    return HTMLResponse(content=html)

@app.get("/events")
def list_events(event_type: str = None, job_id: str = None):
    s3 = boto3.client(
        "s3",
        region_name=os.getenv("AWS_REGION", "us-east-2"),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    )
    bucket = os.getenv("S3_BUCKET_NAME")
    prefix = "event_logs/"
    paginator = s3.get_paginator("list_objects_v2")
    page_iterator = paginator.paginate(Bucket=bucket, Prefix=prefix)

    all_events = []
    for page in page_iterator:
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith(".jsonl"):
                continue
            obj_data = s3.get_object(Bucket=bucket, Key=key)
            lines = obj_data["Body"].read().decode("utf-8").splitlines()
            for line in lines:
                try:
                    event = json.loads(line)
                    all_events.append(event)
                except Exception:
                    continue

    if event_type:
        all_events = [e for e in all_events if e.get("event_type") == event_type]
    if job_id:
        all_events = [e for e in all_events if e.get("job_id") == job_id]

    html = "<h1>SpotIQ Event Log</h1><table border='1'><tr><th>Timestamp</th><th>Event Type</th><th>Job ID</th><th>Details</th></tr>"
    for event in sorted(all_events, key=lambda e: e["timestamp"], reverse=True):
        html += (
            f"<tr><td>{event['timestamp']}</td><td>{event['event_type']}</td>"
            f"<td>{event.get('job_id', '')}</td><td><pre>{json.dumps(event.get('details', {}), indent=2)}</pre></td></tr>"
        )
    html += "</table>"
    return HTMLResponse(content=html)

@app.get("/match-program")
def match_program(title: str):
    if not title:
        return JSONResponse({"error": "Title is required"}, status_code=400)

    try:
        response = requests.get(
            "https://api.tvmaze.com/singlesearch/shows",
            params={"q": title, "embed": "nextepisode"},
        )
        if response.status_code != 200:
            return JSONResponse({"error": "Show not found or API error."}, status_code=response.status_code)

        data = response.json()
        next_ep_info = data.get("_embedded", {}).get("nextepisode")
        is_live = False
        is_first_run = False
        next_airtime = None

        if next_ep_info and next_ep_info.get("airstamp"):
            air_time = datetime.fromisoformat(next_ep_info["airstamp"].replace("Z", "+00:00"))
            now_utc = datetime.utcnow().replace(tzinfo=air_time.tzinfo)
            window = timedelta(minutes=15)

            if air_time - window <= now_utc <= air_time + window:
                is_live = True

            next_airtime = next_ep_info["airstamp"]

            if next_ep_info.get("number") == 1:
                is_first_run = True
            elif data.get("premiered"):
                premiered_date = datetime.fromisoformat(data["premiered"])
                if air_time.date() == premiered_date.date():
                    is_first_run = True

        genres = data.get("genres", [])
        genre_map = {
            "sports": {"Sports"},
            "news": {"News"},
            "reality": {"Reality"},
