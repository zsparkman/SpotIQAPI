from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, HTMLResponse
from emailer import process_email_attachment, send_report, send_error_report
from job_logger import init_db, log_job, update_job_status, get_all_jobs
import uuid
import traceback
import requests
from datetime import datetime, timedelta
import boto3
import os
import json

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

@app.post("/email-inbound")
async def email_inbound(request: Request):
    job_id = str(uuid.uuid4())
    try:
        form = await request.form()
        sender = form.get("sender", "unknown").strip()
        subject = form.get("subject", "No Subject").strip()
        attachment_count = int(form.get("attachment-count", 0))

        if attachment_count == 0:
            reason = "No attachment provided."
            send_error_report(sender, "unknown", subject, reason)
            return JSONResponse({"error": reason}, status_code=400)

        file_key = "attachment-1"
        upload = form.get(file_key)
        filename = upload.filename if hasattr(upload, "filename") else "unknown"
        file_bytes = await upload.read()

        if filename.lower().endswith(".pdf"):
            reason = "PDF files are not supported."
            send_error_report(sender, filename, subject, reason)
            return JSONResponse({"error": reason}, status_code=400)

        log_job(job_id, sender, subject, filename)
        print(f"[email_inbound] Processing job {job_id} from {sender} - {filename}")

        df, parsed_by, parser_name = process_email_attachment(file_bytes, filename)
        output_csv = df.to_csv(index=False).encode("utf-8")
        send_report(sender, output_csv, f"SpotIQ_Report_{filename}")
        update_job_status(job_id, "completed", parsed_by=parsed_by, parser_name=parser_name)

        return JSONResponse({"message": f"Report sent to {sender}."})

    except Exception as e:
        error_msg = str(e)
        traceback.print_exc()
        sender = sender if "sender" in locals() else "unknown"
        subject = subject if "subject" in locals() else "Unknown"
        filename = filename if "filename" in locals() else "unknown"
        update_job_status(job_id, "failed", error_message=error_msg)
        send_error_report(sender, filename, subject, error_msg)
        return JSONResponse({"error": error_msg}, status_code=500)

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
            "documentary": {"Documentary"},
            "drama": {"Drama"},
            "comedy": {"Comedy"},
            "talk": {"Talk Show"},
            "game": {"Game Show"}
        }

        primary_genre = None
        for key, synonyms in genre_map.items():
            if any(g in synonyms for g in genres):
                primary_genre = key
                break

        return {
            "matched_title": data.get("name"),
            "matched_id": data.get("id"),
            "match_confidence": "exact",
            "genres": genres,
            "primary_genre": primary_genre,
            "language": data.get("language"),
            "runtime": data.get("runtime"),
            "premiered": data.get("premiered"),
            "summary": data.get("summary"),
            "officialSite": data.get("officialSite"),
            "rating": data.get("rating", {}).get("average"),
            "image": data.get("image", {}).get("medium"),
            "tvmaze_url": data.get("url"),
            "is_live": is_live,
            "is_first_run": is_first_run,
            "next_airtime": next_airtime,
        }

    except Exception as e:
        return JSONResponse({"error": f"Exception occurred: {str(e)}"}, status_code=500)
