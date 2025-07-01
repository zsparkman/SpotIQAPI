from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, HTMLResponse
from emailer import process_email_attachment, send_report, send_error_report
from job_logger import init_db, log_job, update_job_status, get_all_jobs
import uuid
import traceback
import requests
from datetime import datetime, timedelta

app = FastAPI()
init_db()


@app.get("/")
def read_root():
    return {"message": "SpotIQ API is live"}


@app.get("/jobs")
def list_jobs():
    jobs = get_all_jobs()
    html = "<h1>SpotIQ Job Log</h1><table border='1'><tr><th>Job ID</th><th>Sender</th><th>Subject</th><th>Filename</th><th>Status</th><th>Created At</th><th>Updated At</th><th>Error</th></tr>"
    for job in jobs:
        html += "<tr>" + "".join(f"<td>{c or ''}</td>" for c in job) + "</tr>"
    html += "</table>"
    return HTMLResponse(content=html)


@app.post("/email-inbound")
async def email_inbound(request: Request):
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

        job_id = str(uuid.uuid4())
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
        job_id = job_id if "job_id" in locals() else str(uuid.uuid4())
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
            "match_confiden_
