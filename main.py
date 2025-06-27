from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from emailer import process_email_attachment, send_report, send_error_report
from job_logger import init_db, log_job, update_job_status
import pandas as pd
import requests
import uuid
import traceback
from datetime import datetime

app = FastAPI()

# Initialize the job log database
init_db()


@app.get("/")
def read_root():
    return {"message": "SpotIQ API is live"}


@app.post("/email-inbound")
async def email_inbound(request: Request):
    try:
        form = await request.form()

        # Extract sender, subject, and attachment
        sender = form.get("sender", "unknown").strip()
        subject = form.get("subject", "No Subject").strip()

        attachment_count = int(form.get("attachment-count", 0))
        if attachment_count == 0:
            return JSONResponse({"error": "No attachment provided."}, status_code=400)

        # Handle only first attachment
        file_key = "attachment-1"
        upload = form.get(file_key)
        filename = upload.filename if hasattr(upload, "filename") else "unknown"
        file_bytes = await upload.read()

        # Reject PDFs
        if filename.lower().endswith(".pdf"):
            reason = "PDF files are not supported."
            send_error_report(sender, filename, subject, reason)
            return JSONResponse({"error": reason}, status_code=400)

        # Generate and log job
        job_id = str(uuid.uuid4())
        log_job(job_id, sender, subject, filename)
        print(f"[email_inbound] Processing job {job_id} from {sender} - {filename}")

        # Process file
        df = process_email_attachment(file_bytes)

        # Convert DataFrame to bytes
        output_csv = df.to_csv(index=False).encode("utf-8")

        # Send report
        send_report(sender, output_csv, f"SpotIQ_Report_{filename}")
        update_job_status(job_id, "completed")

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


@app.get("/jobs")
def list_jobs():
    import sqlite3
    from job_logger import DB_PATH

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM jobs ORDER BY created_at DESC")
    rows = cursor.fetchall()
    columns = [col[0] for col in cursor.description]
    conn.close()

    return [dict(zip(columns, row)) for row in rows]


@app.get("/jobs/{job_id}")
def get_job(job_id: str):
    import sqlite3
    from job_logger import DB_PATH

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM jobs WHERE job_id = ?", (job_id,))
    row = cursor.fetchone()
    columns = [col[0] for col in cursor.description]
    conn.close()

    if not row:
        return JSONResponse({"error": "Job not found."}, status_code=404)

    return dict(zip(columns, row))


@app.get("/build-schedule")
def build_schedule(start: str, end: str):
    try:
        start_date = pd.to_datetime(start).date()
        end_date = pd.to_datetime(end).date()

        schedule_rows = []
        for single_date in pd.date_range(start_date, end_date):
            url = f"https://api.tvmaze.com/schedule?country=US&date={single_date.isoformat()}"
            resp = requests.get(url)
            resp.raise_for_status()

            for entry in resp.json():
                show = entry.get("show", {})
                network = show.get("network") or show.get("webChannel")
                airtime = entry.get("airtime")
                if not network or not airtime:
                    continue

                row = {
                    "date": entry.get("airdate"),
                    "time": airtime,
                    "show": show.get("name"),
                    "episode": entry.get("name"),
                    "network": network.get("name") if network else None
                }
                schedule_rows.append(row)

        return {"schedule": schedule_rows}

    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)
