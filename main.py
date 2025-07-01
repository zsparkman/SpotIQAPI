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
            reason = "No attachment provided.
