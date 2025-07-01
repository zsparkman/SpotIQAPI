from fastapi import FastAPI, UploadFile, File, Request
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from emailer import process_email_attachment, send_report, send_error_report
from job_logger import log_job
import uuid
import traceback
import io

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def read_root():
    return {"message": "SpotIQ API is live."}

@app.get("/jobs")
def read_jobs():
    return {"message": "Job endpoint is working."}

@app.post("/email-inbound")
async def email_inbound(request: Request, file: UploadFile = File(...)):
    job_id = str(uuid.uuid4())
    sender = request.headers.get("X-Sender", "unknown")
    subject = request.headers.get("X-Subject", "no-subject")
    filename = file.filename

    print(f"[email_inbound] Processing job {job_id} from {sender} - {filename}")

    try:
        file_bytes = await file.read()
        df = process_email_attachment(file_bytes, filename)
        print(f"[email_inbound] Processed DataFrame: {df.head()}")
        csv_bytes = df.to_csv(index=False).encode("utf-8")
        send_report(sender, csv_bytes, filename)
        log_job(job_id, sender, subject, filename, "success")
        return JSONResponse(content={"message": "File processed successfully."}, status_code=200)
    except Exception as e:
        error_msg = str(e)
        print(f"[email_inbound] ERROR: {error_msg}")
        traceback.print_exc()
        send_error_report(sender, filename, subject, error_msg)
        log_job(job_id, sender, subject, filename, "failed", error_msg)
        return JSONResponse(content={"message": "Failed to process file."}, status_code=500)
