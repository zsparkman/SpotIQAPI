from fastapi import FastAPI, Request, UploadFile, File
from fastapi.responses import JSONResponse
from emailer import process_email_attachment, send_report, send_error_report
from job_logger import init_db, log_job, update_job_status
import uuid
import traceback

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

        # Extract sender, subject, attachment
        sender = form.get("sender", "unknown").strip()
        subject = form.get("subject", "No Subject").strip()

        attachment_count = int(form.get("attachment-count", 0))
        if attachment_count == 0:
            return JSONResponse({"error": "No attachment provided."}, status_code=400)

        # Handle first attachment only
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

        # Update job status and notify sender
        update_job_status(job_id, "failed", error_message=error_msg)
        send_error_report(sender, filename, subject, error_msg)

        return JSONResponse({"error": error_msg}, status_code=500)
