from fastapi import FastAPI, Request, UploadFile, File
from fastapi.responses import JSONResponse
import uvicorn
import pandas as pd
from emailer import process_email_attachment, send_report, send_failure_notice

app = FastAPI()

@app.get("/")
def health_check():
    return {"message": "SpotIQ API is live"}

@app.post("/email-inbound")
async def email_inbound(request: Request):
    form = await request.form()
    print(f"Form keys received: {list(form.keys())}")

    try:
        sender_email = form.get("sender") or form.get("from")
        subject_line = form.get("subject") or form.get("Subject")
        print(f"Sender: {sender_email}")
        print(f"Subject: {subject_line}")

        attachment_count = int(form.get("attachment-count", 0))
        if attachment_count == 0:
            raise ValueError("No attachments found in the email.")

        first_attachment_key = "attachment-1"
        file = form.get(first_attachment_key)
        filename = getattr(file, "filename", "attachment.csv")
        print(f"Filename: {filename}")

        # Reject PDFs
        if filename.lower().endswith(".pdf"):
            raise ValueError("PDF files are not supported at this time.")

        raw_bytes = await file.read()

        # Process and respond
        df = process_email_attachment(raw_bytes)
        csv_bytes = df.to_csv(index=False).encode("utf-8")
        send_report(sender_email, csv_bytes, filename="SpotIQ_Matched_Report.csv")

        return JSONResponse({"status": "success"}, status_code=200)

    except Exception as e:
        error_message = str(e)
        print(f"[ERROR] Failed to process email: {error_message}")

        try:
            send_failure_notice(sender_email, subject_line or "Submission Error", error_message)
        except Exception as email_err:
            print(f"[ERROR] Failed to send failure notice: {email_err}")

        return JSONResponse({"error": error_message}, status_code=500)

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=10000)
