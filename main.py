# main.py

from fastapi import FastAPI, Request, UploadFile, Form
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from emailer import process_email_attachment, send_report
import traceback
import logging

app = FastAPI()

# Optional: Enable CORS if needed
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def root():
    return {"message": "SpotIQ API is live"}

@app.post("/email-inbound")
async def email_inbound(request: Request):
    try:
        form = await request.form()
        print(f"Form keys received: {list(form.keys())}")

        sender = form.get("From") or form.get("from")
        subject = form.get("Subject") or form.get("subject")
        attachment = form.get("attachment-1")
        filename = attachment.filename if attachment else "N/A"

        print(f"Sender: {sender}")
        print(f"Subject: {subject}")
        print(f"Filename: {filename}")

        if not attachment:
            raise ValueError("No attachment found.")

        raw_bytes = await attachment.read()
        print("[process_email_attachment] Raw text decoded. Sending to GPT.")
        df = process_email_attachment(raw_bytes)
        print("[process_email_attachment] CSV parsed and cleaned.")

        csv_bytes = df.to_csv(index=False).encode("utf-8")
        send_report(sender, csv_bytes, filename="matched_report.csv")
        print("Report emailed successfully.")

        return JSONResponse({"status": "processed"}, status_code=200)

    except Exception as e:
        print(f"[process_email_attachment] ERROR: {e}")
        traceback.print_exc()
        return JSONResponse({"error": str(e)}, status_code=500)
