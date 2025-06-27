from fastapi import FastAPI, File, UploadFile, Request
from fastapi.responses import JSONResponse
import pandas as pd
import pytz
import traceback
from datetime import datetime, timedelta
import requests
import io

# Import your parser & matcher modules
from parser import parse_impressions_with_gpt
from matcher import match_dataframe
from emailer import process_email_attachment, send_report

app = FastAPI()

@app.get("/")
def read_root():
    return {"message": "SpotIQ API is live"}

@app.post("/match")
async def match_impressions(
    file: UploadFile = File(...)  # direct CSV upload
):
    try:
        contents = await file.read()
        df = pd.read_csv(io.BytesIO(contents))

        # normalize
        if 'timestamp' not in df.columns:
            return JSONResponse({"error": "Missing 'timestamp' column"}, status_code=400)
        df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce', utc=True)
        df.dropna(subset=['timestamp'], inplace=True)

        # match
        results_df = match_dataframe(df)
        num = len(results_df)
        if num == 0:
            return {"status": "no matches found", "matches_found": 0}
        return {
            "status": "success",
            "matches_found": num,
            "matched_impressions": results_df.to_dict(orient='records')
        }

    except Exception as e:
        print("ERROR:", e)
        traceback.print_exc()
        return JSONResponse({"error": str(e)}, status_code=500)


@app.post("/email-inbound")
async def email_inbound(
    request: Request,
    file: UploadFile = File(...)  # Mailgun attachment
):
    try:
        # 1) Read raw attachment bytes
        raw_bytes = await file.read()

        # 2) Parser step (GPT-powered)
        df = process_email_attachment(raw_bytes)

        # 3) Matcher step
        results_df = match_dataframe(df)

        # 4) Email back the report
        form = await request.form()
        sender = form.get("sender", None) or form.get("from", None)
        if sender:
            csv_bytes = results_df.to_csv(index=False).encode("utf-8")
            send_report(
                to_email=sender,
                report_bytes=csv_bytes,
                filename="SpotIQ_Report.csv"
            )

        # 5) Final webhook response
        return {"status": "processed", "matches_found": len(results_df)}

    except Exception as e:
        print("EMAIL ERROR:", e)
        traceback.print_exc()
        return JSONResponse({"error": str(e)}, status_code=500)
