from fastapi import FastAPI, File, UploadFile, Request
from fastapi.responses import JSONResponse
import pandas as pd
import pytz
import traceback
from datetime import datetime, timedelta
import requests
import io

# Import parser and emailer
from parser import parse_with_gpt
from emailer import process_email_attachment, send_report

app = FastAPI()

def match_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    # Normalize timestamp
    if 'timestamp' not in df.columns:
        raise ValueError("Missing 'timestamp' column")
    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce', utc=True)
    df.dropna(subset=['timestamp'], inplace=True)

    first_date = df['timestamp'].dt.date.min().isoformat()
    last_date = df['timestamp'].dt.date.max().isoformat()

    schedule_rows = []
    for single_date in pd.date_range(first_date, last_date):
        url = f"https://api.tvmaze.com/schedule?country=US&date={single_date.date().isoformat()}"
        resp = requests.get(url)
        resp.raise_for_status()
        for entry in resp.json():
            show = entry['show']
            network = show.get('network') or show.get('webChannel')
            if not network or not entry.get('airtime'):
                continue

            local_naive = datetime.strptime(
                f"{entry['airdate']}T{entry['airtime']}", "%Y-%m-%dT%H:%M"
            )
            eastern = pytz.timezone("US/Eastern")
            local = eastern.localize(local_naive)
            runtime = entry.get('runtime') or 30
            start_utc = local.astimezone(pytz.utc)
            end_utc = start_utc + timedelta(minutes=runtime)

            schedule_rows.append({
                "name": show['name'],
                "channel": network['name'],
                "start": start_utc,
                "end": end_utc
            })

    schedule = pd.DataFrame(schedule_rows)

    def find_match(ts):
        for _, row in schedule.iterrows():
            if row['start'] <= ts <= row['end']:
                return row['name']
        return None

    df['matched_program'] = df['timestamp'].apply(find_match)
    return df.dropna(subset=['matched_program'])

@app.get("/")
def read_root():
    return {"message": "SpotIQ API is live"}

@app.post("/match")
async def match_impressions(file: UploadFile = File(...)):
    try:
        contents = await file.read()
        df = pd.read_csv(io.BytesIO(contents))
        results_df = match_dataframe(df)
        num = len(results_df)
        if num == 0:
            return {"status": "no matches found", "matches_found": 0}
        return {
            "status": "success",
            "matches_found": num,
            "matched_impressions": results_df[['timestamp', 'matched_program']].to_dict(orient='records')
        }
    except Exception as e:
        print("ERROR:", e)
        traceback.print_exc()
        return JSONResponse({"error": str(e)}, status_code=500)

@app.post("/email-inbound")
async def email_inbound(request: Request, file: UploadFile = File(...)):
    try:
        raw_bytes = await file.read()
        df = process_email_attachment(raw_bytes)
        results_df = match_dataframe(df)

        form = await request.form()
        sender = form.get("sender") or form.get("from")
        if sender:
            csv_bytes = results_df[['timestamp', 'matched_program']].to_csv(index=False).encode("utf-8")
            send_report(
                to_email=sender,
                report_bytes=csv_bytes,
                filename="SpotIQ_Report.csv"
            )

        return {"status": "processed", "matches_found": len(results_df)}
    except Exception as e:
        print("EMAIL ERROR:", e)
        traceback.print_exc()
        return JSONResponse({"error": str(e)}, status_code=500)
