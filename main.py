from fastapi import FastAPI, File, UploadFile, Request
from fastapi.responses import JSONResponse
import pandas as pd
import pytz
import traceback
from datetime import datetime, timedelta
import requests
import io
from emailer import process_email_attachment, send_report

app = FastAPI()

@app.get("/")
def read_root():
    return {"message": "SpotIQ API is live"}


def build_schedule(start_date, end_date):
    schedule_rows = []
    for single_date in pd.date_range(start_date, end_date):
        url = f"https://api.tvmaze.com/schedule?country=US&date={single_date.date().isoformat()}"
        resp = requests.get(url)
        resp.raise_for_status()
        for entry in resp.json():
            show = entry['show']
            network = show.get('network') or show.get('webChannel')
            if not network or not entry.get('airtime'):
                continue
            local_naive = datetime.strptime(f"{entry['airdate']}T{entry['airtime']}", "%Y-%m-%dT%H:%M")
            eastern = pytz.timezone("US/Eastern")
            local = eastern.localize(local_naive)
            runtime = entry.get('runtime', 30)
            start_utc = local.astimezone(pytz.utc)
            end_utc = start_utc + timedelta(minutes=runtime)
            schedule_rows.append({
                "name": show['name'],
                "channel": network['name'],
                "start": start_utc,
                "end": end_utc,
                "runtime": runtime
            })
    return pd.DataFrame(schedule_rows)


def find_match_with_confidence(ts, schedule):
    for _, row in schedule.iterrows():
        if row['start'] <= ts <= row['end']:
            time_from_start = (ts - row['start']).total_seconds() / 60
            time_to_end = (row['end'] - ts).total_seconds() / 60
            if time_from_start <= 5:
                confidence = 100
                reason = "Within first 5 minutes of airing"
            elif time_from_start <= 10:
                confidence = 90
                reason = "Within first 10 minutes of airing"
            elif time_to_end <= 5:
                confidence = 60
                reason = "Near end of program"
            else:
                confidence = 75
                reason = "Within airing window"
            return pd.Series([row['name'], confidence, row['channel'], reason])
    return pd.Series([None, None, None, None])


@app.post("/match")
async def match_impressions(file: UploadFile = File(...)):
    try:
        contents = await file.read()
        df = pd.read_csv(io.BytesIO(contents))

        if 'timestamp' not in df.columns:
            return JSONResponse({"error": "Missing 'timestamp' column"}, status_code=400)

        df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce', utc=True)
        df.dropna(subset=['timestamp'], inplace=True)

        first_date = df['timestamp'].dt.date.min().isoformat()
        last_date = df['timestamp'].dt.date.max().isoformat()
        schedule = build_schedule(first_date, last_date)

        df[['matched_program', 'match_confidence', 'matched_channel', 'match_reason']] = df['timestamp'].apply(lambda ts: find_match_with_confidence(ts, schedule))

        matches = df.dropna(subset=['matched_program'])
        num_matches = len(matches)

        if num_matches == 0:
            return {"status": "no matches found", "matches_found": 0}

        return {
            "status": "success",
            "matches_found": num_matches,
            "matched_impressions": matches[['timestamp', 'matched_program', 'matched_channel', 'match_confidence', 'match_reason']].to_dict(orient='records')
        }

    except Exception as e:
        print("ERROR:", e)
        traceback.print_exc()
        return JSONResponse({"error": str(e)}, status_code=500)


@app.post("/email-inbound")
async def email_inbound(request: Request):
    try:
        form = await request.form()
        sender = form.get("sender") or form.get("from")
        attachments = form.getlist("attachment")

        if not attachments:
            return JSONResponse({"error": "No attachments found"}, status_code=400)

        upload = attachments[0]
        raw_bytes = await upload.read()

        df = process_email_attachment(raw_bytes)

        if 'timestamp' not in df.columns:
            return JSONResponse({"error": "Missing 'timestamp' column"}, status_code=400)

        df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce', utc=True)
        df.dropna(subset=['timestamp'], inplace=True)

        first_date = df['timestamp'].dt.date.min().isoformat()
        last_date = df['timestamp'].dt.date.max().isoformat()
        schedule = build_schedule(first_date, last_date)

        df[['matched_program', 'match_confidence', 'matched_channel', 'match_reason']] = df['timestamp'].apply(lambda ts: find_match_with_confidence(ts, schedule))

        matches = df.dropna(subset=['matched_program'])

        csv_buf = io.StringIO()
        matches.to_csv(csv_buf, index=False)
        csv_bytes = csv_buf.getvalue().encode("utf-8")

        send_report(
            to_email=sender,
            report_bytes=csv_bytes,
            filename="SpotIQ_Report.csv"
        )

        return {
            "status": "processed",
            "matches_found": len(matches)
        }

    except Exception as e:
        print("EMAIL ERROR:", e)
        traceback.print_exc()
        return JSONResponse({"error": str(e)}, status_code=500)
