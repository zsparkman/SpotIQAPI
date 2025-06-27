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
            try:
                local_naive = datetime.strptime(f"{entry['airdate']}T{entry['airtime']}", "%Y-%m-%dT%H:%M")
            except Exception:
                continue
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
    best_match = None
    highest_conf = 0
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
            if confidence > highest_conf:
