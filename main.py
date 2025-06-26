from fastapi import FastAPI, File, UploadFile
from fastapi.responses import JSONResponse
import pandas as pd
import pytz
import traceback
from datetime import datetime, timedelta
import requests
import io

app = FastAPI()

@app.get("/")
def read_root():
    return {"message": "SpotIQ API is live"}

@app.post("/match")
async def match_impressions(file: UploadFile = File(...)):
    try:
        # Load CSV into DataFrame
        contents = await file.read()
        df = pd.read_csv(io.BytesIO(contents))

        # Normalize timestamp column
        if 'timestamp' not in df.columns:
            return JSONResponse({"error": "Missing 'timestamp' column"}, status_code=400)
        df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce', utc=True)
        df.dropna(subset=['timestamp'], inplace=True)

        # Get TV schedule
        first_date = df['timestamp'].dt.date.min().isoformat()
        last_date  = df['timestamp'].dt.date.max().isoformat()

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
                local_naive = datetime.strptime(f"{entry['airdate']}T{entry['airtime']}", "%Y-%m-%dT%H:%M")
                eastern = pytz.timezone("US/Eastern")
                local = eastern.localize(local_naive)
                start_utc = local.astimezone(pytz.utc)
                end_utc   = start_utc + timedelta(minutes=entry.get('runtime', 30))
                schedule_rows.append({
                    "name": show['name'],
                    "channel": network['name'],
                    "start": start_utc,
                    "end":   end_utc
                })

        schedule = pd.DataFrame(schedule_rows)

        # Match timestamps
        def find_match(ts):
            for _, row in schedule.iterrows():
                if row['start'] <= ts <= row['end']:
                    return row['name']
            return None

        df['matched_program'] = df['timestamp'].apply(find_match)
        matches = df.dropna(subset=['matched_program'])

        return {"matches": matches[['timestamp', 'matched_program']].to_dict(orient='records')}

    except Exception as e:
        print("ERROR:", e)
        traceback.print_exc()
        return JSONResponse({"error": str(e)}, status_code=500)
