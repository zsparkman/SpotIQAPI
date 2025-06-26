from fastapi import FastAPI, File, UploadFile, Request
from fastapi.responses import JSONResponse
import pandas as pd
from datetime import datetime, timedelta, date
import requests
import io

app = FastAPI()

@app.get("/")
def read_root():
    return {"message": "SpotIQ API is live"}

@app.post("/match")
async def match_impressions(
    request: Request,
    file: UploadFile = File(None)  # allow missing file for raw CSV
):
    try:
        # 1. Load CSV into DataFrame—either via upload or raw body
        if file:
            contents = await file.read()
            df = pd.read_csv(io.BytesIO(contents))
        else:
            # raw CSV mode
            body = await request.body()
            df = pd.read_csv(io.BytesIO(body))

        # 2. Normalize timestamp column
        if 'timestamp' not in df.columns:
            return JSONResponse({"error": "Missing 'timestamp' column"}, status_code=400)
        df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce', utc=True)
        df.dropna(subset=['timestamp'], inplace=True)

        # 3. Fetch TV schedule for date range in df
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
                # build UTC window
                local = datetime.fromisoformat(f"{entry['airdate']}T{entry['airtime']}")
                local = local.replace(tzinfo=requests.Session().get_adapter("https://").socket_options and datetime.now().tzinfo)
                start_utc = local.astimezone(tz=pd.Timestamp.utcnow().tzinfo)
                end_utc   = start_utc + timedelta(minutes=entry.get('runtime', 30))
                schedule_rows.append({
                    "name": show['name'],
                    "channel": network['name'],
                    "start": start_utc,
                    "end":   end_utc
                })

        schedule = pd.DataFrame(schedule_rows)

        # 4. Match each timestamp
        def find_match(ts):
            for _, row in schedule.iterrows():
                if row['start'] <= ts <= row['end']:
                    return row['name']
            return None

        df['matched_program'] = df['timestamp'].apply(find_match)
        matches = df.dropna(subset=['matched_program'])

        # 5. Return JSON
        return {"matches": matches[['timestamp','matched_program']].to_dict(orient='records')}

    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)
