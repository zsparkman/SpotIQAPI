from fastapi import FastAPI, File, UploadFile, Request
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
async def match_impressions(
    request: Request,
    file: UploadFile = File(...)  # make file required
):
    try:
        # 1. Load CSV into DataFrame
        contents = await file.read()
        df = pd.read_csv(io.BytesIO(contents))

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
                local_naive = datetime.strptime(f"{entry['airdate']}T{entry['airtime']}", "%Y-%m-%dT%H:%M")
                eastern = pytz.timezone("US/Eastern")
                local = eastern.localize(local_naive)
                runtime = entry.get('runtime')
                if not isinstance(runtime, int):
                    runtime = 30  # default fallback
                start_utc = local.astimezone(pytz.utc)
                end_utc   = start_utc + timedelta(minutes=runtime)
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
        num_matches = len(matches)

        # 5. Return structured response
        if num_matches == 0:
            return {"status": "no matches found", "matches_found": 0}

        return {
            "status": "success",
            "matches_found": num_matches,
            "matched_impressions": matches[['timestamp', 'matched_program']].to_dict(orient='records')
        }

    except Exception as e:
        print("ERROR:", e)
        traceback.print_exc()
        return JSONResponse({"error": str(e)}, status_code=500)
