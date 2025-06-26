from fastapi import FastAPI, File, UploadFile
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
async def match_impressions(file: UploadFile = File(...)):
    try:
        # Load the uploaded CSV file into a DataFrame
        contents = await file.read()
        impressions_df = pd.read_csv(io.BytesIO(contents))
        
        # Check for required columns
        if 'timestamp' not in impressions_df.columns:
            return JSONResponse(content={"error": "Missing 'timestamp' column"}, status_code=400)

        # Parse timestamps
        impressions_df['timestamp'] = pd.to_datetime(impressions_df['timestamp'], errors='coerce', utc=True)
        impressions_df.dropna(subset=['timestamp'], inplace=True)

        # Fetch today's schedule from TVmaze
        today = date.today().isoformat()
        schedule_url = f"https://api.tvmaze.com/schedule?country=US&date={today}"
        response = requests.get(schedule_url)
        if response.status_code != 200:
            return JSONResponse(content={"error": "TV schedule fetch failed"}, status_code=500)
        tv_data = response.json()

        # Build schedule DataFrame
        schedule = []
        for show in tv_data:
            start_time = datetime.fromisoformat(f"{show['airdate']}T{show['airtime']}")
            end_time = start_time + timedelta(minutes=show['runtime'] or 30)
            schedule.append({
                "name": show['show']['name'],
                "network": show['show']['network']['name'] if show['show']['network'] else 'N/A',
                "start": start_time,
                "end": end_time
            })
        schedule_df = pd.DataFrame(schedule)

        # Match each impression to a show
        def find_match(ts):
            for _, row in schedule_df.iterrows():
                if row['start'] <= ts <= row['end']:
                    return row['name']
            return None

        impressions_df['matched_program'] = impressions_df['timestamp'].apply(find_match)
        matches = impressions_df.dropna(subset=['matched_program'])

        # Return matched records
        results = matches[['timestamp', 'matched_program']].to_dict(orient='records')
        return {"matches": results}

    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)
