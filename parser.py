import os
import openai
import pandas as pd

# Correct way to initialize the OpenAI client (>=1.0.0)
client = openai.OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

def parse_with_gpt(raw_text: str) -> str:
    prompt = f"""You are a data parsing assistant. The following is a CSV data dump. Read the column names and data and suggest a cleaned format that extracts standardized column headers and parses each row.

CSV:
{raw_text}

Respond with a CSV-formatted string using clearly labeled fields such as 'timestamp', 'creative_id', 'viewer_id', 'region', etc.
Infer field meanings if unclear."""

    response = client.chat.completions.create(
        model="gpt-3.5-turbo",
        messages=[
            {"role": "system", "content": "You are a data parsing assistant."},
            {"role": "user", "content": prompt}
        ]
    )

    return response.choices[0].message.content
