import openai
import pandas as pd
import io
import os

# Set your OpenAI API key
openai.api_key = os.getenv("OPENAI_API_KEY")

def load_csv(file_path):
    with open(file_path, 'rb') as f:
        return f.read()

def parse_with_gpt(file_bytes):
    csv_data = file_bytes if isinstance(file_bytes, str) else file_bytes.decode('utf-8')

    prompt = f"""You are a data parsing assistant. The following is a CSV data dump. Read the column names and data and suggest a cleaned format that extracts standardized column headers and parses each row.

CSV:
{csv_data}

Respond with a CSV with standardized columns like 'timestamp', 'creative_id', 'viewer_id', 'region', etc. If a field is unclear, infer what it should be and label it clearly.
"""

    response = openai.chat.completions.create(
        model="gpt-3.5-turbo",
        messages=[
            {"role": "system", "content": "You are a helpful data parsing assistant."},
            {"role": "user", "content": prompt}
        ]
    )

    return response.choices[0].message.content

if __name__ == "__main__":
    file_path = "test_impressions.csv"
    file_bytes = load_csv(file_path)
    parsed_output = parse_with_gpt(file_bytes)
    print(parsed_output)
