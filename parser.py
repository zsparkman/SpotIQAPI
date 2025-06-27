import openai
import pandas as pd
import io
import os
import json

# Set your OpenAI API key from environment variable
openai.api_key = os.getenv("OPENAI_API_KEY")

def parse_with_gpt(file_text: str) -> str:
    """
    Sends raw CSV text to OpenAI and expects a response in JSON format
    which is then converted back into a CSV string.
    
    :param file_text: Decoded CSV content as string
    :return: CSV string formatted with standardized headers
    """
    prompt = f"""You are a data parsing assistant. The following is a CSV data dump.
Read the column names and data and infer cleaned and standardized field names such as 
'timestamp', 'creative_id', 'viewer_id', etc. Convert the data into a valid JSON array 
with clearly labeled fields.

CSV:
{file_text}

Respond ONLY with a JSON array where each object corresponds to one row."""

    response = openai.ChatCompletion.create(
        model="gpt-3.5-turbo",
        messages=[
            {"role": "system", "content": "You are a data parsing assistant."},
            {"role": "user", "content": prompt}
        ]
    )

    raw_json = response.choices[0].message["content"]

    try:
        data = json.loads(raw_json)
        df = pd.DataFrame(data)
        return df.to_csv(index=False)
    except Exception as e:
        raise ValueError(f"Failed to parse GPT response into CSV. Raw response:\n{raw_json}\n\nError: {e}")

# For CLI testing only
if __name__ == "__main__":
    file_path = "test_impressions.csv"
    with open(file_path, "r", encoding="utf-8") as f:
        file_text = f.read()
    csv_out = parse_with_gpt(file_text)
    print(csv_out)
