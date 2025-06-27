import openai
import os

# Create OpenAI client using environment variable
client = openai.OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

def parse_with_gpt(raw_text: str) -> str:
    """
    Takes raw CSV-like text, sends it to GPT to return a cleaned CSV string.
    """
    prompt = f"""You are a data cleaning assistant.

You will be given a raw CSV dump. Your task is to clean and standardize it.

- Output valid CSV format ONLY.
- Use consistent headers such as: timestamp, creative_id, viewer_id, region, etc.
- Do NOT include commentary, explanation, JSON, markdown formatting, or code blocks.

Raw CSV input:
{raw_text}

Clean and standardize the output as CSV:"""

    response = client.chat.completions.create(
        model="gpt-3.5-turbo",
        messages=[
            {"role": "system", "content": "You are a CSV data cleaner."},
            {"role": "user", "content": prompt}
        ]
    )

    return response.choices[0].message.content.strip()
