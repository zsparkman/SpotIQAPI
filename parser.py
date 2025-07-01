import openai
import os

def parse_with_gpt(raw_text: str) -> str:
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("Missing OPENAI_API_KEY in environment.")

    client = openai.OpenAI(api_key=api_key)

    prompt = f"""You are a data cleaning assistant.

You will be given a raw CSV dump. Your task is to clean and standardize it.

- Output valid CSV format ONLY.
- Standardize headers to common terms like: timestamp, creative_id, viewer_id, region.
- Do NOT include commentary, explanation, JSON, markdown formatting, or code blocks.

Raw CSV input:
{raw_text}

Clean and standardize the output as CSV:"""

    response = client.chat.completions.create(
        model="gpt-4-turbo",
        messages=[
            {"role": "system", "content": "You are a CSV data cleaner."},
            {"role": "user", "content": prompt}
        ]
    )

    result = response.choices[0].message.content.strip()
    if ',' not in result or '\n' not in result:
        raise ValueError("Unexpected response format from GPT; missing CSV structure.")
    
    return result
