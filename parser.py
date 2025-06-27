import openai
import os
import textwrap

MAX_CHARS = 12000  # ~4,000 tokens per chunk; safely under GPT-4 limit

def parse_with_gpt(raw_text: str) -> str:
    """
    Splits raw CSV text into safe-size chunks, sends each to GPT-4, combines cleaned outputs.
    """
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("Missing OPENAI_API_KEY in environment.")

    client = openai.OpenAI(api_key=api_key)

    # Break raw text into manageable chunks
    chunks = textwrap.wrap(raw_text, MAX_CHARS, break_long_words=False, replace_whitespace=False)

    cleaned_chunks = []
    for i, chunk in enumerate(chunks):
        print(f"[parse_with_gpt] Processing chunk {i+1}/{len(chunks)}")
        prompt = f"""You are a data cleaning assistant.

You will be given a raw CSV dump. Your task is to clean and standardize it.

- Output valid CSV format ONLY.
- Standardize headers to common terms like: timestamp, creative_id, viewer_id, region.
- Do NOT include commentary, explanation, JSON, markdown formatting, or code blocks.

Raw CSV input:
{chunk}

Clean and standardize the output as CSV:"""

        response = client.chat.completions.create(
            model="gpt-4",
            messages=[
                {"role": "system", "content": "You are a CSV data cleaner."},
                {"role": "user", "content": prompt}
            ]
        )

        result = response.choices[0].message.content.strip()
        if ',' not in result or '\n' not in result:
            raise ValueError(f"[Chunk {i+1}] Unexpected GPT response — missing CSV structure.")

        cleaned_chunks.append(result)

    # Merge cleaned CSVs: preserve header from first, skip headers in subsequent chunks
    final_output = cleaned_chunks[0]
    for chunk in cleaned_chunks[1:]:
        lines = chunk.strip().splitlines()
        if len(lines) > 1:
            final_output += "\n" + "\n".join(lines[1:])  # skip repeated headers

    return final_output
