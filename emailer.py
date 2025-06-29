import io
import os
import base64
import pandas as pd
import requests
import tempfile

from load_parsers import load_all_parsers
from main_parser import save_to_unhandled
from parser import parse_with_gpt
from parsers_registry import get_parser_for_content

MAILGUN_DOMAIN = os.getenv("MAILGUN_DOMAIN")
MAILGUN_API_KEY = os.getenv("MAILGUN_API_KEY")


def push_unhandled_to_github(file_bytes: bytes, filename: str):
    token = os.getenv("GITHUB_TOKEN")
    repo = os.getenv("GITHUB_REPO")
    branch = os.getenv("GITHUB_BRANCH", "main")

    url = f"https://api.github.com/repos/{repo}/contents/unhandled_logs/{filename}"
    encoded = base64.b64encode(file_bytes).decode("utf-8")

    data = {
        "message": f"Add unhandled log: {filename}",
        "content": encoded,
        "branch": branch
    }

    headers = {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github.v3+json"
    }

    response = requests.put(url, headers=headers, json=data)
    if response.status_code not in (200, 201):
        raise RuntimeError(f"GitHub upload failed: {response.status_code} - {response.text}")
    else:
        print(f"[✓] Unhandled log pushed to GitHub: {filename}")


def process_email_attachment(raw_bytes: bytes) -> pd.DataFrame:
    try:
        raw_text = raw_bytes.decode("utf-8", errors="ignore")
        fingerprint = raw_text[:2048]
        parser_name = get_parser_for_content(fingerprint)
        parser_map = load_all_parsers()

        if parser_name and parser_name in parser_map:
            print("[✓] Found matched parser. Trying it.")
            with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmp:
                tmp.write(raw_bytes)
                tmp_path = tmp.name
            try:
                records = parser_map[parser_name](tmp_path)
                df = pd.DataFrame(records)
                df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True, errors='coerce')
                df.dropna(subset=['timestamp'], inplace=True)
                return df
            except Exception as e:
                print(f"[x] Parser failed: {e}")

        print("[↪] Falling back to GPT.")
        parsed_csv = parse_with_gpt(raw_text)
        df = pd.read_csv(io.StringIO(parsed_csv))
        df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True, errors='coerce')
        df.dropna(subset=['timestamp'], inplace=True)

        with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmp:
            tmp.write(raw_bytes)
            tmp_path = tmp.name
            save_to_unhandled(tmp_path)
            push_unhandled_to_github(raw_bytes, os.path.basename(tmp_path))

        return df

    except Exception as e:
        print(f"[process_email_attachment] ERROR: {e}")
        raise RuntimeError(f"Failed to process email attachment: {e}")
