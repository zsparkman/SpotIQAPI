# emailer.py

import io
import os
import pandas as pd
import requests
import tempfile
from load_parsers import get_parser
from main_parser import save_to_unhandled
# from parser import parse_with_gpt  # Optional GPT fallback if desired

MAILGUN_DOMAIN = os.getenv("MAILGUN_DOMAIN")
MAILGUN_API_KEY = os.getenv("MAILGUN_API_KEY")

def process_email_attachment(raw_bytes: bytes) -> pd.DataFrame:
    """
    Try parsing with dynamic parser. If no parser is found, save to unhandled_logs.
    Optional: fallback to GPT if explicitly allowed.
    """
    with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmp:
        tmp.write(raw_bytes)
        tmp_path = tmp.name

    file_name = os.path.basename(tmp_path)
    parser_func = get_parser(file_name)

    if parser_func:
        try:
            print(f"[✓] Found parser for {file_name}. Running it.")
            records = parser_func(tmp_path)
            df = pd.DataFrame(records)
            df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True, errors='coerce')
            df.dropna(subset=['timestamp'], inplace=True)
            return df
        except Exception as e:
            print(f"[x] Parser failed for {file_name}: {e}")
            save_to_unhandled(tmp_path)
            raise RuntimeError("Parser failed. File saved for training.")
    else:
        print(f"[!] No parser available for {file_name}.")
        save_to_unhandled(tmp_path)

        # Optional GPT fallback
        # try:
        #     raw_text = raw_bytes.decode("utf-8", errors="ignore")
        #     print("[fallback] Sending to GPT fallback.")
        #     parsed_csv = parse_with_gpt(raw_text)
        #     df = pd.read_csv(io.StringIO(parsed_csv))
        #     df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True, errors='coerce')
        #     df.dropna(subset=['timestamp'], inplace=True)
        #     return df
        # except Exception as e:
        #     raise RuntimeError(f"GPT fallback also failed: {e}")

        raise RuntimeError("No parser found. File saved for training.")

def send_report(to_email: str, report_bytes: bytes, filename: str):
    """
    Sends the matched report as an attachment back to the sender via Mailgun.
    """
    if not MAILGUN_DOMAIN or not MAILGUN_API_KEY:
        raise RuntimeError("Missing Mailgun config: MAILGUN_DOMAIN or MAILGUN_API_KEY")

    response = requests.post(
        f"https://api.mailgun.net/v3/{MAILGUN_DOMAIN}/messages",
        auth=("api", MAILGUN_API_KEY),
        files=[("attachment", (filename, report_bytes))],
        data={
            "from": f"SpotIQ <mailer@{MAILGUN_DOMAIN}>",
            "to": [to_email],
            "subject": "Your SpotIQ Matched Report",
            "text": "Attached is your SpotIQ match report as a CSV file."
        }
    )

    if response.status_code != 200:
        raise RuntimeError(f"Failed to send email: {response.status_code} - {response.text}")

def send_error_report(to_email: str, filename: str, subject: str, error_message: str):
    """
    Sends an error message email to the user.
    """
    if not MAILGUN_DOMAIN or not MAILGUN_API_KEY:
        raise RuntimeError("Missing Mailgun config: MAILGUN_DOMAIN or MAILGUN_API_KEY")

    message = f"""We were unable to process your file.

File: {filename}
Subject: {subject}

Error:
{error_message}

Please review and try again, or contact support."""

    response = requests.post(
        f"https://api.mailgun.net/v3/{MAILGUN_DOMAIN}/messages",
        auth=("api", MAILGUN_API_KEY),
        data={
            "from": f"SpotIQ <mailer@{MAILGUN_DOMAIN}>",
            "to": [to_email],
            "subject": "SpotIQ Processing Failed",
            "text": message
        }
    )

    if response.status_code != 200:
        raise RuntimeError(f"Failed to send error email: {response.status_code} - {response.text}")
