# emailer.py

import io
import os
import pandas as pd
import requests
from load_parsers import load_all_parsers
from main_parser import save_to_unhandled
from parser import parse_with_gpt

MAILGUN_DOMAIN = os.getenv("MAILGUN_DOMAIN")
MAILGUN_API_KEY = os.getenv("MAILGUN_API_KEY")

# Load parsers on module import
PARSERS = load_all_parsers()

def process_email_attachment(raw_bytes: bytes, filename: str) -> pd.DataFrame:
    """
    Attempt to parse the file using known parsers. If none match, fallback to GPT
    and log the unhandled file.
    """
    try:
        raw_text = raw_bytes.decode("utf-8", errors="ignore")
    except Exception as e:
        raise RuntimeError(f"Failed to decode file: {e}")

    for parser in PARSERS:
        try:
            df = parser(raw_text)
            df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True, errors='coerce')
            df.dropna(subset=['timestamp'], inplace=True)
            print(f"[process_email_attachment] Parsed using {parser.__name__}")
            return df
        except Exception as e:
            print(f"[process_email_attachment] Parser {parser.__name__} failed: {e}")
            continue

    # Fallback to GPT if no parser succeeds
    print("[process_email_attachment] No matching parser found. Using GPT.")
    try:
        parsed_csv = parse_with_gpt(raw_text)
        df = pd.read_csv(io.StringIO(parsed_csv))
        df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True, errors='coerce')
        df.dropna(subset=['timestamp'], inplace=True)
        return df
    except Exception as e:
        print(f"[process_email_attachment] GPT parsing failed: {e}")
        save_to_unhandled(filename, raw_text)
        raise RuntimeError("No parser found. File saved for training.")

def send_report(to_email: str, report_bytes: bytes, filename: str):
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
