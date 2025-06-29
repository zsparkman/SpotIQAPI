# emailer.py

import io
import os
import pandas as pd
import requests
from parser import parse_with_gpt
from main_parser import save_to_unhandled
from load_parsers import load_all_parsers

MAILGUN_DOMAIN = os.getenv("MAILGUN_DOMAIN")
MAILGUN_API_KEY = os.getenv("MAILGUN_API_KEY")

def process_email_attachment(raw_bytes: bytes) -> pd.DataFrame:
    """
    Try to parse the attachment using a known parser.
    If none work, fallback to GPT.
    If that fails, save the unhandled log.
    """
    raw_text = raw_bytes.decode("utf-8", errors="ignore")
    filename = "unknown.csv"

    # Try all loaded custom parsers
    try:
        for parser_func in load_all_parsers():
            try:
                df = parser_func(raw_bytes)
                if not df.empty:
                    print("[process_email_attachment] Parsed using custom parser.")
                    return df
            except Exception as parse_error:
                print(f"[parser error] {parse_error}")
    except Exception as e:
        print(f"[parser loading error] {e}")

    # Fallback to GPT
    try:
        print("[GPT fallback] No parser matched, using GPT.")
        parsed_csv = parse_with_gpt(raw_text)
        df = pd.read_csv(io.StringIO(parsed_csv))
        df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True, errors='coerce')
        df.dropna(subset=['timestamp'], inplace=True)
        return df
    except Exception as gpt_error:
        print(f"[GPT error] {gpt_error}")
        save_to_unhandled(filename, raw_bytes)
        raise RuntimeError("No parser found. File saved for training.")

def send_report(to_email: str, report_bytes: bytes, filename: str):
    if not MAILGUN_DOMAIN or not MAILGUN_API_KEY:
        raise RuntimeError("Missing Mailgun config.")

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
        raise RuntimeError("Missing Mailgun config.")

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
