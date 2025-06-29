# emailer.py

import io
import os
import pandas as pd
import requests
from parser import parse_with_gpt
from load_parsers import load_all_parsers
from parsers_registry import get_parser_for_content
from main_parser import save_to_unhandled

MAILGUN_DOMAIN = os.getenv("MAILGUN_DOMAIN")
MAILGUN_API_KEY = os.getenv("MAILGUN_API_KEY")

# Load all available parsers at startup
PARSERS = load_all_parsers()

def process_email_attachment(file_bytes: bytes) -> pd.DataFrame:
    """
    Attempt to parse the attachment using a known parser.
    If no parser matches, fallback to GPT. If GPT fails, save to unhandled.
    """
    try:
        raw_text = file_bytes.decode("utf-8", errors="ignore")
        df = None

        parser_func = get_parser_for_content(raw_text, PARSERS)
        if parser_func:
            try:
                df = parser_func(raw_text)
            except Exception as e:
                print(f"[!] Parser matched but failed: {e}")
                save_to_unhandled("fallback_failed.csv", file_bytes)
                raise RuntimeError("Matched parser failed. Sent to unhandled logs.")

        if df is None:
            print("[*] No parser matched. Using GPT.")
            parsed_csv = parse_with_gpt(raw_text)
            df = pd.read_csv(io.StringIO(parsed_csv))

        df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True, errors='coerce')
        df.dropna(subset=['timestamp'], inplace=True)
        return df

    except Exception as e:
        print(f"[process_email_attachment] ERROR: {e}")
        save_to_unhandled("unhandled_fallback.csv", file_bytes)
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
