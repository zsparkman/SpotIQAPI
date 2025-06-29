# emailer.py

import io
import os
import pandas as pd
import requests
from main_parser import get_parser_output, save_to_unhandled
from parser import parse_with_gpt

MAILGUN_DOMAIN = os.getenv("MAILGUN_DOMAIN")
MAILGUN_API_KEY = os.getenv("MAILGUN_API_KEY")

def process_email_attachment(raw_bytes: bytes) -> pd.DataFrame:
    """
    Tries parser-based matching, falls back to GPT, and logs to unhandled if all fails.
    """
    try:
        print("[process_email_attachment] Attempting parser match...")
        df = get_parser_output(raw_bytes)
        print("[process_email_attachment] Matched parser succeeded.")
        return df
    except Exception as parser_err:
        print(f"[process_email_attachment] No parser matched: {parser_err}")
        try:
            raw_text = raw_bytes.decode("utf-8", errors="ignore")
            parsed_csv = parse_with_gpt(raw_text)
            df = pd.read_csv(io.StringIO(parsed_csv))
            df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True, errors='coerce')
            df.dropna(subset=['timestamp'], inplace=True)
            print("[process_email_attachment] Fallback to GPT succeeded.")
            return df
        except Exception as gpt_err:
            print(f"[process_email_attachment] GPT fallback failed: {gpt_err}")
            save_to_unhandled("unknown.csv", raw_bytes)
            raise RuntimeError("No parser found and GPT fallback failed.")

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
