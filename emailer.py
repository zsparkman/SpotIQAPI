import io
import os
import pandas as pd
import requests
from parser import parse_with_gpt
from main_parser import get_parser_output, save_to_unhandled
from parsers_registry import get_parser_for_content
from load_parsers import load_all_parsers

MAILGUN_DOMAIN = os.getenv("MAILGUN_DOMAIN")
MAILGUN_API_KEY = os.getenv("MAILGUN_API_KEY")


def process_email_attachment(raw_bytes: bytes, filename: str) -> pd.DataFrame:
    """
    Attempt to process using a registered parser. If no parser matches, fallback to GPT.
    Save unhandled logs even when GPT succeeds.
    """
    try:
        print("[process_email_attachment] Attempting parser match...")
        raw_text = raw_bytes.decode("utf-8", errors="ignore")
        parser_name = get_parser_for_content(raw_text)
        if not parser_name:
            raise ValueError("No matching parser in registry.")
        parser_func = load_all_parsers().get(parser_name)
        if not parser_func:
            raise ValueError(f"Parser function '{parser_name}' not found.")
        df = get_parser_output(parser_func, raw_text)
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

            # Save raw file for later training even though GPT succeeded
            save_to_unhandled(filename, raw_bytes)

            print("[process_email_attachment] Fallback to GPT succeeded.")
            return df
        except Exception as gpt_err:
            print(f"[process_email_attachment] GPT fallback failed: {gpt_err}")
            save_to_unhandled(filename, raw_bytes)
            raise RuntimeError("No parser found and GPT fallback failed.")


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
