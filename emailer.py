import io
import os
import pandas as pd
import requests
import threading
from parser import parse_with_gpt
from main_parser import get_parser_output, save_to_unhandled
from parsers_registry import compute_fingerprint
from load_parsers import load_all_parsers
from parser_trainer import handle_unprocessed_files

MAILGUN_DOMAIN = os.getenv("MAILGUN_DOMAIN")
MAILGUN_API_KEY = os.getenv("MAILGUN_API_KEY")

def process_email_attachment(raw_bytes: bytes, filename: str):
    try:
        print("[process_email_attachment] Attempting parser match...")
        raw_text = raw_bytes.decode("utf-8", errors="ignore")
        fingerprint = compute_fingerprint(raw_text)
        parser_func = load_all_parsers().get(fingerprint)

        if not parser_func:
            raise ValueError(f"No parser found for fingerprint: {fingerprint}")

        df = get_parser_output(parser_func, raw_text)
        print("[process_email_attachment] Matched parser succeeded.")
        return df, "parser", f"{fingerprint}.py"

    except Exception as parser_err:
        print(f"[process_email_attachment] No parser matched: {parser_err}")
        try:
            raw_text = raw_bytes.decode("utf-8", errors="ignore")
            parsed_csv = parse_with_gpt(raw_text)
            df = pd.read_csv(io.StringIO(parsed_csv))
            df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True, errors='coerce')
            df.dropna(subset=['timestamp'], inplace=True)

            save_to_unhandled(filename, raw_bytes)
            threading.Thread(target=handle_unprocessed_files, daemon=True).start()

            print("[process_email_attachment] Fallback to GPT succeeded.")
            return df, "gpt", None

        except Exception as gpt_err:
            print(f"[process_email_attachment] GPT fallback failed: {gpt_err}")
            save_to_unhandled(filename, raw_bytes)
            raise RuntimeError("No parser found and GPT fallback failed.")

def send_report(to_email: str, report_bytes: bytes, filename: str):
    if not MAILGUN_DOMAIN or not MAILGUN_API_KEY:
        raise RuntimeError("Missing Mailgun config")

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
        raise RuntimeError("Missing Mailgun config")

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
