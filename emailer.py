import io
import os
import pandas as pd
import requests
import tempfile
from load_parsers import get_parser
from main_parser import save_to_unhandled
from parser import parse_with_gpt

MAILGUN_DOMAIN = os.getenv("MAILGUN_DOMAIN")
MAILGUN_API_KEY = os.getenv("MAILGUN_API_KEY")


def process_email_attachment(raw_bytes: bytes) -> pd.DataFrame:
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmp:
            tmp.write(raw_bytes)
            tmp_path = tmp.name

        parser_func = get_parser(os.path.basename(tmp_path))
        if parser_func:
            print("[process_email_attachment] Found parser. Using it.")
            records = parser_func(tmp_path)
            df = pd.DataFrame(records)
        else:
            print("[process_email_attachment] No parser matched. Falling back to GPT.")
            raw_text = raw_bytes.decode("utf-8", errors="ignore")
            parsed_csv = parse_with_gpt(raw_text)
            df = pd.read_csv(io.StringIO(parsed_csv))

        df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True, errors='coerce')
        df.dropna(subset=['timestamp'], inplace=True)
        return df

    except Exception as e:
        print(f"[process_email_attachment] ERROR: {e}")
        save_to_unhandled(tmp_path)
        raise RuntimeError(f"Failed to process email attachment: {e}")


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
