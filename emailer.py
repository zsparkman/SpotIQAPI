# emailer.py

import io
import os
import pandas as pd
import requests
from parser import parse_with_gpt

MAILGUN_DOMAIN = os.getenv("MAILGUN_DOMAIN")
MAILGUN_API_KEY = os.getenv("MAILGUN_API_KEY")

def process_email_attachment(raw_bytes: bytes) -> pd.DataFrame:
    """
    Decode the raw attachment bytes, send the text to parser.parse_with_gpt(),
    then read the returned CSV text into a DataFrame.
    """
    try:
        raw_text = raw_bytes.decode("utf-8", errors="ignore")
        parsed_csv = parse_with_gpt(raw_text)
        df = pd.read_csv(io.StringIO(parsed_csv))
        df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True, errors='coerce')
        df.dropna(subset=['timestamp'], inplace=True)
        return df
    except Exception as e:
        raise RuntimeError(f"Failed to process email attachment: {e}")

def send_report(to_email: str, report_bytes: bytes, filename: str):
    """
    Sends the matched report as an attachment back to the sender via Mailgun.

    :param to_email: recipient's email address
    :param report_bytes: byte contents of the CSV file
    :param filename: name for the attachment
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
