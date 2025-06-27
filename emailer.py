# emailer.py

import io
import os
import pandas as pd
import requests
from parser import parse_with_gpt
import time

MAILGUN_DOMAIN = os.getenv("MAILGUN_DOMAIN")
MAILGUN_API_KEY = os.getenv("MAILGUN_API_KEY")

def process_email_attachment(raw_bytes: bytes) -> pd.DataFrame:
    """
    Decode the raw attachment bytes, send the text to parser.parse_with_gpt(),
    then read the returned CSV text into a DataFrame.
    """
    try:
        raw_text = raw_bytes.decode("utf-8", errors="ignore")
        print("[process_email_attachment] Raw text decoded. Sending to GPT.")
        parsed_csv = parse_with_gpt(raw_text)
        print("[process_email_attachment] CSV parsing via GPT completed.")

        df = pd.read_csv(io.StringIO(parsed_csv))
        df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True, errors='coerce')
        df.dropna(subset=['timestamp'], inplace=True)

        print(f"[process_email_attachment] Parsed DataFrame with {len(df)} rows.")
        return df
    except Exception as e:
        print("[process_email_attachment] ERROR:", e)
        raise RuntimeError(f"Failed to process email attachment: {e}")

def send_report(to_email: str, report_bytes: bytes, filename: str, retries: int = 3):
    """
    Sends the matched report as an attachment back to the sender via Mailgun.

    :param to_email: recipient's email address
    :param report_bytes: byte contents of the CSV file
    :param filename: name for the attachment
    :param retries: number of retry attempts on failure
    """
    if not MAILGUN_DOMAIN or not MAILGUN_API_KEY:
        raise RuntimeError("Missing Mailgun config: MAILGUN_DOMAIN or MAILGUN_API_KEY")

    attempt = 0
    while attempt < retries:
        try:
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

            if response.status_code == 200:
                print(f"[send_report] Email successfully sent to {to_email}")
                return
            else:
                print(f"[send_report] Attempt {attempt+1}: Failed to send email ({response.status_code}) - {response.text}")
        except Exception as e:
            print(f"[send_report] Attempt {attempt+1}: Exception while sending email - {e}")

        attempt += 1
        time.sleep(2)

    raise RuntimeError(f"[send_report] Failed to send email to {to_email} after {retries} attempts.")
