# emailer.py

import io
import os
import pandas as pd
import requests
from parser import parse_with_gpt
from main_parser import get_parser_output, save_to_unhandled

MAILGUN_DOMAIN = os.getenv("MAILGUN_DOMAIN")
MAILGUN_API_KEY = os.getenv("MAILGUN_API_KEY")


def process_email_attachment(raw_bytes: bytes, filename: str) -> pd.DataFrame:
    """
    Attempt to process using parser. If no parser matches, fallback to GPT.
    Save unhandled logs even when GPT succeeds.
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

            # Save raw file for later training even though GPT succeeded
            save_to_unhandled(filename, raw_bytes)

            print("[process_email_attachment] Fallback to GPT succeeded
