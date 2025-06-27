# emailer.py

import io
import pandas as pd
from parser import parse_impressions_with_gpt

def process_email_attachment(raw_bytes: bytes) -> pd.DataFrame:
    """
    Decode the raw attachment bytes, send the text to parser.parse_impressions_with_gpt(),
    then read the returned CSV text into a DataFrame.
    
    :param raw_bytes: the raw bytes of the uploaded impressions file
    :return: DataFrame with at least a 'timestamp' column (UTC) and any other parsed fields
    """
    # 1) Decode bytes to text
    raw_text = raw_bytes.decode("utf-8", errors="ignore")
    
    # 2) Send to your GPT-powered parser
    parsed_csv = parse_impressions_with_gpt(raw_text)
    
    # 3) Load the CSV text into a DataFrame
    df = pd.read_csv(io.StringIO(parsed_csv))
    
    # 4) Ensure timestamp is parsed as datetime UTC
    df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True, errors='coerce')
    df.dropna(subset=['timestamp'], inplace=True)
    
    return df
