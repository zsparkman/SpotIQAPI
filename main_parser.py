import os
import hashlib
import pandas as pd
from s3_utils import upload_unhandled_log

PARSERS_DIR = "parsers"

def fingerprint_csv(df) -> str:
    norm = ",".join(sorted(col.strip().lower() for col in df.columns))
    return hashlib.md5(norm.encode("utf-8")).hexdigest()

def save_to_unhandled(filename: str, content: bytes):
    try:
        upload_unhandled_log(filename, content)
    except Exception as e:
        print(f"[S3] Upload failed: {e}")

    unhandled_dir = "unhandled_logs"
    os.makedirs(unhandled_dir, exist_ok=True)
    filepath = os.path.join(unhandled_dir, filename)
    with open(filepath, "wb") as f:
        f.write(content)
    print(f"[↪] Saved unhandled log to {filepath}")

def get_parser_output(parser_func, raw_text: str) -> pd.DataFrame:
    df = parser_func(raw_text)
    if not isinstance(df, pd.DataFrame):
        raise ValueError("Parser function did not return a DataFrame.")
    return df
