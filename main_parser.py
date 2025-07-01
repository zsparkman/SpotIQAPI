import os
import hashlib
import pandas as pd
from s3_utils import upload_unhandled_log

PARSERS_DIR = "parsers"

def fingerprint_csv(df) -> str:
    norm = ",".join(sorted(col.strip().lower() for col in df.columns))
    return hashlib.md5(norm.encode("utf-8")).hexdigest()

def save_parser_to_repo(fingerprint: str, parser_code: str) -> str:
    if not os.path.exists(PARSERS_DIR):
        os.makedirs(PARSERS_DIR)

    filename = f"{fingerprint}.py"
    filepath = os.path.join(PARSERS_DIR, filename)

    with open(filepath, "w") as f:
        f.write(parser_code)

    return filepath

def save_to_unhandled(filename: str, content: bytes):
    unhandled_dir = "unhandled_logs"
    os.makedirs(unhandled_dir, exist_ok=True)

    filepath = os.path.join(unhandled_dir, filename)
    with open(filepath, "wb") as f:
        f.write(content)

    print(f"[↪] Saved unhandled log to {filepath}")
    
    # Also upload to S3
    try:
        upload_unhandled_log(filename, content)
    except Exception as e:
        print(f"[✖] Failed to upload unhandled log to S3: {e}")

def get_parser_output(parser_func, raw_text: str) -> pd.DataFrame:
    df = parser_func(raw_text)
    if not isinstance(df, pd.DataFrame):
        raise ValueError("Parser function did not return a DataFrame.")
    return df
