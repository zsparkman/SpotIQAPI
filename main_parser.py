# main_parser.py

import os
import hashlib
import pandas as pd

PARSERS_DIR = "parsers"

def fingerprint_csv(df) -> str:
    """
    Generates a fingerprint for a DataFrame based on sorted, normalized column headers.
    """
    norm = ",".join(sorted(col.strip().lower() for col in df.columns))
    return hashlib.md5(norm.encode("utf-8")).hexdigest()

def save_parser_to_repo(fingerprint: str, parser_code: str) -> str:
    """
    Saves a parser to the local parsers directory with a standardized filename.
    """
    if not os.path.exists(PARSERS_DIR):
        os.makedirs(PARSERS_DIR)

    filename = f"{fingerprint}.py"
    filepath = os.path.join(PARSERS_DIR, filename)

    with open(filepath, "w") as f:
        f.write(parser_code)

    return filepath

def save_to_unhandled(filename: str, content: bytes):
    """
    Saves an unrecognized file to the unhandled_logs directory for later review/training.
    """
    unhandled_dir = "unhandled_logs"
    if not os.path.exists(unhandled_dir):
        os.makedirs(unhandled_dir)

    filepath = os.path.join(unhandled_dir, filename)
    with open(filepath, "wb") as f:
        f.write(content)
    print(f"[↪] Saved unhandled log to {filepath}")

def get_parser_output(parser_func, raw_text: str) -> pd.DataFrame:
    """
    Executes the provided parser function on raw text to produce a DataFrame.
    """
    df = parser_func(raw_text)
    if not isinstance(df, pd.DataFrame):
        raise ValueError("Parser function did not return a DataFrame.")
    return df
