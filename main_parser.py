# main_parser.py

import os
import hashlib

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
