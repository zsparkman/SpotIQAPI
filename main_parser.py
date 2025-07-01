import os
import hashlib
import pandas as pd

UNHANDLED_LOGS_DIR = "unhandled_logs"
HANDLED_LOGS_DIR = "handled_logs"
PARSERS_DIR = "parsers"

os.makedirs(UNHANDLED_LOGS_DIR, exist_ok=True)
os.makedirs(HANDLED_LOGS_DIR, exist_ok=True)
os.makedirs(PARSERS_DIR, exist_ok=True)

def fingerprint_csv(content: str) -> str:
    return hashlib.sha256(content.encode("utf-8")).hexdigest()

def save_to_unhandled(filename: str, content: str):
    path = os.path.join(UNHANDLED_LOGS_DIR, filename)
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)

def get_parser_output(parser_module, raw_text: str) -> str:
    return parser_module.parse(raw_text)

def save_parser_to_repo(fingerprint: str, code: str):
    filename = f"{fingerprint}.py"
    path = os.path.join(PARSERS_DIR, filename)
    with open(path, "w", encoding="utf-8") as f:
        f.write(code)

def mark_log_handled(filename: str):
    src = os.path.join(UNHANDLED_LOGS_DIR, filename)
    dst = os.path.join(HANDLED_LOGS_DIR, filename)
    if os.path.exists(src):
        os.rename(src, dst)
